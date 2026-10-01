# Java 在当前小时新 K 线缺失时的返回行为

调查日期：2026-09-28。结论针对当前线上 Java 版本：普通单币 K 线接口及 bulk 不会因为当前小时的新 bar 缺失而自动创建零成交量 bar，也不会在这些查询路径同步请求 REST 回填。不指定时间范围时，它们返回缓存里最近的历史 bar；这不排除后台任务随后补入新数据。

## 核对对象及验证方法

- 线上实例：`192.0.2.30`，服务 `kline-proxy`，查询时 PID `531654`。
- 实际加载 JAR：`/opt/kline-proxy/kline-proxy-1.8.1-low-risk-4b8808074930.jar`。
- JAR SHA-256：`4b88080749301f9556067db6ca4b3b3e0e72465844af81a92247172de8111a4a`。
- `AbstractKlineService`、`KlineService`、`BinanceFutureController`、`BinanceSpotController`、`KlineSet` 五个关键类的线上字节码 SHA-256 均与本地 `target/classes` 一致。
- 使用这些实际编译类及 MockMvc 调用控制器，用测试 harness 控制缓存、服务时钟和上游响应。JDK 21 运行，18 个场景全部通过。这是受控复现，不是线上故障注入；结果中的耗时不是性能基准。

证据：[线上身份](../research/java-missing-forming-20260928/deployed-identity.json)、[本地类哈希](../research/java-missing-forming-20260928/local-class-match.json)、[复现程序](../research/java-missing-forming-20260928/JavaMissingFormingProbe.java)、[完整返回值](../research/java-missing-forming-20260928/results.json)。没有修改、重编译或部署主程序。

## 实际返回值

设当前为 UTC 17:00:20，缓存有最近 10 根，最后一根 openTime 为 16:00，16:00 bar 已确认收盘，但尚无 17:00 bar。

| 请求或场景 | 实际结果 | 查询触发 REST 次数 |
|---|---|---:|
| 合约 `/fapi/v1/klines?symbol=BTCUSDT&interval=1h&limit=10` | 返回缓存最近 10 根，末根仍为 16:00 | 0 |
| 现货 `/api/v3/klines` 相同参数 | 同上 | 0 |
| 上述两个接口 `limit=1` | 仅返回 16:00 bar，保留真实成交量 | 0 |
| 显式指定 `startTime=17:00`、`endTime=17:59:59.999` | `[]`，不回退到 16:00 | 0 |
| bulk `closed_only=false` | 最近 10 根，末根 16:00；`finalized=true`、`pending=[]`、`waited_ms=0` | 0 |
| bulk `closed_only=true` | 最近 10 根已到收盘时间的 bar，末根 16:00；同样无需等待 | 0 |
| 单币缓存完全为空 | `[]` | 0 |
| bulk 中指定币种缓存为空 | `klines` 中省略该币种 | 0 |
| 连 16:00 bar 都不存在、最新只有 15:00 | `closed_only=true` 仍可返回更早的 10 根且 `finalized=true` | 0 |
| 内部显式调用 `queryKlineArray(..., makeUp=true)` | 执行补全，返回 stub REST 提供的 17:00 bar | 1（stub） |

最后两个场景说明：Java 的 `finalized` 不是完整性证明；内部虽有同步 REST 补全能力，普通公开 K 线接口没有启用它。

## 真实新 bar 到达后的可见性

另一个场景先请求 `closed_only=false` bulk，再注入 17:00 的真实 forming bar：

- 单币查询立即返回 17:00 新 bar。
- 紧接着再次发相同 bulk 请求，仍返回旧缓存响应。
- 等待 1,100 ms 后再次发相同 bulk 请求，返回值包含 17:00 bar。

这是 bulk 一秒响应缓存的行为；不是 WebSocket 未写入内存。

## Java 已有的补零适用范围

收到更晚的真实 bar 后，`fillKlines` 会补两根已观察到的 bar 之间的缺口。例如已有 16:00，随后收到 18:00，才会在中间补 17:00：

- OHLC 全部等于 16:00 的 close。
- volume、quoteVolume、tradeNum、taker buy base/quote volume 全为 0。
- 流式更新产生的这种补点不被标记为已确认收盘。

它不会只因当前时钟到了 17:00，就从末根 16:00 向后外推 17:00 占位 bar。

## 源码位置与边界

- [KlineService.java](../src/main/java/com/zx/quant/klineproxy/service/KlineService.java)：默认重载传入 `makeUp=false`。
- [AbstractKlineService.java](../src/main/java/com/zx/quant/klineproxy/service/impl/AbstractKlineService.java)：`queryKlines`（398 行起）处理范围回退；`awaitJustClosedBarsFinal`（505 行起）及 `hasNonFinalBar` 只等待已存在但未确认的上一周期 bar；`buildBulkKlinesResponse`（618 行起）遍历内存；`fillKlines`（879 行起）仅补内部缺口。
- [BinanceSpotController.java](../src/main/java/com/zx/quant/klineproxy/controller/BinanceSpotController.java)：普通现货 K 线走上述缓存路径；非空 `timeZone` 或 `interval=1M` 直接透传官方 REST，不能套用本报告的普通 1h 查询结论。
- [BinanceFutureController.java](../src/main/java/com/zx/quant/klineproxy/controller/BinanceFutureController.java)：单币调用默认查询重载，bulk GET/POST 调用同一服务方法。

Java actuator 的健康状态也不等价于 Rust 的 `stale_tail` 检查。Java 不报同样的 503，不能证明 Java 已经有本小时的真实 bar。

## 对 Rust 临时构造的含义

“有上一根已确认 close、尚无当前 bar 时，用 close 构造当前周期零成交量占位”将是新增行为，不是当前 Java 已有行为。若实现，应保留内部来源标记、保持未收盘状态，并由真实 WebSocket/REST 数据整体替换；占位的存在不能掩盖真实数据是否过期，也不能阻止后台继续获取真实数据。

币安官方上游的独立实测在 Rust 仓库 `research/binance-forming-20260928/` 留存，需与这里的受控 Java 结论分别理解。
