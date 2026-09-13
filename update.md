## Unreleased

- 优化收盘热路径：按市场、symbol、interval 分片处理；同一 openTime 的排队 forming 保留最新有效快照，所有 `x=true` 单独进入有界优先队列，容量满时背压。新增 `kline.ingress.*` 配置和接纳、处理、合并、失败、背压指标。
- WS、REST、恢复统一原子提交，修复相同成交笔数 final 未替换旧值及并发旧消息回写；final 修订会失效 bulk 缓存。WS 补洞占位不提前确认收盘，持久化只保存一致的已确认 final 快照；正常停机先停止收帧并排空消息，再持久化。
- 收齐统计从逐消息扫描全部缓存改为边界计数，bulk 改为按 bar 定向通知；增加单条更新快速路径，减少 forming 诊断分配和版本元数据开销，修复 PING/PONG 名称误判与 Netty buffer 引用泄漏。[实现、回放结果和验证边界](docs/kline-ingress-implementation-20260913.md)。
- 增加收盘消息分段耗时诊断，区分 Netty 帧回调、线程池排队、JSON / 协议解码、缓存更新、收盘通知和既有收齐统计；整点后 30 秒延后输出汇总、连接统计及最多 10 条尾部消息。默认开启，可用 `kline.diagnostics.closedBarLatencyEnabled=false` 停止收集和输出。口径见 [closed-bar-latency.md](docs/closed-bar-latency.md)。
- 合约 K 线支持按周期选择流：`kline.binance.future.intervalSyncConfigs.<interval>.useContinuousKlineStream` 默认 `false`，使用原有 `<symbol>@kline_<interval>`；设为 `true` 后，永续合约使用 `<pair>_<contractType>@continuousKline_<interval>`。例如只为 `1h` 开启，`1d` 仍可保持普通流。
- 普通流 `BinanceKlineStream` 与连续流 `BinanceContinuousKlineStream` 各自封装主题生成、解析及符号映射，共用 `AbstractBinanceKlineStream` 的解码流程；服务共用缓存、持久化和 `x=true` 收盘通知。支持原始消息及 combined stream 封装。
- 连续流通过 exchange info 的 `pair + contractType` 映射到唯一、状态为 `TRADING` 的交易符号，支持 `PERPETUAL` / `TRADIFI_PERPETUAL`。交割合约、缺失或有歧义的映射继续使用普通流，避免把滚动连续序列写入单个交割合约的历史。订阅缓存会随模式或合约映射变化失效。

## 1.7.16 (2026-09-03)
- delisted / halted symbols: the bulk finality wait (`closed_only=true`) only waits for symbols whose exchange status is `TRADING` (exchange info, refreshed every 5 min). A symbol delisted mid-hour whose closing update never arrives no longer holds the response until the 8 s cap; it is returned as-is and listed in the new response field `not_trading` (and in `BULK_FINAL_WAIT… not_trading=[…]`). `CLOSED_BAR_SETTLED` / `CLOSED_BAR_SETTLE_INCOMPLETE` count `expected`/`arrived`/`pending` over TRADING symbols only and add `not_trading=<n> not_trading_symbols=[…]`. When exchange info is unavailable/empty the filter is off (every symbol is waited for, as in 1.7.14).

## 1.7.15 (2026-09-03)
- config fix: the embedded server is Undertow, so the 1.7.14 `server.tomcat.*` keys were inert; use `server.undertow.threads.worker` (400) / `server.undertow.threads.io` (4) so ~300 fleet requests can block concurrently at the boundary.
- log `CLOSED_BAR_SETTLED interval boundary expected arrived first_ms p50_ms p90_ms max_ms last` once every symbol that has the just-closed bar received its closing update (`x=true`): `max_ms` = how long after the boundary the previous bar is available for ALL symbols; `CLOSED_BAR_SETTLE_INCOMPLETE … pending_symbols=[…]` when some closing updates are still missing 30 s after the boundary (checked every 5 s).

## 1.7.14 (2026-09-03)
- bulk klines (`closed_only=true`): the just-closed bar is only returned once every requested symbol that has a bar for it received its FINAL update (websocket `x=true`, REST sync, restore, or synthetic fill); requests inside the settling window block until then (cap `kline.bulk.finalWaitMaxMs`, default 8000 ms, measured from the boundary). Measured before the fix: at T+1 s 30–34% of symbols were served the pre-close snapshot (13–17% with a different close). Non-final responses are never cached; the cache key includes the interval boundary. Response gains `finalized`, `pending`, `waited_ms`. Config: `kline.bulk.finalWaitEnabled` (true), `kline.bulk.finalWaitMaxMs` (8000). Ops: raise `server.tomcat.threads.max` (600) because ~300 fleet requests block simultaneously at the boundary.

# kline-proxy 1.7 更新日志

对比版本：

- `1.6`: `86b7e9be`
- `1.7`: `922e2553`

本次版本重点围绕 Binance 接口兼容性、行情数据正确性、运行时性能和本地持久化能力进行了增强。

## 主要更新

- 新增 K 线本地磁盘持久化能力。支持按 `service/interval/symbol/day` 分片落盘，服务启动时优先加载本地已保存的已闭合 K 线，再补齐本地缺失区间，降低冷启动期间对 Binance 的依赖。
- 新增持久化保留策略。支持按 `interval` 或 `interval + symbol` 配置最大落盘条数；未显式配置时，默认按 `minMaintainCount * 2` 保留，并自动清理超出范围的历史分片文件。
- 优化启动恢复流程。服务启动时会先恢复本地 K 线缓存，再启动 websocket 和后续补数逻辑，缩短重启后的历史数据回填时间。

- 现货接口兼容性增强。`/api/v3/ticker/24hr` 支持 `symbols` JSON 数组参数、`type=MINI`、`symbolStatus`；`/api/v3/klines` 支持 `timeZone`，并对 `1M` 月线走官方 REST 语义。
- 合约接口兼容性增强。`/fapi/v1/fundingRate` 改为支持 `symbol/startTime/endTime/limit` 历史查询；`/fapi/v1/premiumIndex`、`/fapi/v1/fundingRate` 的返回格式更贴近 Binance 官方字符串语义。
- 错误响应改为 Binance 风格。接口异常现在返回标准 HTTP 状态码和 `{code, msg}` 结构，不再统一返回 `200` 和纯字符串。
- 修复 `symbol` 与 `symbols` 参数组合处理问题。新增参数冲突校验、非法 `symbols` 格式校验，以及单元素数组返回形态兼容。

- 修复多项 K 线数据问题，包括缺口补数逻辑、`closeTime` 语义、空洞数组返回、现货与合约限流器混用、定时刷新异常中断等问题。
- 修复统计指标计算问题。ATR 等指标的数据转换链路和计算逻辑得到修正，减少错误值和不必要的序列化开销。

- 优化 websocket 热路径。移除了“每个 topic 一个轮询监控任务”的旧模型，改为 client 级巡检；同时将消息处理改为单次解析，减少重复 JSON 反序列化和线程调度开销。
- 优化全市场 ticker 性能。无参数的 `/ticker/price` 和 `/ticker/24hr` 增加短 TTL 快照缓存，并对热点大响应增加预序列化 JSON 复用，降低重复请求延迟和上游访问压力。
- 优化 K 线补数调度。限制批量补数并发，减少递归 fanout 和线程池争用，提升同步过程的平稳性。
- 优化默认内存占用。默认 K 线保留量调整为更保守的配置，降低常驻对象数量和默认运行内存压力。

- 新增覆盖接口契约、websocket、K 线补数、持久化恢复与清理逻辑的测试用例，整体回归保障明显增强。
- 同步更新 Dockerfile、构建脚本和示例配置，方便部署和版本切换。

## 变更概览

- 兼容性：更贴近 Binance 官方公共行情接口行为
- 正确性：修复 K 线、ticker、统计指标和错误语义问题
- 性能：减少 websocket 调度、重复解析和大响应重复序列化开销
- 可运维性：新增本地持久化、默认配置优化和部署脚本调整
