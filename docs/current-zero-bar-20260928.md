# 当前周期零成交量占位 bar

2026-09-28 已随 Java 1.8.2 部署到 market.feng.dog；Rust 0.4.15 同步部署到 market-mirror.feng.dog。Java 与 Rust 使用相同规则，适用于现货和合约的内存 K 线查询。线上验证范围见 [部署记录](current-zero-deployment-20260928.md)。

## 构造条件

必须在本进程收到并接受**紧邻上一根 K 线的 WebSocket `x=true`**，且当前 bar 尚未进入缓存、当前时间仍属于紧邻的下一个周期，才允许构造。REST 将旧 bar 标为已收盘、snapshot 恢复、历史补洞、`x=false`、其他旧周期的 `x=true` 都不能单独授权构造。确认记录按 symbol/interval 隔离，不跨周期继承，也不落盘。

| 字段 | 临时值 |
|---|---|
| openTime | 上一根 closeTime + 1 |
| closeTime | 当前周期结束前 1 ms；月线按真实日历月计算 |
| open/high/low/close | 上一根 close |
| volume、quoteVolume、tradeNum、主动买入量/额 | 0 |

这是尚未收到真实更新时的临时返回值，不代表交易所已确认没有成交。

## 查询与替换

- 单币最新查询及 `closed_only=false` bulk 在选取窗口时加入这一根，计入 limit；显式历史范围不追加范围外的 bar。
- `closed_only=true` 不包含占位。占位不写入 KlineSet/Series，不标记收盘，不写入 snapshot，不跨多个缺失周期连续外推。
- 当前真实 WS/REST bar 一旦进入缓存，立即取代占位；即使真实 bar 也是 0 笔成交，也没有与占位冲突的版本比较。
- bulk 校验序列结构代次与占位到期时间。收到上一根 `x=true`、写入当前真实 bar、修正上一根最终值，都可使旧响应失效，不等待原有 1 秒 TTL。正常活动 bar 的后续更新仍沿用既有短缓存策略。
- 查询路径不新增 REST、限流等待或定时全量扫描。K 线订正按既有周期运行，不因构造占位请求 REST；订正仍只查看实际缓存，不把占位当成上游数据。
- 已知停止交易的币种不构造。现货非空 `timeZone` / `1M` 的普通 HTTP 查询仍按既有规则透传官方 REST，此路径不做本地外推。

重启后仅恢复历史数据，不能靠磁盘中的“已收盘”标记补零，必须重新收到对应上一根的 `x=true`。没有这帧且当前真实 bar 也未到达时，最新查询继续返回已有历史。

## 实现与验证

Java：`KlineSet` 保存每条序列最近接受的流式收盘 openTime，`KlineFill` 保留四种数值模式，`AbstractKlineService` 在单币/bulk 查询时构造只读占位。

`CurrentZeroBarTest` 验证双控制器、四种数值模式、确认来源、相同数值的迟到 `x=true`、真实数据替换、历史范围/limit、月周边界、跨周期失效、停止交易和 snapshot 隔离。执行命令：JDK 21 `mvn -o test`。

最终验证：全量 227 项，226 通过、1 项原有跳过，0 失败、0 错误；`git diff --check` 通过。

Rust 对应说明位于 Rust 工程 `docs/current-zero-bar-20260928.md`。
