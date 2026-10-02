# bulk 周期边界的时钟滞后（2026-09-30）

`POST /fapi/v1/klines/bulk` 在 `closed_only=true` 时，应等到刚收盘的 bar 收到 WebSocket `x=true`（FINAL）后再返回。整点后最初约 10 ms 内到达的请求却被判为「边界前」：没有等待，直接按上一小时的视图返回，刚收盘的 bar 不在结果里。本文说明原因、修复方式、测试和上线后的观察项。本次只改代码和测试，未部署。

## 缺陷

bulk 用 `boundary = floor(now / interval) * interval` 决定哪根 bar 刚收盘，代码见 `AbstractKlineService.queryBulkKlines` / `queryBulkSnapshot` / `awaitJustClosedBarsFinal`。合约和现货的 `now` 取自 `getServerTime()`，即 `ExchangeService.queryServerTime()`：

```java
// BinanceFutureExchangeServiceImpl / BinanceSpotExchangeServiceImpl
queryServerTime() = System.currentTimeMillis() - serverTimeDelta
refreshServerTimeDelta(): delta = 收到 /time 响应之后的本机时间 - serverTime   // 每小时一次
```

Binance 盖 `serverTime` 的时刻大约在往返的中点，而 `delta` 在响应到达后才计算，没有扣除回程时间。于是代理眼里的「服务器时间」比真实时间晚约 RTT/2。到 fapi 的 RTT 实测 20–25 ms，所以落后约 10 ms 以上；如果那一小时的采样恰好偏慢，落后得更多。宿主机时钟由 chrony 校准，实测只快 0.35 ms，因此偏差来自估计方法，而不是本机时钟。

整点后约 10 ms 内到达的 `closed_only` 请求会发生三件事：

1. 被判为边界前：`boundary` 仍是上一小时，刚收盘的 bar 被当作「未收盘」排除，也不进入 FINAL 等待。
2. 如果此前已有同一组参数在边界前算好的缓存快照（缓存 key 为旧 boundary，`validUntil` 为 forming bar 的 closeTime），这个快照会被直接返回。
3. 响应里没有刚收盘的那根 bar。

## 证据（2026-09-30 15:00Z）

- nos-rs child 分片 0–2 在 T+0–4 ms 发出请求（发送端时钟由 AWS 同步），3–7 ms 就拿到响应，响应中缺 14:00 的 bar。
- nginx 在 :00 的 rt p50：15:00 为 7 ms；14:00 为 28 ms，16:00 为 78 ms。
- 这些快速返回的请求没有对应的 `BULK_FINAL_WAIT` 日志。发生了等待的请求，日志里的 `boundary` 正确记为 15:00。

## 修复

### 1. 边界判定改用宿主机时钟，但不早于交易所时间估计

`AbstractKlineService` 新增两个方法：

- `getHostTime()`：本机时钟，生产环境下由 chrony 校准。
- `getBoundaryTime()`：bulk 专用时钟，默认等于 `getServerTime()`。

合约和现货实现把 `getBoundaryTime()` 覆盖为 `hostClockBoundaryTime()`，其取值由开关 `kline.bulk.hostClockBoundary` 决定：

```java
kline.bulk.hostClockBoundary ? Math.max(getHostTime(), getServerTime()) : getServerTime()
```

- 默认值为 `true`，即使用上面 `max` 的判定。
- 设为 `false` 时，边界判定与本次修复之前完全一致，只用 `getServerTime()`。
- `preBoundaryWaitMs` 仍由它自己的配置单独控制，设为 0 即关闭。

bulk 路径内所有依赖时间的判定都换成这个时钟，包括：

- 刚收盘 bar 的判定、缓存 key 中的 boundary、FINAL 等待的起点和上限。
- `CachedBulk.current` 的有效期判断。
- `ClosedKlineViewCache.snapshot` 中 `closeTime <= now` 的选择和 `validFrom`/`validUntil`。
- 响应中的 `ts_ms`。

这些判定必须共用一个时钟。如果边界已经翻过，但视图仍按滞后的时钟选择，那么在 `x=true` 已先到、无需等待的情况下，刚收盘的 bar 还是会被排除。

`getServerTime()` 本身和它的其他使用者都没有改，逐项结论见下面的「审计」一节。

**为什么不改用 RTT 中点来修正估计值？** 中点公式是 `delta = (t_send + t_recv) / 2 - serverTime`。这样做会改动全局的「服务器时间」，影响 `/fapi/v1/time`、`/api/v3/time`、exchangeInfo 的 `serverTime`、REST 收盘判定、收齐诊断等所有使用者，超出本次范围。单次采样的残差约为链路不对称量的一半；采样偏慢时误差会更大。此外有一个实际风险：REST 写入时的 `finalizeClosed && closeTime <= now` 是在响应回来之后才取 `now`。现在的时钟滞后约一个回程，恰好抵消了这段时间。如果换成无偏时钟，在边界后约一个回程（约 10 ms）的窗口内，Binance 在边界前处理的 REST forming bar 会被错误标记为 final。宿主机时钟已经由 chrony 校准，资金费边界（`BinanceFutureExchangeServiceImpl` 直接用 `System.currentTimeMillis()`）和限流（`RateLimitManagerImpl`）也早已依赖它，所以不引入新的依赖。

**为什么还要和交易所时间估计取 `max`？** 取 `max` 后，新时钟在任何时刻都不早于修复前的时钟，因此不会重新引入滞后。如果 chrony 失效、本机时钟变慢，边界判定会退回到交易所估计值（最多落后约 RTT/2），由第 2 部分的等待窗口兜底。本机时钟偏快是安全方向：`closed_only` 请求只会提前开始等待 `x=true`；如果超过 8 s 上限，响应会以 `finalized=false` 加 `pending` 明确标出，不会悄悄漏掉 bar。

### 2. 边界前等待窗口

新增配置 `kline.bulk.preBoundaryWaitMs`，默认 250 ms，有效值限制在 0–2000 ms，设为 0 即关闭。

`closed_only=true` 的请求如果在边界前不到这个时长到达，会先用 `Thread.sleep` 按墙钟睡到边界，然后按边界处理：请求使用的时钟以该边界为下限，再走正常的 FINAL 等待。等待上限仍是 8 s，从边界开始计。

- 只在 FINAL 等待开启时生效：需要 `finalWaitEnabled=true` 且 `finalWaitMaxMs > 0`。
- `closed_only=false`、symbol 集合为空、离边界更远的请求，行为都不变。
- 每次这类等待记一行日志：`BULK_PRE_BOUNDARY_WAIT interval boundary early_ms slept_ms`。

这个窗口处理两类请求：调用方自身时钟偏快导致的早到，以及残余时钟误差造成的「看起来早到」。在修复后的时钟下，边界前的缓存快照在边界之后不会再被返回，原因有两点：key 中的 boundary 已经翻过，旧快照的 `validUntil` 也已过期。

### 审计：其他使用 `getServerTime()` 的时间判定

| 位置 | 作用 | 结论 |
|---|---|---|
| bulk：`queryBulkKlines`、`queryBulkSnapshot`、`awaitJustClosedBarsFinal`、`buildBulkKlinesResponse`、`CachedBulk.current`、`ClosedKlineViewCache.snapshot` 的时间条件 | 周期边界、缓存 key 与有效期、已收盘 bar 的选择 | 改为 bulk 时钟（本次修复） |
| `updateKlinesInternal` 中 REST 的 `closeTime <= now` | 判定 REST bar 是否已收盘 | 不改。滞后是安全方向（只会晚标 final），原因见上文 |
| `restoreKlines` | 判定从磁盘恢复的 bar 是否已收盘 | 不改。持久化只写入 `closeTime < now` 的 final bar |
| `queryKlines` 的占位 bar、`calculateRealStartEndTime` / `buildMakeUpTimeRanges` | 单币查询和 REST 补数的时间窗口 | 不改。占位 bar 要等上一根 `x=true`，而它在边界后数百 ms 才到，约 10 ms 的偏差无影响；REST 的 startTime/endTime 本来就按交易所时间 |
| 资金费（`BinanceFutureExchangeServiceImpl`） | 资金费边界、缓存 key、发布宽限 | 已在用 `System.currentTimeMillis()`，不受这个偏差影响 |
| `RateLimitManagerImpl` | 限流窗口 | 用本机时钟，不涉及 server time |
| ticker 价格簿（`isStreamAlive`、`applyAbsent`、快照的 requestTime） | 2 s 级的断流判断与先后排序 | 不改 |
| 持久化避让窗口、RPC 同步的 `HourBoundaryGuard` | 秒级窗口 | 不改 |
| `recordClosedBarArrival`、`closedBarLatencyRecorder` | 诊断 | 不改。注意 `CLOSED_BAR_SETTLED` 的 `first_ms`/`max_ms` 等以 server time 计，比真实值小约 RTT/2 |
| `/fapi/v1/time`、`/api/v3/time`、exchangeInfo 的 `serverTime` | 对外接口 | 不改 |

## 测试

新增 `BulkBoundaryClockTest`，11 项。测试使用生产类 `BinanceFutureKlineServiceImpl` / `BinanceSpotKlineServiceImpl`，并固定两个时钟：`getHostTime()` 代表真实时间，交易所时间估计比它落后 10 ms。

| 测试 | 场景 | 变异检查 |
|---|---|---|
| (a) `justAfterTheBoundaryATrailingExchangeClockStillWaitsForTheClosingBar`（合约、现货两个参数） | 先在 T−5 s 缓存一份上一小时的快照；到真实 T+5 ms 时交易所估计仍是 T−5 ms，等待窗口关闭。期望：不返回缓存快照，等到 `x=true`，返回 14:00 的 FINAL bar | 修复前代码和 M1 失败 |
| (b) `aRequestJustBeforeTheBoundaryWaitsForItThenForTheClosingBar` | T−100 ms 到达：先睡到边界，再等到 FINAL，返回 14:00 的 bar | 修复前代码、M2、M8 失败 |
| (b) `theCapAfterAPreBoundaryWaitIsMeasuredFromTheBoundary` | 同上但收盘更新一直不来：上限从边界算起，返回 `pending=[BTCUSDT]` | 修复前代码、M2、M8 失败 |
| (c) `aRequestSecondsBeforeTheBoundaryIsAnsweredAtOnceFromThePreviousHour` | T−5 s 到达：行为不变，立即返回上一小时视图，不等待 | 修复前代码下通过（回归保护）；M4 失败 |
| `closedOnlyFalseDoesNotWaitForTheBoundary` | `closed_only=false` 在窗口内也不等待 | 修复前代码下通过（回归保护）；M5 失败 |
| `aHostClockBehindTheExchangeEstimateDoesNotDelayTheBoundary` | 本机时钟落后于交易所估计时，按估计值判定边界 | 修复前代码下通过（修复前本来就用估计值）；M6 失败 |
| `preBoundaryWaitBindsDefaultsTo250AndIsCapped` | 配置绑定、默认值、上下限 | 修复前代码下无法编译；M7 失败 |
| `theSwitchOffRestoresTheServerTimeBoundaryDecision`（合约、现货两个参数） | `hostClockBoundary=false`，重放 (a) 的事故场景。期望恢复修复前的判定：在 T+5 ms 立即返回 T−5 s 缓存的同一份快照，最后一根为 13:00，没有等待，`ts_ms` = T−5010 | 修复前代码下无法编译（缺开关）；M9 失败 |
| `hostClockBoundaryBindsAndDefaultsToOn` | 开关的绑定，以及默认值为 `true` | 修复前代码下无法编译；M10 失败 |

变异体说明（每个变异体都在独立副本中重新编译）：

- M0：`src/main` 整体回退到 master，测试保持本分支版本。
- M1：去掉合约/现货对 `getBoundaryTime` 的覆盖，边界回到 `getServerTime()`。
- M2：关闭边界前等待。
- M3：M1+M2 合并。这是能编译的前提下与修复前行为等价的版本。
- M4：等待窗口不做上限判断。
- M5：等待也作用于 `closed_only=false`。
- M6：`getBoundaryTime` 只用本机时钟，不取 `max`。
- M7：配置不做范围限制。
- M8：等待之后不以边界作为时钟下限。
- M9：忽略开关，始终使用宿主机时钟。
- M10：开关默认值改为 `false`。

结果见 `research/boundary-clock-bias-20260930/mutation.log`，可用同目录下的 `mutate.py` 复现。

- 在 M3 下，(a) 返回的正是 T−5 s 缓存的那份上一小时快照（`tsMs` = T−5010），与事故机制一致；两个 (b) 测试的最后一根 bar 都是 13:00，而不是 14:00。
- 在 M0 下，测试引用的新方法不存在，编译直接失败。

已有测试 `BinanceFutureContinuousKlineTest` 的 fixture 直接构造生产类，并把交易所时钟固定在 `BOUNDARY + 150`。改动后生产类会读取真实本机时钟，这个 fixture 因此失败 8 项。修正方式是把 `getHostTime()` 也固定到同一时刻，断言一条都没有改。

全量测试：JDK 21.0.8，`mvn -B -ntp clean test`，共 328 项，327 通过，0 失败，0 错误，1 项原有跳过。修改前 master 为 317 项，本次新增 11 项。摘要见 `research/boundary-clock-bias-20260930/full-test.log`。

新测试含计时断言。`BulkBoundaryClockTest`、`BinanceFutureContinuousKlineTest`、`AbstractKlineServiceFillKlinesTest`、`CurrentZeroBarTest` 这 4 个测试类在加开关后合计 87 项，连续重复运行 5 轮，均全部通过（加开关前为 84 项，同样 5 轮全部通过）。`git diff --check` 通过。

## 上线注意

- **行为变化。** 整点后几 ms 内到达的合约 bulk `closed_only` 请求，以前会立即返回上一小时的视图，现在会等到刚收盘 bar 的 `x=true`。nos-rs 分片 0–2 的响应时间会从 3–7 ms 升到与其他分片同一量级（14:00 为 28 ms，16:00 为 78 ms，取决于当时的收齐速度）。这是修复的预期代价。
- **`ts_ms`。** 对 `closed_only` 与 `closed_only=false` 的 bulk 响应，`ts_ms` 改为同一个 bulk 时钟，比以前晚约 RTT/2。JSON 形状不变。
- **上线后前几个整点要看的指标：**
  - nginx 在 :00 的 bulk rt p50/p10：不应再出现 3–7 ms 的快速返回。以 15:00 的 7 ms 为反例，14:00 的 28 ms、16:00 的 78 ms 为参照。
  - `BULK_FINAL_WAIT` 行数：每个整点应覆盖所有在 T+0–数 ms 发出请求的分片 key，尤其是以前没有日志的分片 0–2。对每一行，`boundary` 都应等于该整点。这类行按单飞 key 计数，参数相同的并发请求只记一行。
  - `BULK_FINAL_WAIT_CAP`：不应上升。
  - `BULK_PRE_BOUNDARY_WAIT`：正常情况下只有少量边界前 250 ms 内到达的请求。如果每个整点都大量出现，说明有调用方时钟偏快，或宿主机时钟异常。
  - nos-rs 侧：每个分片拿到的 bulk 结果都应包含刚收盘的 bar，也就是 15:00 事故里缺失的那种情况不再出现。
  - 宿主机：`chronyc tracking` 的 System time 偏差应保持在 ms 以下。修复后的边界判定依赖它；本机时钟变慢时会退回交易所估计值，由 250 ms 窗口兜底。
- **回滚。** 两项改动各有开关，都在启动时读取，改完需要重启服务（不需要换 jar）：
  - 在实机配置文件中设 `kline.bulk.hostClockBoundary: false`：边界判定恢复为修复前的 `getServerTime()`。
  - 设 `kline.bulk.preBoundaryWaitMs: 0`：关闭边界前等待。
  - 两项都设：bulk 的边界行为与修复前一致。
  - 也可以直接回滚到旧 jar。
  - 实机使用自己的 `--spring.config.location`，不设这两项时按代码默认值生效（`true` / `250`）。
- **Rust 版未审计。** Rust 版 kline-proxy（market-mirror）如果用同样的方式估计服务器时间，也可能有同样的偏差。它不在本次范围内，没有检查。
