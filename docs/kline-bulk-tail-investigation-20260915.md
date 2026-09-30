# bulk 长尾进一步调查（2026-09-15）

本轮确认了两件事：**夜间的长尾主要发生在连接建立后、Tomcat 返回响应头之前；资金费率的同步缓存加载存在可复现的跨接口阻塞机制。** 线上已捕获该加载路径持锁等待网络，但尚未捕获到夜间 500–600ms 峰值发生时的完整调用栈，因此不能把所有慢小时都归因于它。

**09:00、10:00、11:00、12:00 迪拜时间的四个自然整点均已完成采样并分析，K 线 bulk p99 分别为 122、109、125、190ms。** 四轮都没有复现历史 500–600ms 峰值。新证据确认整点存在 CPU 运行排队，JIT 仍在编译，nginx 也消耗了可观的 CPU；资金费率同步缓存加载内的网络等待分别仅 **11.512、12.712、11.325、12.255ms**。12:00 较前一小时变慢，与行情就绪推迟及 CPU 调度竞争的观测一致，但仍缺逐请求分解，无法精确分摊新增的 65ms。全部计划采样已于 12:01 完成，任务已退出。

此前“预热后曾恢复”仍是观测事实，但不能作为后续波动的充分解释。当前应重点测量共享虚拟线程调度器的锁等待、运行排队与 CPU，而不是继续默认归因于 JIT。

## 历史慢请求卡在哪个阶段

重新提取了 09-14 15:00 至 09-15 04:00 UTC 共 14 个整点分钟的 nginx POST 记录，每个 bulk 接口每小时均为 193 次。新增比较 `uct`（上游建连）、`uht`（上游响应头）与 `rt`（请求总耗时），分位数沿用 `sorted[floor((n-1)*q)]`。

下表每行取该小时 K 线 bulk 排序后位于 p99 位置的**同一条请求**，不是将不同分位数相减。时间为迪拜时间，单位 ms。

| 小时 | 该请求总耗时 | 建连 | 建连后至响应头 |
| --- | ---: | ---: | ---: |
| 00:00 | 513 | 1 | 511 |
| 01:00 | 636 | 1 | 636 |
| 04:00 | 591 | 0 | 591 |
| 08:00 | 347 | 2 | 345 |

nginx 毫秒计时可能有约 1ms 的字段差异，例如 01:00 该请求的 `uht=637ms`、`rt=636ms`。00:00–07:00 的 277 条 ≥300ms K 线请求中，建连最大仅 8ms；资金费率的 192 条 ≥300ms 请求建连最大为 9ms。

因此，这批历史长尾不是大响应体慢慢传输或一秒级建连重试导致的；延迟位于建连后等待 Tomcat 返回响应头的阶段，包含接收、调度、业务执行、内部等待和响应编码，不能直接等同为 CPU 计算时间。

各小时约 409–411KB 的 K 线大响应在整点后第 3–6 秒才返回，耗时约 23–32ms，并不与第一秒的慢请求重叠。01:00 合约在 T+154.742ms 全部 ready，但最后的 `BULK_FINAL_WAIT` 到 T+668ms 才打印；04:00 对应为 T+217.644ms 和 T+642ms。这证明 `waited_ms` 不能全部解释为等待上游收盘消息。

GC 的已有检查仍成立：八个整点的 T-5 至 T+10 秒窗口只有一次约 24ms GC，整个夜间单次暂停最大约 34ms。当前 cgroup 没有 CPU quota，父级 `nr_throttled=0`。这些证据不支持将数百毫秒长尾解释为 GC 长暂停或配置的 CPU 配额限流。

## 已复现的具体机制：缓存加载占住共享工作线程

`BinanceFutureExchangeServiceImpl.loadChunk` 在 `historicalChunkCache.get(key, loader)` 的同步 loader 中调用币安 HTTP 接口；该 loader 位于 Caffeine / `ConcurrentHashMap.compute` 的同步区内。资金费率定时预热还会外套一层 `bulkFundingRecentCache.get`。

当新窗口未命中缓存时，一条虚拟线程持有缓存锁等待网络，其他资金费率请求争抢同一缓存条目的锁。Java 21 上这些等待可能占住有限的 carrier（实际执行虚拟线程的底层工作线程），使不依赖资金费率数据的 K 线请求也无法及时运行。[Java 21 虚拟线程机制](https://openjdk.org/jeps/444)。

在独立 JVM 中，用实际生产 Service、Caffeine 3.1.8 和固定行情进行测试：虚拟线程并行度为 2，两个资金费率查询争用同一缺失 chunk，同时提交 64 个 K 线查询。模拟上游用 `Object.wait(400)`，匹配线上 OkHttp 等待响应的阻塞方式；全部 K 线已收盘，不涉及等待真实币安数据。

| 对照条件，各 3 轮 | K 线请求 p99 范围 |
| --- | ---: |
| 资金费率缓存命中，共享虚拟线程 | 3.8–11.2ms |
| 缓存未命中，持锁等待模拟上游 400ms | **370.7–376.9ms** |
| 相同缓存未命中，将整个资金费率查询放在独立平台线程 | **3.1–4.6ms** |

慢的三轮中，整个案例的进程 CPU 仅约 12–24ms，K 线 Service 自身的 p99 耗时约 0.23–0.56ms，其余主要是执行前排队。所有 576 个对照 K 线响应均验证为 finalized、无 pending、每组 6 币 × 4 根。

这是机制验证，不是线上延迟预测：测试在本地 macOS / arm64 / Azul JDK 21.0.8 上运行，采用模拟 400ms 上游等待；线上为 Linux / x86_64 / Oracle JDK 21.0.2。测试设置了两个虚拟线程 carrier，没有把本地主机物理 CPU 限制为双核。它直接调用生产 Service，没有包含完整 HTTP 路由。

### 仅看 VirtualThreadPinned 会漏掉这类阻塞

上述 `Object.wait` 实验中，JFR 的 `VirtualThreadPinned` 事件为 **0**，但仍产生了约 374ms 的跨接口延迟；捕获到的是 `JavaMonitorWait` 和 `JavaMonitorEnter`。因此，“Pinned 事件为 0”不能用于排除所有占住 carrier 的锁等待。

初步实验曾使用 `Thread.sleep(400)`，同样复现延迟，但会产生 `VirtualThreadPinned` 事件。上表采用更接近线上 OkHttp 的 `Object.wait` 版本；归档的 Java 源码对应上表，避免混用两种实验结果。

## 线上已捕获的对应路径与限制

本次只读 JFR 覆盖 04:04:29–04:07:29 UTC，捕获了 08:05 迪拜时间资金费率重试中的一次 **26.402ms** 等待：

`scheduling-27（virtual） → retryBulkFundingRatesCache → Caffeine 缓存加载 → fetchHistoricalChunk → Retrofit/OkHttp → Http2Stream.takeHeaders → Object.wait`

调用栈中有两层 `ConcurrentHashMap.compute`，与代码中的两层同步缓存 loader 一致。这确认了线上确实执行“缓存加载内等待网络”的路径；该次没有慢 bulk 并发，26ms 也不足以证明它就是昨晚 636ms 的直接原因。

不同慢小时还可能有不同主因：01:00 两个 bulk 接口的 p50 都为 575ms，符合共享执行资源被占用的形态；而多个其他小时资金费率很早已返回，K 线的早期等待请求仍到 T+400–600ms 才恢复。资金费率锁只是一个已验证风险，后者仍需对照锁事件、carrier 状态与 CPU 才能归因。此前采样出现过的交易品种集合重复构造、数字格式化，仍是 CPU 热点候选。

## 突发回放发现的独立建连问题

另从实例外，通过真实 HTTPS POST 进行有上限的只读回放：16、64、192、192 组，每组一个 K 线请求和一个资金费率请求，共 928 次，全部 200。K 线每次 6 币 × 4 根，均 finalized、无 pending、`waited_ms=0`；资金费率查询使用已缓存的近期窗口。客户端耗时没有与 nginx 耗时混用。

| 同时发起的组数 | K 线 nginx p99 | 建连 p99 | 逐请求计算的建连后至响应头 p99 |
| --- | ---: | ---: | ---: |
| 16 | 7ms | 1ms | 7ms |
| 64 | 53ms | 5ms | 50ms |
| 192，第 1 轮 | 1,033ms | 1,026ms | 110ms |
| 192，第 2 轮 | 1,254ms | 1,040ms | 221ms |

这里的一秒级建连延迟已被直接测到。实例 nginx 为 1.26.3，未配置上游连接复用，Tomcat 监听队列上限现场为 100；这些是突发连接积压的具体排查点。nginx 1.26.3 默认使用 HTTP/1.0，自动启用上游 keep-alive 是后续版本的变化，不能套用新版默认配置。[nginx 官方说明](https://blog.nginx.org/blog/keep-alive-to-upstreams-is-now-default-in-nginx-1-29-7)、[上游连接与计时变量](https://nginx.org/en/docs/http/ngx_http_upstream_module.html)。

此次回放比真实整点请求到达更集中，未采集到该批次 TCP 丢弃计数的前后差值，因此没有把具体 TCP 溢出机制当作已经完全证明。尤其不能用这批回放的一秒级建连问题解释上文历史请求的 513/636/591ms：它们的建连时间很短。

## 09:00 自然整点的实际结果

observer `541382` 于 05:01:10 UTC 完成并退出。JFR 实际录制约 75 秒，原文件约 1.8MB；T-3 至 T+10 秒的 `/proc` 采集得到 646 个样本。没有加入额外 bulk 流量。09:12 迪拜时间复查，服务仍为 PID `531654`、`NRestarts=0`、健康状态 `UP`，该 JFR 已自动停止。

下表为 2026-09-15 迪拜时间；ready 是整点后的最大延迟，bulk 为各整点完成分钟内 193 条 POST 的 nginx 请求耗时。单位 ms。

| 指标 | 08:00 | 09:00，含诊断采样 |
| --- | ---: | ---: |
| 合约全部 ready | 186.020 | **146.438** |
| 现货全部 ready | 150.581 | **109.131** |
| K 线 bulk p50 | 46 | **14** |
| K 线 bulk p99 | 347 | **122** |
| 资金费率 bulk p99 | 94 | **37** |

09:00 两个 bulk 接口各 193 次，全部 HTTP 200；合约 718/718、现货 491/491 的收盘消息全部到齐，没有 closed-bar 丢弃或队列溢出。21 条 `BULK_FINAL_WAIT` 的等待最大为 117ms，最后一条日志在 T+174ms，全部 `pending_after=0`。HTTP 200 本身不代表最新一期资金费率已发布。

### 锁等待：路径存在，但本轮没有长时间堵塞

T+1ms，虚拟线程 `scheduling-28` 在资金费率定时预热中进入 `loadChunk`，调用栈包含两层 `ConcurrentHashMap.compute`，随后在 OkHttp 的 `Http2Stream.takeHeaders → Object.wait` 等待 **11.512ms**。第一秒没有捕获到资金费率缓存路径上达到 1ms 门槛的 `JavaMonitorEnter` 竞争事件。

第一秒捕获到的 monitor enter 最长 **5.306ms**，来自 Tomcat Poller/Acceptor 的 selector 同步；收盘监测器另有约 1.5–4.7ms 的锁等待。没有观察到数百毫秒的同步锁等待。`VirtualThreadPinned=0` 仍不能单独用于排除所有 carrier 阻塞，但这次更完整的 monitor wait/enter 记录也没有显示资金费率造成长堵塞。

### CPU：已捕获到运行排队，JIT 也确实参与

取实际样本 T+4.128 至 T+551.944ms，共 **547.816ms**：

| 观测量 | 结果 |
| --- | ---: |
| 整机 CPU 忙碌比例 | **94.5%** |
| JVM 消耗 CPU，按进程 ticks | 约 **530ms** |
| C1 + C2 实际被调度运行时间，按 schedstat | **102.390ms** |
| carrier `537725` 运行 / 在运行队列等待 | **110.643 / 250.424ms** |
| carrier `539242` 运行 / 在运行队列等待 | **110.594 / 245.908ms** |
| 同窗口 CPU steal | 0 ticks |

Linux `schedstat` 第二个字段是线程已可运行、但在运行队列等待 CPU 的累计时间，区别于等待网络或锁。[Linux 调度统计定义](https://www.kernel.org/doc/html/v6.12/scheduler/sched-stats.html)。两个 carrier 的排队分别约 250ms 和 246ms，说明整点 CPU 调度竞争确实发生；这些是**跨多个请求的线程累计值，不能相加成单个请求的 p99**。

该窗口 JIT 占 JVM CPU 约 19%，占整机忙碌 CPU 约 10%；进程已连续运行约 13.6 小时，仍会发生新的编译。JFR 在第一秒记录到 9 次编译，其中 C2 的 `queryBulkFundingRates` 从 T+363ms 开始，历时 125.115ms；`normalizeFundingSymbols` 从 T+489ms 开始，历时 55.471ms。**编译事件历时包含等待调度，不是纯 CPU 耗时，更不是同等长度的全 JVM 暂停。** 最大的这次 C2 编译开始于最后一条收盘等待日志之后，不能用于解释此前已结束的收盘等待。

因此，证据支持“CPU 运行排队存在，JIT 会参与争用”，不支持“完全没有 JIT 影响”，也不支持“此前所有 500–600ms 峰值都是 JIT”。本次还未采集其他进程的独立 CPU，整机忙碌量中属于 JVM 以外的部分不能直接归给 nginx。

第一秒的 23 个 Java 执行样本中再次出现交易品种集合构造、`DecimalFormat` / `ConvertUtil` 数字格式化和 Spring/Tomcat 请求处理。样本数不足以估计稳定的热点占比，暂作为后续优化候选。

### GC 和建连：本轮没有出现对应长尾

T-5 至 T+10 秒没有 GC 暂停；第一秒的 Cleanup safepoint 约 **3.870ms**。录制内的一次约 20.624ms GC 在 T-8 秒，未与整点请求重叠。

K 线 bulk 建连 p99 为 **13ms**，最大 14ms；资金费率建连 p99 为 **10ms**，最大 16ms。K 线处在 p99 位置的同一请求为 `rt=122ms, uct=1ms, uht=122ms`。采样覆盖的 13 秒内，主机 `ListenOverflows`、`ListenDrops`、`TCPSynRetrans` 增量均为 0，没有重现此前集中回放的一秒级建连问题。

### 结论边界与后续采样

这次自然整点提供了一个较快小时的完整对照，**历史 500–600ms 峰值的直接原因还未定案**。nginx 时间戳只有秒级且缺少与 JFR 对应的请求 ID，无法将某一个 HTTP p99 精确拆分为收盘等待、锁等待、CPU 排队和计算时间。ready 指标还使用了交易所时钟修正（本轮合约 -5ms、现货 -6ms），与 JFR 的主机时间对齐时需保留这几毫秒差异。采样自身开销未单独量化；JFR 中没有 `CPULoad` 事件，本节 CPU 结论完全来自 `/proc` 原始计数。

已于 09:17:59 迪拜时间启动固定三轮的后续自然整点观测：**10:00、11:00、12:00**，series PID `542002`，第一轮 observer `542003`。每轮为 75 秒有界 JFR 和约 13 秒 CPU 采样，额外记录 nginx 与采样器进程的 CPU ticks，不产生额外 bulk 请求。输出分别为实例证据目录下的 `natural-hour06/07/08`。三轮均完成，series 及最后一轮 observer `542803` 已于 12:01 后退出；后续结果见下文。

若资金费率缓存机制在慢整点被确认，修复方向是将同步网络加载及其等待移出共享 carrier 的监视器阻塞路径，并保留同一小时所有请求共享同一资金费率快照、失败重试和失效语义。不能只把网络调用搬到线程池、却让 HTTP 线程继续持有缓存锁等待。减少交易品种集合重建与重复格式化，也可作为独立的 CPU 优化继续验证。

## 10:00、11:00 的后续结果

两轮均正常完成。服务继续使用 PID `531654`，没有重启、换包或业务配置变动；11:38 复查健康状态 `UP`，已完成的 JFR 均停止。每个整点两个接口各 193 条 POST，新增 772 条全部 HTTP 200；每轮合约 718/718、现货 491/491 的收盘消息齐全，丢弃和队列溢出为 0。

| 指标，单位 ms | 09:00 | 10:00 | 11:00 |
| --- | ---: | ---: | ---: |
| 合约全部 ready，整点后 | 146.438 | 169.655 | 151.651 |
| 现货全部 ready，整点后 | 109.131 | 150.628 | 110.127 |
| K 线 bulk p50 | 14 | 12 | 12 |
| K 线 bulk p99 | **122** | **109** | **125** |
| 资金费率 bulk p99 | 37 | 46 | 44 |
| 资金费率定时预热的缓存内网络等待 | 11.512 | 12.712 | 11.325 |
| 最后一条收盘等待日志，整点后 | 174 | 187 | 180 |

两轮第一秒都没有捕获到达到 1ms 门槛的资金费率缓存 monitor enter 竞争。所有 monitor enter 事件的最长值分别为 2.638ms、5.427ms；`VirtualThreadPinned` 为 0。10:00 另有一次约 78.957ms 的现货交易品种缓存网络等待，运行在独立平台线程 `Thread-333`，不应误算为资金费率虚拟线程阻塞。

新增的 nginx CPU 计数补上了此前一处缺口。以下比较采用每轮最接近 T 至 T+550ms 的两个样本；实际覆盖 10:00 的 T-9.637 至 T+558.250ms，以及 11:00 的 T+9.301 至 T+555.189ms。**CPU 时间可以跨两核累加，与单请求耗时不同。JIT 已包含在 JVM CPU 内，不能再次相加。**

| 同一观测窗口内的计数 | 10:00 | 11:00 |
| --- | ---: | ---: |
| 窗口墙钟时长 | 567.887ms | 545.888ms |
| 整机 CPU 忙碌比例 | 90.9% | 83.5% |
| JVM CPU，进程 ticks | 约 600ms | 约 440ms |
| nginx CPU，全部 nginx 进程 ticks 合计 | 约 **390ms** | 约 **410ms** |
| 其中 JIT 实际运行，C1+C2 schedstat | **97.511ms** | **21.717ms** |
| 两个 carrier 分别累计等待 CPU | 191.352 / 257.703ms | 250.500 / 211.137ms |
| 采样器自身 CPU，进程 ticks | 约 50ms | 约 60ms |

这说明整点共享 CPU 竞争的参与者包含 nginx 和 JVM，继续只盯 Java 内部会漏掉一部分负载。当前没有 nginx 内部的 CPU 调用栈，不能进一步认定具体消耗来自 TLS、连接处理还是其他步骤。

10:00 的 JFR 还记录了 `tradingSymbolsOrNull` 和 `queryBulkKlines` 升到 C2 的编译，分别从 T+224ms 和 T+413ms 开始，历时约 150ms 和 145ms。11:00 JIT 实际 CPU 大幅下降，K 线 p99 却由 109ms 小幅升到 125ms，说明不能将 p99 按 JIT 编译时间直接推算；这些观察也不足以排除 JIT 在别的慢小时放大排队。

采样器占用约双核容量的 4.4% / 5.5%，已纳入上表；JFR 在 JVM 内的额外开销仍没有单独量化。`/proc` 文件不是原子快照，CPU ticks 为 10ms 粒度，所列进程 CPU 与整机 CPU 可能有计数误差，不能按差值精确追踪剩余负载。两个 carrier 的等待值都是跨请求累计，不代表任一 HTTP 请求延迟了 200 多毫秒。

两轮 T-5 至 T+10 秒均无 GC 暂停，11:00 只有 2.190ms Cleanup safepoint。K 线建连 p99 为 15/16ms，未出现一秒级建连；两轮 13 秒采样内 `ListenOverflows` 和 `ListenDrops` 均无增长。11:00 主机 `TCPSynRetrans` 在 T+8.263 秒增 1，发生在整点第一秒请求之后，且该计数属于全主机，不能归到 bulk。

**当前可下的结论是：连续三个小时的请求延迟已恢复到 109–125ms；资金费率缓存锁仍是可复现的风险，但没有证据将历史 500–600ms 峰值归因于它；整点 CPU 运行排队及 nginx、JIT 的资源竞争已被测到。** 三轮都属于较快样本，因此不能把它们的 CPU 竞争直接当成历史峰值的充分解释。仍缺慢峰值发生时的同口径采样与逐请求关联。

## 12:00 最后一轮结果与阶段结论

最后一轮于 12:01:11 迪拜时间完成。12:06 复查确认 series `542002` 和 observer `542803` 均已退出，JFR 没有遗留录制；服务 PID `531654`、`NRestarts=0`、健康状态 `UP`。本次共有 386 条 bulk POST，全部 HTTP 200；合约 718/718、现货 491/491 的收盘消息齐全，丢弃及队列溢出为 0。

| 指标，单位 ms | 09:00 | 10:00 | 11:00 | 12:00 |
| --- | ---: | ---: | ---: | ---: |
| 合约全部 ready，整点后 | 146.438 | 169.655 | 151.651 | **175.964** |
| 现货全部 ready，整点后 | 109.131 | 150.628 | 110.127 | **173.575** |
| K 线 bulk p50 | 14 | 12 | 12 | **14** |
| K 线 bulk p99 | 122 | 109 | 125 | **190** |
| 资金费率 bulk p99 | 37 | 46 | 44 | **51** |
| 最后一条收盘等待日志，整点后 | 174 | 187 | 180 | **233** |

12:00 K 线 p99 比 11:00 增加 65ms。与其同向变化的观测包括：合约全部 ready 推迟 **24.313ms**；合约消息接收 offset 最大值由 139ms 变为 155ms，入队等待最大值由 16.540ms 变为 38.659ms；17 条收盘等待日志的等待最大值为 **184ms**（前一小时 119ms），最后一条日志推迟 **53ms**。这些量来自不同分布和请求，不能将最大值差直接相加成 p99 的分解。日志缺少 nginx/JFR 通用请求 ID，当前只能说“等待收盘与恢复执行的延迟上升，和 HTTP 长尾上升相伴”。

资金费率定时预热的缓存内网络等待仅 **12.255ms**；第一秒没有达到 1ms 门槛的资金费率缓存 monitor enter 竞争，所有 monitor enter 中最长 **8.191ms** 来自收盘监测器。没有证据显示本轮 190ms 长尾由资金费率网络长等待造成。

T-5.266 至 T+545.045ms，实际 **550.310ms** 的采样窗口内：

| 观测量 | 12:00 |
| --- | ---: |
| 整机 CPU 忙碌比例 | **95.4%** |
| JVM CPU，进程 ticks | 约 **560ms** |
| nginx CPU，进程 ticks 合计 | 约 **420ms** |
| JIT 实际运行，C1+C2 schedstat，已包含在 JVM CPU 中 | **90.745ms** |
| 两个 carrier 分别累计等待 CPU | **253.895 / 250.270ms** |
| 采样器自身 CPU，进程 ticks | 约 **60ms** |

这次第一条较长的 C2 编译从 **T+97ms** 开始，与行情收盘及请求等待阶段重叠：`DelayedWorkQueue.add` 编译历时 170.959ms，接着 `DelayedWorkQueue.offer` 从 T+269ms 开始，历时 122.468ms。编译历时包含 CPU 排队，不能当作 JVM 暂停时间；本轮只有约 90.745ms 的编译线程实际运行量。相比 11:00，这为“编译工作与请求争用 CPU”提供了更直接的时序关联，但没有无编译的同负载对照，无法把新增的 65ms 全归给 JIT。

T-5 至 T+10 秒没有 GC 暂停，第一秒 Cleanup safepoint 为 **3.879ms**。K 线建连 p99 为 **12ms**，p99 对应的同一条请求为 `rt=190ms, uct=0ms, uht=189ms`，主要等待发生在建连后至响应头阶段；nginx 计时约有 1ms 字段差异。整个 13 秒 CPU 采样窗口内，`ListenOverflows`、`ListenDrops`、`TCPSynRetrans` 均无增长。捕获到的一次文件写入仅 2.781ms，未看到长时间文件写入。

采样器占用了约双核容量的 5.5%，JFR 在 JVM 内的开销仍未单独量化。本节 CPU 值描述的是有诊断采样时的状态；进程和线程 CPU 为累计量，不是单请求延迟。

**四轮采样后的阶段结论：整点收盘数据到达与处理时间存在波动，nginx、JVM 业务和 JIT 会共同竞争两核 CPU，CPU 运行排队已直接测到；资金费率同步缓存加载的跨接口阻塞仍是隔离实验已复现的风险，本轮未抓到它造成线上长尾。** 四轮都未重现此前 500–600ms 峰值，因此历史峰值的直接原因仍未定案。所有计划内诊断任务已结束，没有继续排队的定时采样。

## 证据

[汇总与阶段统计](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/summary.json)、[逐请求 nginx 历史阶段](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/nginx-connect-history.json)、[线上资金费率等待调用栈](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/online-funding-wait.json)、[隔离实验源码](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/FundingPinningHarness.java)、[隔离实验结果](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/monitor-wait-local.json)、[实验锁事件](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/monitor-wait-local.jsonl)、[有界观测脚本](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/observe_next_hour.py)、[JFR 文件清单及 SHA-256](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/jfr-manifest.json)。

09:00 新增：[计算汇总](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour05/analysis.json)、[原始 CPU 样本](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour05/cpu.json)、[所选 JFR 性能事件](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour05/selected-events.jsonl)、[nginx 阶段和任务退出检查](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour05/nginx-and-health.json)、[本轮文件校验清单](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour05/manifest.json)、[复算脚本](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/analyze_natural_hour.py)、[后续三轮启动回执](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/series-start.json)。

10:00 / 11:00 新增：[10:00 计算汇总](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour06/analysis.json)、[11:00 计算汇总](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour07/analysis.json)、[10:00 JFR 性能事件](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour06/selected-events.jsonl)、[11:00 JFR 性能事件](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour07/selected-events.jsonl)、[11:38 后续任务状态](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/series-status-1138.json)。每个目录保留相应 CPU、小时统计、nginx 和 SHA-256 清单；原始 JFR 文件沿用观测脚本的 `natural-hour05.jfr` 文件名，实际小时由所在目录、JFR 启动参数及事件时间校验，三轮文件没有覆盖。

12:00 新增：[计算汇总](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour08/analysis.json)、[所选 JFR 性能事件](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour08/selected-events.jsonl)、[nginx 阶段与任务退出检查](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour08/nginx-and-health.json)、[文件校验清单](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/natural-hour08/manifest.json)、[三轮任务完成回执](../research/closed-bar-ingress/evidence/bulk-tail-review-20260915/series-status-complete.json)。

原始 JFR 留在实例或本地 `/tmp/kline-tail-review-20260915`，仓库中只保留选出的性能事件及脱敏 HTTP 记录。04:06:22–04:07:27 的 CPU 样本没有覆盖 04:08 的公网回放，没有用于计算该回放的 CPU 或 JIT 占比。

## 13:00 Nginx 独立排查补充

后续的独立 Nginx perf / TLS 被动采样已捕获到整点第一秒 **383 次新 TLS 1.3 握手，全部未恢复会话**；378 条连接在同秒由客户端先发出 TCP 关闭信号。约 550ms 内 Nginx 消耗 **463ms CPU**，约一半 CPU 样本的调用栈包含 TLS 握手。这轮 K 线 / 资金费率 bulk p99 为 **127 / 26ms**。详见 [Nginx 整点 CPU 排查](nginx-hourly-cpu-investigation-20260915.md)。这补上了 Nginx 内部调用栈证据，不是此前四轮相同口径的 JFR 采样，也不能将 Nginx CPU 直接换算成历史 p99。
