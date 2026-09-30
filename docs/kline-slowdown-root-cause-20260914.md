# 新版整点延迟调查（2026-09-14）

> 2026-09-15 补充：后续八个整点再次出现长尾，预热后的一次恢复不足以解释长期波动。新调查复现了同步资金费率缓存加载对共享虚拟线程的阻塞，并确认 `VirtualThreadPinned=0` 不能排除全部锁等待。09:00 至 12:00 迪拜时间的四轮自然采样得到 bulk p99 122、109、125、190ms，捕获到 CPU 运行排队及持续 JIT 编译；后三轮还测到 nginx 在整点约半秒内消耗 390/410/420ms CPU。12:00 变慢伴随合约 ready 推迟约 24ms、收盘等待日志推迟 53ms，且 JIT 与等待阶段重叠。四轮资金费率网络等待仅 11–13ms，均未复现历史 500–600ms 峰值，其直接原因仍待慢小时现场证据。计划内采样任务均已完成退出。见[后续调查](kline-bulk-tail-investigation-20260915.md)。下文保留当时的观测与结论。

调查对象为 PID `531654`，15:26 UTC 启动的 `4b8808074930` 版本；对照为 `bc65e955300a`。两版的配置相同，生产源码差异只有 `AbstractKlineService`、`KlineSet`、`BinanceStatisticServiceImpl` 三个文件。112 个依赖 JAR 内容一致；Controller、资金费率服务、数字转换、final 等待注册器、收盘统计器和消息 dispatcher 的主类文件完全相同。bulk 查询、等待、构造响应等五个方法在去除常量池编号后字节码一致，见 [部署产物对照](../research/closed-bar-ingress/evidence/rootcause-review-20260914/jar-code-comparison.json)。另有编译生成类差异，例如匿名类捕获字段的赋值顺序；`IntervalBoundaryGuard` 的指令一致。

结论：**最符合现有证据的是发布重启后的低频 HTTP 路径预热不足，叠加整点 CPU 和线程调度资源竞争。** 同样输入下未复现三项优化导致的直接计算性能回归；实际 HTTP 热路径字节码也一致。经过普通时段的只读 POST 回放，未换包、未重启的同一进程在 19:00 UTC / 23:00 迪拜时间恢复到 bulk p99 **92ms**，低于旧版 19:00 的 287ms。

JIT 确实占用整点资源，但不能把此前三个小时的全部回退归给 JIT：那三次没有 JFR，而且小时之间的请求到达时序不完全相同。当前自然整点已经留存 CPU、调用栈、锁事件和应用日志；其结论见下文。

## 已确认的现象

下表沿用此前的 nginx 分位数算法：排序后取 `floor((n-1)*q)`。时间为迪拜时间，同一天；全部 bulk 请求均为 HTTP 200，每个接口每小时 193 次。

| 指标 | 19:00 旧版 | 20:00 新版 | 21:00 新版 | 22:00 新版 | 23:00 新版 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 合约全部 ready，整点后 ms | 188.279 | 248.578 | 211.702 | 178.212 | 164.992 |
| 现货全部 ready，整点后 ms | 129.543 | 274.301 | 105.298 | 217.875 | 136.882 |
| K 线 bulk HTTP p50，ms | 28 | 415 | 332 | 250 | 12 |
| K 线 bulk HTTP p99，ms | 287 | 875 | 821 | 660 | 92 |
| 资金费率 bulk HTTP p50，ms | 16 | 408 | 328 | 242 | 10 |

22:00 的合约数据已经比 19:00 更早就绪，但 bulk 尾延迟仍然高。资金费率与 K 线接口的中位数同时上升、同时回落，表明问题不局限于收盘消息何时到达。

应用日志也显示，请求没有在 final 后及时完成续跑：20:00 最晚的 `BULK_FINAL_WAIT` 日志在 T+918ms，21:00 在 T+884ms，22:00 在 T+704ms。对应合约 final 最晚约 T+249、212、178ms。`waited_ms` 包含等待线程重新得到执行机会的时间，不能全部解释为等待币安数据。它也不包括随后完整的响应构造和发送。

此前 19:00、20:00、21:00 的 T-5 到 T+10 秒没有 GC 暂停。合约上游事件最晚时间基本稳定在 T+105–108ms。现货时钟校准偏移和网络到达时间有波动，不能把其 ready 差值全部归因于 Java 代码。

## 23:00 自然整点验证

同一 PID、同一 JAR、同一业务配置。19:00 UTC 整点没有加入观测 HTTP 请求；两个 bulk 接口各 193 次，均为 200。此前普通时段共做了 584 次 K 线 bulk 和 272 次资金费率 bulk 只读查询，因而这次的 HTTP 预热状态已改变，不能将恢复全部归因于自然运行时间增长。

- 合约 718 个、现货 491 个均完整 final，无 ingress failure、背压或统计溢出。合约 ready 为 T+164.992ms，现货为 T+136.882ms。
- K 线 bulk p50 **12ms**、p99 **92ms**，资金费率 bulk p50 **10ms**、p99 **41ms**。
- 15 条 `BULK_FINAL_WAIT` 日志全部在 T+143–173ms 完成，最长 `waited_ms=86`。之前 final 已到、请求却到 T+700–900ms 才结束等待的现象没有重现。
- 整点第一秒约 **650ms JVM CPU**，其中 C2 约 80ms、C1 约 20ms，合计约 **15.4%**；前 300ms 为约 320ms JVM CPU、70ms 编译 CPU，约 **21.9%**。仍有 JIT 工作，不能把“有编译活动”等同于“异常编译风暴”。
- T-5 到 T+10 秒没有 GC 暂停，整段录制没有 `VirtualThreadPinned` 事件。整点第一秒 HTTP monitor 等待最长约 **4.7ms**，日志写入有一次约 **6.6ms**；收盘统计器在消息线程上有一次约 **12.1ms** 的 monitor 等待，没有捕获到数百毫秒的长锁。
- 首秒 55 个 HTTP Java 执行样本中，12 个经过 `tradingSymbolsOrNull`，6 个经过数字格式化。这两项仍是后续减少 CPU 的具体位置。它们在旧版也存在。

CPU 采样步长约 20ms、ticks 精度 10ms，各线程读数不是原子快照。上述首秒 JIT 比例与普通时段回放的 28%–30% 来自不同负载、不同窗口，不能据此计算严格的 JIT 降幅。也没有旧版该小时的同口径 CPU 轨迹。

[整点应用及 HTTP 汇总](../research/closed-bar-ingress/evidence/rootcause-review-20260914/hour19-summary.json)、[CPU 差分](../research/closed-bar-ingress/evidence/rootcause-review-20260914/hour19-cpu-summary.json)、[首秒执行栈和等待事件](../research/closed-bar-ingress/evidence/rootcause-review-20260914/hour19-first-second-events.json)。

## 普通时段现场采样

18:18–18:28 UTC 做了三次有自动终止时间的 JFR，以及有明确请求总数上限的只读查询。没有重启应用、修改 JVM 参数或改变业务配置。

单次 6 币、2 根 1h bulk 请求中位数约 1.9ms；16 并发约 14.7ms。随后按客户端同时请求 K 线和资金费率的方式回放，64 组并发的 K 线 p99 约 68ms。完整 192 组回放中：

| 现场回放 | K 线 p50 | K 线 p99 | JIT / JVM CPU |
| --- | ---: | ---: | ---: |
| 192 个 K 线请求 | 119.9ms | 约 211ms | 约 28% |
| 192 个 K 线 + 192 个资金费率请求 | 154.6ms | 约 204ms | 约 30% |

CPU 比例取自 `/proc` 的编译线程 CPU ticks 与进程 CPU ticks，区别于 JFR `Compilation.duration` 的累计编译耗时。两个窗口约 241ms、259ms，分别有约 80ms、90ms 编译线程 CPU；ticks 精度为 10ms，窗口端点与请求首尾相差约 5–9ms，因此比例只取近似值。

调用栈反复出现：

- `queryBulkKlines → awaitJustClosedBarsFinal → tradingSymbolsOrNull → querySymbols`：每个独立 bulk 请求扫描完整交易品种列表并构造集合。
- `buildBulkKlinesResponse → ConvertUtil → DecimalFormat`：重复格式化各请求需要的 K 线。
- Spring POST 参数处理、JSON 解析及 Caffeine 写入路径的编译事件。

192 个 K 线请求那一批有 42 个 HTTP Java 执行样本，其中 15 个经过 `tradingSymbolsOrNull`，8 个经过数字格式化；混合批次分别是 36 个中的 7 个和 8 个。这些是 Java 调用栈采样计数，不是整个 JVM 的 CPU 占比，也不是稳定的长期百分比。

普通时段采样没有捕获到虚拟线程 pinning，也没有捕获到 ≥1ms 的 HTTP monitor 竞争。采样时实例有两个虚拟线程 carrier；资源不足时，已经被唤醒的请求仍需等待执行。虚拟线程的调度与 pinning 机制见 [OpenJDK JEP 444](https://openjdk.org/jeps/444)。后续自然整点的锁事件已单独列在上文。

**测量限制：** 回放客户端也在实例上运行，会与服务共享双核；JFR 也有自身开销。这些回放不是此前真实整点的精确复现，不能声称 20:00 的延迟有 30% 来自 JIT。

## 两版受控对照

在独立 JVM 中使用相同输入和保留历史，分别加载部署前、部署后的类，避免修改运行中实例的数据。

### 收盘处理与等待

每版 20 轮预热、20 轮测量；每轮 718 合约 + 491 现货收盘，192 个虚拟线程请求，每个请求 6 币、4 根 K 线。

| 测量轮次的中位数 | 旧版 | 新版 |
| --- | ---: | ---: |
| 全部收盘处理完成 | 132.202ms | 132.245ms |
| 每轮最晚 bulk 完成 | 132.863ms | 132.963ms |

全部 80 轮均验证：1,209 条 final 完整处理、数值正确、无遗留 pending、无 ingress failure 或丢弃。这里的收盘完成测点在处理函数返回后，不能与生产日志的 ready 测点直接混用。

### 完整 POST 路径

使用实际 Tomcat、Spring MVC、生产 Controller 和 Service，填入固定行情与资金费率数据，不启动 WebSocket、REST 补数和定时持久化。每版连续回放 10 批，每批 192 个 K 线 POST + 192 个资金费率 POST；两批间隔 1.2 秒。

| 384 个请求的中位数 | 旧版 | 新版 |
| --- | ---: | ---: |
| 首批 | 136.9ms | 136.6ms |
| 第十批 | 41.4ms | 40.5ms |

全部 7,680 次请求为 200；3,840 个 K 线响应均为 finalized，包含预期的 6 币 × 4 根，`waited_ms=0`。两版均有显著预热成本，未显示新版的请求构造路径直接退化。

**测量限制：** 以上隔离实验在本地 macOS / aarch64 / JDK 21.0.8 运行；`ActiveProcessorCount=2` 控制 JVM 的并行度选择，没有将主机物理 CPU 限制为两核。它不能复现生产 Linux / x86_64 / JDK 21.0.2 的绝对延迟。尝试的 Docker 双核实验没有成功启动，未使用其结果。

## 解释与后续方向

这轮修改主要减少后台重复工作，没有消除上述 HTTP 热点。部署重启清空了已编译代码及 HTTP 路径的运行画像；每小时只有约 193 个 K 线 bulk POST，运行数小时不意味着这一条低频路径已经充分预热。现场 `Compiler.codelist` 中，bulk 主方法仍有 C1 版本，完整回放又触发了 POST 路径的 C2 编译。

因此，当前证据支持“发布重启后的预热差异，加上整点共享 CPU 和线程调度资源竞争”的解释；尚不能用这些证据分配此前每个整点的全部额外毫秒数。23:00 已恢复，并不代表之后每一小时都保证低于 100ms。

下一步优先补上部署流程的真实 POST 参数绑定与响应序列化预热——此前的数据完整性和接口烟测不足以覆盖这一点。然后减少 bulk 每次重建交易品种集合，并复用未变化 final K 线的展示编码。后两项需要分别保证交易状态刷新和 final 修正后的失效正确性。直接关闭 JIT、调整编译器线程或切换 JDK 并不是已验证的修复方案。

本轮没有修改生产业务代码、配置或启动参数；诊断录制已经停止，定时 CPU 采样程序已退出，应用仍为原 PID，自动重启次数为 0。

## 证据

[调查汇总](../research/closed-bar-ingress/evidence/rootcause-review-20260914/summary.json)、[HTTP 两接口逐小时对照](../research/closed-bar-ingress/evidence/rootcause-review-20260914/http-route-hour-summary.json)、[现场回放线程 CPU](../research/closed-bar-ingress/evidence/rootcause-review-20260914/fullbatch-cpu-summary.json)、[调用栈与编译事件](../research/closed-bar-ingress/evidence/rootcause-review-20260914/fullbatch-phase-summary.json)、[POST 逐轮结果](../research/closed-bar-ingress/evidence/rootcause-review-20260914/post-warmup-summary.json)。

原始材料在实例 `/opt/kline-proxy/deployments/20260914_lowrisk_4b8808074930/rootcause-review-20260914-1817` 和本地 `/tmp/kline-rootcause-review-20260914-1817`；JFR 文件的大小和 SHA-256 见 [manifest](../research/closed-bar-ingress/evidence/rootcause-review-20260914/jfr-manifest.json)。
