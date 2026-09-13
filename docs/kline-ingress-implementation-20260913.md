# Kline ingress 优化实现与验收（2026-09-13）

部署状态更新：`407afae` 已于 2026-09-13 17:05 UTC 上线，17:09 完成验收；18:00 首个生产整点的合约全部 ready 为 T+200.545ms。实际 HTTP、CPU 和持久化结果见[部署实测报告](kline-ingress-deployment-20260913.md)。下文保留实现阶段的回放与验证记录。

这次优化以 `1025516` 为基线，保留普通 K 线与连续合约 K 线的两个独立协议实现，减少整点收盘统计和重复 forming 消息占用的 CPU，并修复并发更新导致 final 数据回退的问题。`useContinuousKlineStream` 的默认值仍为 `false`，它只选择订阅协议，不控制调度优化。

工作窗口从 14:47:54 UTC 开始，用户要求两小时内自行循环优化。各轮修改、测试和测量记录在 [OPTIMIZATION_LOG](../research/closed-bar-ingress/OPTIMIZATION_LOG.md)；实现前的定位过程和基线反例见 [研究报告](kline-ingress-optimization-20260913.md)。本次未修改策略、币池、交易过滤或收益计算。

**已经落地的改动**

| 组件 | 最终行为 | 解决的问题 |
| --- | --- | --- |
| `BinanceKlineStream` / `BinanceContinuousKlineStream` | 协议解码和 symbol 映射独立；共享轻量 header 扫描，输出统一的市场、symbol、interval、openTime | 普通/连续流切换时，同一序列仍使用同一存储和调度键；Netty 分类只读已发布的连续合约路由，不请求 exchange REST |
| `KlineMessageDispatcher` | 按市场、symbol、interval 分片；每个分片一个 writer；forming 槽包含 openTime | 同序列按单 writer 执行，旧小时与新小时互不覆盖 |
| 收盘队列 | 每条 `x=true` 单独入 FIFO，优先处理；连续 64 条 final 后给 forming 一次机会 | 不再用 `DiscardOldest` 丢弃未解析的收盘任务；重复 final 也逐条执行 |
| forming 队列 | 只合并尚未开始执行、同一 bar 的更新；已有 final 可替代排队的 forming | 大量中间快照不再完整解码、写缓存、更新业务指标；没有额外批处理等待时间 |
| `KlineSet.commit` | WS、REST、restore、补洞都经同一原子提交锁；先发布数据，再发布 final 标记 | 修复相同成交笔数 final 不写入、旧 forming 并发覆盖新值、final=true 却保留旧价格三类反例 |
| `ClosedBarArrivalTracker` | 每个边界准备 expected/pending，逐币移除；完成前和维护时复核币池 | 消除每条收盘消息扫描全部缓存的 O(N×M) 工作；保留新币、下架和状态改变的核对 |
| `FinalBarWaitRegistry` | 只通知等待该 bar 的 bulk 请求；注册后重查，版本计数保留检查期间到达的通知 | 避免每条 final 唤醒所有查询；超时、取消和完成均清理订阅；25 ms 轮询只作为缓存移除的兜底 |
| 单条 WS 更新 | 当前、相邻和已存在历史 bar 直接提交；确有时间缺口时才进入批量补洞 | 省掉常见路径的列表、集合、排序和补洞准备 |
| 持久化 | 提交锁内复制已确认 final 的引用，再在锁外编码、落盘；新增 final 即使值未变也标脏 | 时间已过的 forming 和 WS 补洞占位值不会在重启后被当成确认收盘；REST 补洞维持原有闭合规则 |
| 停机 | 停止客户端、阻止自动重连、排空已接收的通用/行情任务，再执行服务持久化销毁 | 消除销毁服务时队列仍有收盘任务的顺序缺口 |
| 分配与资源 | forming 不创建收盘诊断对象、不重复更新已记录的 topic 心跳；版本时间戳使用 primitive；释放接收及发送路径遗留的 Netty buffer 引用 | 减少高频分配和断连、关闭时的直接内存泄漏 |

分类失败、未知封装、控制消息走可靠的通用队列，不进入 forming 合并。通用队列也采用有界背压。协议共享的数据对象位于 `model`，模型不依赖 dispatcher 实现。收齐日志目前只在完成/超时执行汇总；最终一次排序和日志仍在触发汇总的线程上，并未宣称实现了独立异步日志管线。

**提交和排序约定**

forming 不能覆盖已 final 的 bar。第一条 WS final 可以替代同成交笔数的 forming；后续更新按成交笔数 `n`、双方都存在时的事件时间 `E`、本进程接收序号判断旧版本。`E` 不是唯一版本号；普通流的 trade ID 与连续流的 update ID 没有被混作一个全局序号。未知时间戳和数值零有明确区分。REST/restore 不会用相同笔数的旧数据覆盖已有直播版本；REST 可补缺失 bar。

有效 final 修订会增加 bulk 缓存版本并失效旧响应，防止之前缓存的收盘价格继续被服务。版本元数据与 retained bar 一起裁剪，保留期间迟到的旧 final 仍有比较依据。这里的“最新 forming”指上述排序规则下的最新有效快照。

**同条件回放结果**

环境：macOS arm64，Zulu JDK 21.0.8，`-Xms1g -Xmx2g -XX:ActiveProcessorCount=2`，每个场景单独启动 JVM。`ActiveProcessorCount=2` 只改变 JVM 判断的处理器数量，**不是 CPU 配额或绑核**。不能把本地约 +133 ms 的结果当作生产延迟承诺。

回放真实的解析、协议适配、提交、收齐统计、bulk 和指标代码；718 合约币、490 现货币，每币 1h/1d 各 1,000 根历史。输入为合成消息；没有网络/Netty 解帧、HTTP 响应序列化、实际磁盘写入、ticker 负载或 1d 实时帧。INFO/WARN 输出关闭，收盘诊断采样开启。CPU 为回放窗口内全进程 CPU，含 JIT/GC 和启用的 reader；输入构造在计时窗口外。

下面一组充分预热的对照为 **100 轮预热 + 50 轮测量**。每轮普通流量 3,624 帧，其中 1,208 条 final，最晚注入在边界后 131 ms。表格为逐轮中位数；实现列为第七轮，后续原始时间戳存储改动另做源码快照复核。

| 场景 | 基线 CPU ms/轮 | 优化 CPU ms/轮 | 最后 handler / bulk 返回（边界后 ms） |
| --- | ---: | ---: | --- |
| 普通流量 | 207.6 | 72.6 | handler：142.5 → 133.7 |
| 96 个不同 6 币集合的 bulk 查询 | 193.7 | 112.1 | bulk：138.1 → 131.8 |
| 优化代码、定向通知前后的 bulk 对照 | 124.7 | 112.1 | bulk：132.0 → 131.8 |

普通流量全进程 CPU 约降 65%，bulk 场景约降 42%。CPU 在不同 JVM、预热阶段和 GC 周期之间有波动；短轮初测与这组充分预热结果均保留，不挑选最小一次当收益。定向通知前后还有形成中诊断分配等小改动，因此这 10% 差异不是该组件单独的精确因果效应。

突发回放每轮 49,528 帧，包含 48,320 条 forming 和 1,208 条 final，不做网络节流。充分预热后优化版本 CPU 中位数 85.3 ms；约 35,153 条 forming 在队列阶段被合并，约占 73%；每一轮所有 final、最终收盘值和最新 forming 值均正确。基线突发场景会丢 final 和出现错误值，不能把它和正确完成全部工作的方案当作等量吞吐比较。

逐轮范围、计数及 warmup 标记见 [steady-summary](../research/closed-bar-ingress/evidence/implementation/steady-summary.json) 和同目录原始结果。最终源码快照的追加复核写入本报告末尾。

**可靠性与长期运行验证**

收盘交付计数和缓存值分别校验。测试包括：相同 `n` 的 final、并发旧 writer、重复/修订 final、乱序 `E` 与接收序号、REST/restore 冲突、两个小时和周期、普通/连续封装、未知字段与错误分类回退、容量耗尽、被中断的生产者、停机排空、等待者注册竞态与清理、补洞和持久化。

16 个并发生产者交错写入现货/合约、1h/1d、多个 openTime。每分片 final/forming 容量都设为 2，closed key 容量设为 1 强制回收；所有 final 完整执行，缓存最终值与独立排序预期相符。Spring 上下文测试确认 60 个注册客户端共享注入的 dispatcher，配置可以绑定，并验证 final 在持久化销毁之前处理完。

第七轮长期回放包含 20 轮预热和 **1,200 个测量小时，1,449,600 条测量 final**，所有轮次计数、缓存和队列排空校验通过。每序列最多 1,050 根 bar；达到保留上限后版本数随窗口回收；没有游离于 bar map 的版本和 final 标记。[长期回放证据](../research/closed-bar-ingress/evidence/implementation/soak-summary.json)。这验证的是连续边界和保留生命周期，不是 50 天真实网络连接稳定性。

版本元数据有内存成本。第七轮 1,000 小时后的强制 GC 堆约为 675–682 MiB，数量随 1,000–1,050 根保留窗口摆动。第八轮用 JDK Instrumentation 测量：带独立 `Long` 的版本记录共 48 字节，primitive 时间戳加 presence bit 为 32 字节；每个有事件时间的保留版本少 16 字节，约 126 万版本时结构上少约 19 MiB，未把这个结构推算当作精确全进程 RSS 改善。[测量代码和结果](../research/closed-bar-ingress/evidence/implementation/version-footprint.json)。

**配置与观察口径**

```yaml
kline:
  ingress:
    workers: 4
    finalQueueCapacityPerWorker: 2048
    formingQueueCapacityPerWorker: 4096
    closedKeyCapacityPerWorker: 2048
    coalesceFormingUpdates: true
```

容量均为每个 worker 的上限；closed key 缓存满了只回收加速标记，存储层仍保护 final。关闭合并会逐条处理 forming，同时保留有界队列和 final 保护。默认 4 个 worker 沿用此前并发规模；未依据没有真实限核的本机回放直接调整生产线程数。

Prometheus 新增 `websocket_kline_ingress_*`：`received_total{closed}` 为接纳数量，含等待容量的生产者；`processed_total{closed}` 为完成的 handler **尝试**，必须同时看 `failures_total`；`coalesced_total{reason}` 区分被替代与旧 forming；`queue{closed}` 不含正在执行或等待容量的生产者；另有背压次数/秒数和活动 worker。形成中业务处理指标下降不等于断流，topic 心跳在收帧时更新。

正常饱和时，已接纳 final 不会因队列满而被丢弃；生产者会等待容量。处理异常会明确记录并计入失败，不会伪装成成功提交。仍是内存队列，没有 WAL，不能保证进程崩溃、强杀或停机排空超时后恢复每条原始消息。现有 REST 补数恢复的是 bar 内容。部署观察应同时检查 final 接纳/处理差、失败、背压、`frame_to_enqueue_ms`、`queue_ms`、`ready_offset_ms` 和 bulk 返回时间。

**生产参照与交付边界**

优化过程中只读取生产日志。16:00 UTC 生产仍为原 PID 515424、基线版本：合约 718/718，`E` 最晚 +109 ms，收帧最晚 +140 ms，队列 p50 245.1 ms、max 423.7 ms，全部 ready +565.4 ms，handler 完成最晚 +574.0 ms。14:00 重启后首个小时的 ready 则为 +2,096 ms；生产自身的预热和负载变化很大，不能拿跨小时变化当作新代码收益。[16:00 生产证据](../research/closed-bar-ingress/evidence/implementation/production-1600-summary.json)。

上线后的 2 CPU 实测、多个真实整点、UTC 00:00 的 1h/1d 同时收盘仍需实际运行观察。本地混合周期并发测试覆盖键隔离，不能替代这项生产验收。策略逻辑未变，本次没有以回放 CPU 降幅推算交易收益。

**最终复核与打包**

最终热路径源码快照再次执行 100 次预热、50 次测量，结果如下；这些是在相同本机条件下的新 JVM 复核，不能把不同运行间的小幅变化全部归因于最后一次修改。

| 场景 | CPU 中位数 ms/轮（min–max） | 正确性 |
| --- | ---: | --- |
| 普通流量 | 66.7（39.0–92.1） | 1,208/1,208 final，各轮缓存值均正确 |
| 突发 49,528 帧 | 86.5（83.9–120.2） | 每轮约合并 37,553/48,320 条 forming，约 78%；final 全部正确 |
| 96 个 bulk 查询 | 95.7（64.2–125.8） | 每轮所有响应 finalized；最后响应中位数 +131.6 ms |

[最终性能结果](../research/closed-bar-ingress/evidence/implementation/final-summary.json)。工作线程 JFR 分配采样权重由基线约 82.3 MiB/轮降到约 17.6 MiB/轮，约降 79%；这是采样估计，未把它当作精确全进程分配量。收齐统计从基线消息工作线程样本的 2,416/2,628 降为 131/308；分母为工作线程执行样本，不能当作全进程 CPU 百分比。[JFR 分析](../research/closed-bar-ingress/evidence/implementation/final-jfr-analysis.json)。

再次通过 1,200 个测量小时、1,449,600 条 final 的长跑。相同第 1,219 轮、1,264,776 个版本记录下，强制 GC 后堆从 714,774,864 字节变为 694,350,968 字节，少约 **19.5 MiB**，与每版本少 16 字节的结构测量接近。无游离标记、无缓存错误；保留数量仍不超过每序列 1,050。[最终长期验证](../research/closed-bar-ingress/evidence/implementation/final-soak-summary.json)。

每币 100 条重复 final 的 10 轮测量共 **1,208,000 条 final**，逐条处理、失败为零、最终值正确。forming 接收数也严格等于实际处理、合并、旧版本拒绝三者之和。[重复收盘结果](../research/closed-bar-ingress/evidence/implementation/final-duplicate-finals.json)。

最后将旧研究入口兼容代码移回回放器，生产代码不再保留无用计数器和私有重放入口。清理后的源码另从空目录构建，四类场景各 5 次预热、5 次测量全部通过；这些短轮只用作最终源码验证，不替代充分预热性能数据。[最终源码 smoke](../research/closed-bar-ingress/evidence/implementation/release-smoke-summary.json)、[源文件 SHA-256](../research/closed-bar-ingress/evidence/implementation/release-source-manifest.json)。

`mvn clean verify`：152 项，151 通过，1 项原有 opt-in 冷加载基准跳过；所引用的同一生产实现也已单独启用该基准并通过，因此 152 项均有执行通过证据。该基准实际写入并恢复 40×9,000=360,000 根 K 线；不把单次结果外推为整支交易账户群的启动时间。[完整构建日志](../research/closed-bar-ingress/evidence/implementation/release-verify.log)、[冷加载日志](../research/closed-bar-ingress/evidence/implementation/coldload-benchmark.log)。

停机协调使用 [Spring SmartLifecycle](https://docs.spring.io/spring-framework/docs/6.1.15/javadoc-api/org/springframework/context/SmartLifecycle.html)，上下文关闭测试验证队列排空先于持久化销毁。另补充了“接纳的连续流 final 排队期间 exchange 状态刷新”测试，确认仍提交到接纳时解析出的实际 symbol。对新收到但已不可映射的合约，沿用原协议的拒绝规则，调度保障不等于可以恢复未知合约身份。

安装包已由干净构建生成，SHA-256 见 [artifact.json](../research/closed-bar-ingress/evidence/implementation/artifact.json)。实现验收结束时尚未部署；上文生产日志均为旧版本参照。随后上线的结果已单独记录在[部署实测报告](kline-ingress-deployment-20260913.md)。
