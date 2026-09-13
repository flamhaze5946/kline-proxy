# kline-proxy 收盘延迟诊断：2026-09-13 14:00 UTC

本次 718 个合约币全部在整点后 **131 ms** 内进入 Netty 的完整 WebSocket 帧回调；收盘可读时间最晚为 **2096.006 ms**。已直接量到的主要等待是共享消息线程池排队。

## 同一条最晚消息：UNITREEUSDT

| 环节 | 时间 / 耗时 |
| --- | ---: |
| Binance 事件 E | +106.000 ms |
| Netty 帧回调（校时后） | +131.000 ms |
| E 到帧回调 | 25.000 ms |
| 线程池排队 | **1936.694 ms** |
| 开始处理至收盘标记/通知完成 | 28.312 ms |
| 收盘标记/通知完成，相对边界 | **+2096.006 ms** |
| 随后的既有收齐统计 | 77.435 ms |
| 消息指标更新完成，相对边界 | +2173.444 ms |

该条消息从 E 到收盘可读的时间差中，排队占 97.32%。其中缓存阶段的 28.134 ms 为经过时间，也包含线程被调度出去的时间，不能当作纯 CPU 消耗。

## 队列为什么会积压

1. 现货和合约共用同一个消息线程池；本次收盘样本中实际线程数和活跃线程数均为 4，队列长度采样最高 1818，进程累计消息丢弃数为 0。合约排队 p50=852.284 ms，p90=1666.664 ms。现货 490 个币也在排同一个队列。
2. 代码配置为 core=4、max=20、队列容量=4096。达到核心线程数后优先排队，队列无法入队才扩到 max，因此当前的 max=20 不会在积压几百条时自动提供 20 个工作线程。[代码](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/client/ws/client/AbstractWebSocketClient.java:734)；[JDK 21 ThreadPoolExecutor 文档](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html)。
3. 已量到的常见处理阶段中，既有收齐统计耗时最明显：合约 settle p50=2.868 ms、p90=17.794 ms；相比之下 JSON / 协议解码 / 缓存 / 收盘通知的 p50 分别仅 0.029 / 0.055 / 0.012 / 0.002 ms。现有 `recordClosedBarArrival` 每条收盘消息都会查询/构建 TRADING 币池并扫描 K 线缓存。这个同步阶段会继续占用工作线程，延后队列中其他消息的处理。[代码](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/src/main/java/com/zx/quant/klineproxy/service/impl/AbstractKlineService.java:722)。这是优先进一步剖析和优化的对象；目前还没有把该阶段内部的 CPU、锁等待、调度等待逐项拆开。
4. 服务器只有 2 个 CPU，整点后的前两个约 1 秒采样区间主机均为 100% 忙碌。观察窗口（约 -5 到 +12 秒）中，JIT 编译线程消耗约 3.30 CPU 秒，4 个消息线程合计约 2.55 CPU 秒。这是重启后的第一个整点，存在明显编译工作；窗口级计数不能精确归因每一毫秒，也不能据此把本次比先前 +1204 ms 更慢的差异全归于新增日志。
5. 关键的前约 2.1 秒没有 GC 回收暂停记录，仅有两次 Cleanup safepoint，约 10.05 和 15.11 ms。14:00:39 的 32.732 ms GC 发生在收盘处理结束之后。三次 Binance REST 时间接口检查及 NTP 状态也没有显示秒级时钟偏差。

根据这些证据，下一步应优先减少逐消息同步收齐统计的工作，再评估消息处理隔离/优先级和线程数；CPU 已饱和，仅提高 max 值并不能证明能改善延迟。

## 口径与验证

- 本次一个整点：合约 718/718，现货 490/490；诊断样本数等于既有统计的预期/到齐数；无样本溢出。所有连接样本数之和一致，详细消息阶段相加误差不超过 0.02 ms。
- E 是事件时间；Netty 回调不是 NIC/内核到达时刻。两者之差含传输、上游/Netty 调度及校时误差。与 E 的比较使用校正墙上时间，本地阶段使用单调时钟。
- 不同列的最大值不能相加。表格使用同一条 UNITREEUSDT 消息。既有 `CLOSED_BAR_SETTLED.max_ms=2096` 在收齐统计内部采样；新的 done_offset 包含该消息后续统计和指标工作，口径更晚。
- 这是 14:00 的新增诊断，无法精确重建未分段记录的 13:00 那次 +1204 ms；后续应用会继续每小时输出日志。
- 已 commit/push/deploy `1025516aa9177bc6073284a37850ac85c32589d5`；合约 1h `useContinuousKlineStream=true`。诊断默认开启，整点后至少 30 秒才集中输出。此整点共 38 条收盘相关日志（含现货/合约既有汇总）。
- JDK 21 `mvn -B verify`：114 个测试，113 通过，1 个既有可选基准跳过，0 失败/错误。部署后 1h / 1d 各 718 币最近两根 closed bar 与部署前完全一致，外网接口正常，进程 PID 515424、自动重启 0 次。
- 一次性 CPU/日志采样已自动结束；应用中的常规诊断继续运行。

证据：[原始观察](/tmp/kline-proxy-latency-20260913/observation-1400.json)、[可复算分析脚本](/tmp/kline-proxy-latency-20260913/analyze.py)、[分析结果](/tmp/kline-proxy-latency-20260913/analysis.json)、[部署记录](/tmp/kline-proxy-latency-20260913/deployment.json)、[部署验证](/tmp/kline-proxy-latency-20260913/verification.json)、[时钟检查](/tmp/kline-proxy-latency-20260913/clock-check.json)、[日志字段说明](/Users/flamhaze5946/Workspace/Java/Personal/kline-proxy/docs/closed-bar-latency.md)。
