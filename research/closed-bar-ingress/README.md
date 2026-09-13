# Closed-bar ingress research

正式实现与验收记录见 [优化结果](../../docs/kline-ingress-implementation-20260913.md)。当前实现可从源码快照单独构建并严格校验：

```bash
export JAVA_HOME=/Users/flamhaze5946/Library/Java/JavaVirtualMachines/azul-21.0.8/Contents/Home
python3 research/closed-bar-ingress/run_current.py --prepare --work-dir /tmp/kline-ingress-current
python3 research/closed-bar-ingress/run_current.py --work-dir /tmp/kline-ingress-current --scenario soak
```

`run_current.py` 复制当前 `pom.xml`、`src/main` 和回放器到空工作目录后构建，记录源码 SHA-256；不复用工作区的编译输出、不启动应用。默认包含普通流量、突发、96 个 bulk 请求、每币 100 条重复收盘四组场景。`soak` 另跑 1,200 个测量小时，每 100 小时在计时窗口外强制 GC 并检查保留数量与元数据。正常 bar 保留上限是 `history + 50`。JFR 和逐轮结果保存在工作目录，任何 final 条数、缓存值、队列排空或保留检查失败都会以非零码退出。100 次预热、50 次测量可用 `--warmups 100 --runs 50` 重现更充分预热的对照口径。

下面保留优化前的隔离研究，`run.py` 始终针对固定基线，不会自动切换到当前实现。

研究基线：`1025516aa9177bc6073284a37850ac85c32589d5`。结论与限制见 [调研报告](../../docs/kline-ingress-optimization-20260913.md)。这些代码仅用于隔离实验，不能直接接入生产。

`ReplayHarness` 调用生产的解析、协议、缓存、final、收齐统计与 bulk 方法；固定 exchange fixture 替代网络。`cached-stats.patch` 为固定边界币池的统计原型；`poll-only.patch` 仅用于量化去掉广播的收益及延迟代价。合并队列原型位于 harness 内，保留每条 final，但 final 队列无界、旧 slot 未回收，不具备生产过载保障。

需要 JDK 21、Maven、Python 3、git、tar、patch。本机实测使用 Zulu 21.0.8；`ActiveProcessorCount=2` **不是限核**。运行时不启动应用、不发送网络行情请求；准备阶段 Maven 可能下载依赖。

```bash
export JAVA_HOME=/Users/flamhaze5946/Library/Java/JavaVirtualMachines/azul-21.0.8/Contents/Home
python3 research/closed-bar-ingress/run.py --prepare --work-dir /tmp/kline-ingress-replay
python3 research/closed-bar-ingress/analyze_jfr.py /tmp/kline-ingress-replay
```

`--prepare` 要求空目录，读取上述 commit 的 git archive 后在该目录编译，不依赖当前工作区的 target/classes，也不修改生产源文件。完整矩阵默认 12 次预热、10 次测量。可重复传入 `--scenario` 选择场景；`--warmups 1 --runs 1` 可用于 smoke，但结果不能作为预热性能数据。

普通回放每轮 3,624 帧，final 注入最晚为整点后 131 ms。突发回放每轮 49,528 帧，不做网络节流；其 `close_done_*_ms` 仍沿用边界偏移字段，而突发从模拟边界前 20 ms 开始，若需相对突发开始的时间，应加 20 ms。不要把这种人工注入偏移与生产 Binance E 比较。

CPU 是回放时间窗内的全进程 CPU，包含工作线程、元信息扫描、GC/JIT，以及场景启用的 bulk reader；不包含 fixture 初始化和输入字符串构造，但后续 GC 可能回收其对象。INFO/WARN 日志输出关闭，诊断采样开启，没有网络/Netty 解帧、HTTP、持久化磁盘写入、1d 实时帧或 ticker 负载。1d 的历史集合仍保存在内存中参与实际缓存扫描。

`CorrectnessProbe` 会对基线输出三个 `correct=false`，这是成功复现已知反例，不是通过正确性检查。它会在未能复现预期反例时以非零码退出。高压 FIFO 场景本来就可能丢失 final，因此不能只按 runner 退出 0 判断数据正确，必须读每轮的计数和缓存校验字段。

本次完整证据在 [evidence](evidence/summary.json)。JFR 文件保存在 `/tmp/kline-proxy-optimization-20260913/jfr`，可用本机 JDK Mission Control 或 VisualVM 打开；[文件大小与 SHA-256 清单](evidence/jfr-manifest.json) 可校验原始记录。大体积 JFR、依赖 jar 和中间编译目录未纳入仓库。

证据仅包含有效的 `*-bulk96-gated` 场景。早期以每个 reader 短睡眠对齐启动的 bulk 实验增加了测试自身的调度开销，已经排除。持久化结果还保留每轮数据、正确性反例和此前 14:00 生产分段分析，便于区分测量来源。
