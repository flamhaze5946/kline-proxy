# 2026-09-13 生产部署观察

正式程序版本 `407afae`，旧版对照 `1025516`。最终结论见 [部署报告](../../../docs/kline-ingress-deployment-20260913.md)。

本目录为一次有明确截止时间的生产观察记录，脚本中的日期、服务器路径和 PID 是该次观测的真实值，**不是可以不修改就再次执行的通用部署工具**。`observe.py`、`cpu_watch.py`、`profile_hour.py`、`verify_rest.py` 在服务器执行；`probe_public.py` 在本机经公网执行。它们不修改行情数据；JFR 录制会改变 JVM 采样设置并有观测开销。

仓库根目录执行计算和图表复现：

```bash
python3 research/closed-bar-ingress/deployment-observation/compare_hours.py research/closed-bar-ingress/evidence/deployment-20260913
python3 research/closed-bar-ingress/deployment-observation/plot_hours.py
python3 research/closed-bar-ingress/deployment-observation/plot_persistence.py
```

前一个命令仅依赖 Python 标准库，核验两小时日志各阶段、样本数、四个 HTTP 响应的内容和 final 计数。绘图需要 matplotlib，macOS 上使用 Hiragino Sans GB；导出的 PNG/SVG 已包含在证据目录并经目视检查。

`summarize_cpu.py` 从 `/proc` 原始 JSONL 和 metadata 生成每秒差分；完整的紧凑时间序列保存在 `cpu-timeline.jsonl`。线程表的分母仅为首尾均存在的线程，新增 HTTP 线程另行披露。不要用约 17 秒的线程快照除以约 16 秒的进程采样时间，或将 CPU 秒当作墙上时间。

`ExtractJfr.java` 用 JDK 21 的 RecordingFile API 仅导出性能事件，排除环境／系统属性。`summarize_jfr.py` 可从导出的 JSONL 汇总指定 `[begin-ms,end-ms)` 窗口；FileWrite 的空路径属于非缓存写入。原始 JFR 没有提交到仓库。

```bash
java ExtractJfr.java recording.jfr events.jsonl
python3 summarize_jfr.py events.jsonl summary.json --begin-ms 1789322395000 --end-ms 1789322412000
```

证据位于 `../evidence/deployment-20260913`：

- `baseline-1700`、`optimized-1800`：观察记录、探针响应、时钟校准、nginx 汇总；原始观察中的无关 warning/error 行已省略。
- `hour-comparison.json`：校验后的分段延迟、线程累计值、ingress 计数和探针时间。
- `hour-window-jfr.json`、`hour-first3s-jfr.json`：整点 Java 栈与锁等待；其中没有持久化样本、缓存写入或 GC 暂停。
- `persistence-profile-summary.json`、`post-hour-persistence-jfr.json`：两个独立时段的持久化热点、分配权重和文件写入摘要。
- `cpu-timeline.jsonl`、`cpu-observer-summary.json`：一秒 CPU/I/O 时间序列与观察器自身成本。`persistence-cpu-input.json` 保留前两轮绘图的原始派生口径。
- `optimized-1800/deployment.json`、`final-health.json`：上线数据核验、安装包哈希和观察任务结束状态。

旧版安装包、配置、unit、完整缓存备份以及原始 JFR 位于服务器 `/opt/kline-proxy/deployments/20260913_ingress_407afae_retry`；本机原始分析目录 `/tmp/kline-proxy-deploy-407afae-20260913`。报告中的数据均来自真实生产观察；没有用本地合成回放替代上线实测。
