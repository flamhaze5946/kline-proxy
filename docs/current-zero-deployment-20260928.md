# Java / Rust 部署记录：2026-09-28

已部署 Java **1.8.2** 到 https://market.feng.dog（192.0.2.30），Rust **0.4.15** 到 https://market-mirror.feng.dog（192.0.2.40）。以下时间均为 UTC（迪拜时间加 4 小时）。

| 项目 | Java | Rust |
|---|---|---|
| 启动时间 | 18:48:19 | 18:49:36 |
| 核验结果 | active、健康 UP | active、2456/2456 就绪 |
| 自动重启计数 | 0 | 0 |
| 启动日志 WARN/ERROR | 0 | 0 |

## 上线行为

现货和合约必须在本进程收到并接受紧邻上一根 WebSocket `x=true` 后，才能在缺少当前真实 bar 时临时补零。占位只用于查询，不落盘、不标记已收盘，真实 bar 到达后立即替换；不新增 REST 请求。完整门槛见 [实现说明](current-zero-bar-20260928.md)。

Rust 同时取消尾部缺失、最终帧迟到、静默重订阅及 recheck 提示触发的即时 K 线 REST 修复，改为等待定时订正。启动历史加载、新序列/容量初始化、实际连接重连恢复仍保留；非 K 线业务与显式透传沿用既有规则。

## 验证

- 发布前 Java 227 项测试，226 通过、1 项原有跳过；Rust 174 项全部通过，Clippy 与格式检查通过。Java jar 中 205 个 class 条目与测试所用 class 一致。
- Java 恢复 496 条现货及 732 条合约小时快照；732 条合约 1h、727 条合约 1d 的最近两根收盘数据与部署前逐字段一致。现货 5 个币种 × 1h/1d 的最近 10 根收盘数据也一致，18:52:27 完成核验。
- Rust 恢复 2451 条序列、2,187,679 根 bar，损坏快照为 0；18:54:20 首次采样到 2456/2456 就绪，14/14 连接有数据，实际及就绪原因中的 stale_tail 均为 0。
- 东京测试机通过两个公网域名，比较两市场 × 两周期 × 5 个币种的最近 10 根收盘 K 线，20 组完全一致；Rust 两市场 bulk GET 检查通过。
- 两端合约 bulk POST 的 `closed_only=true/false` 共 4 项均 HTTP 200、无 pending、每币 10 根。最后核验时间 18:56:36。本机接口检查 Java 10 项、Rust 12 项通过。

本次未开展性能压测，尚未观察部署后的新整点补零过程。重启恢复的快照不能代替本进程收到的上一根 `x=true`。

证据：[汇总](../research/current-zero-deploy-20260928/summary.json)、[Java 部署及前后数据核验](../research/current-zero-deploy-20260928/java-activation.json)、[Java 运行状态](../research/current-zero-deploy-20260928/java-final-status.json)、[公网数据比较](../research/current-zero-deploy-20260928/public-smoke.json)、[公网 POST](../research/current-zero-deploy-20260928/public-post.json)、[启动日志](../research/current-zero-deploy-20260928/java-startup.log)。

## Java 产物与回滚

线上 jar：`/opt/kline-proxy/kline-proxy-1.8.2-zero-dcddee5e229b.jar`。

SHA256：`dcddee5e229b1365ea3e36cebaa6d1557c1e5cc404b3a0775c24fd55bee63a07`，与本地产物一致。

旧 jar `kline-proxy-1.8.1-low-risk-4b8808074930.jar` 保留。备份位于 `/opt/kline-proxy/deployments/1.8.2-zero-20260928T184801Z`，包含原 service、配置和停止写入后的数据副本。回滚时恢复该目录的 service 文件，执行 daemon-reload 并重启 `kline-proxy`。本次未触发回滚。

Java 生产配置与 JVM 参数保持原样，仅切换 jar 路径；原有 spot/future 1h 持久化配置保留。Rust 旧版 0.4.14 及一致性快照备份也已保留，其路径见汇总文件。两端 Nginx 配置未改动。
