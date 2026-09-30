# 落盘避让与虚拟线程部署记录

`kline-proxy` 实例于 2026-09-13 **18:46:27 UTC / 22:46:27 迪拜时间**完成验收。新进程 PID 为 `518766`，安装包为 `kline-proxy-1.8.1-persistence-bc65e955300a.jar`。

实例的 `/opt/kline-proxy/application.yaml` 已设置：

```yaml
spring.threads.virtual.enabled: true
kline.persistence.boundaryGuardBeforeMs: 30000
kline.persistence.boundaryGuardAfterMs: 30000
```

启用的 interval 为现货、合约的 `1h` 和 `1d`。正常落盘间隔仍为 300 秒，周期开始前后各 30 秒避让。合约 `1h` 连续合约流保持启用。

验收结果：

- 718 个合约的 `1h`、`1d` 最近两根收盘 K 线逐项与重启前一致；日线恢复完成及验收用时约 247 秒。
- 现货 BTCUSDT、ETHUSDT 的 `1h`、`1d` 收盘数据抽查一致。
- 回环健康检查为 `UP`，公网 GET、POST bulk 请求均返回 200。
- 在一个真实本机 HTTP POST 等待请求体最后一个字节时执行线程转储，确认处理请求的线程为 `#2760 "tomcat-handler-2563" virtual`；随后补齐请求体并收到 200。虚拟线程已在生产 HTTP 请求处理路径生效。
- 新进程稳定运行，systemd `NRestarts=0`。

部署目录及回滚材料：`/opt/kline-proxy/deployments/20260913_persistence_bc65e955300a`。其中保存了旧安装包、`service.before`、`application.before.yaml`、停机后一致的 `cache.before`、部署脚本、构建源码快照，以及 `deployment.json`、`final-health.json` 和线程核验证据。

新安装包 SHA-256：`bc65e955300aa07f4d898d98779bf4b5df69c750d8860589f9d2d56098514764`。源码快照基于 `29fb85d0239d4331337f4e19a908a8a785d45b51` 工作区中的落盘避让改动，快照 SHA-256 为 `dce7f05718b008fe4d1f850f866368c00703dca8a10e1429db33fb55eefbd2f3`。构建前的完整测试为 187 项通过，1 项可选基准测试跳过，包含真实 Tomcat 的虚拟线程启用测试。

避让行为及短 interval 的配置限制见[周期边界避让说明](kline-persistence-boundary-guard.md)。本记录验证发布、数据恢复和线程开关生效，不作为整点延迟改善的测量结果。
