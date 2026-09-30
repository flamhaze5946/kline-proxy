# 低风险性能优化部署记录

`kline-proxy` 实例于 2026-09-14 **15:30:18 UTC / 19:30:18 迪拜时间**完成数据验收。新进程 PID 为 `531654`，安装包为 `kline-proxy-1.8.1-low-risk-4b8808074930.jar`。

相对之前线上版本，生产源码仅变更三个文件，包含：

- 未收盘 K 线更新不再触发重复落盘；收盘、已收盘数据修正、缓存裁剪和失败重试仍保留落盘信号。尚未满足时间条件的 final 会继续等待落盘。
- REST 返回已缓存且连续的最新一页时按批更新，避免重复生成缺口占位数据；其他页面沿用原路径。
- BigDecimal 滚动均值和成交额窗口改为增量求和，保持精度、舍入和最终结果；去除多余排序及未使用的计算。

线上 `application.yaml` 的内容和 SHA-256 保持一致。HTTP 虚拟线程、合约 1h 连续合约流，以及周期边界前后各 30 秒的落盘避让继续启用。

验收结果：

- 718 个合约的 `1h`、`1d` 最近两根收盘 K 线，逐项与换装前一致，均已 finalized、无 pending。
- 现货 BTCUSDT、ETHUSDT 的 `1h`、`1d` 收盘数据抽查一致。
- 公网 GET、POST bulk 均返回 200，BTCUSDT、ETHUSDT 的小时线与换装前一致；本机实际 POST 路径也已执行预热。
- 现货、合约 BTCUSDT 和 ETHUSDT 的小时线磁盘分片共四组抽样，最近两行的前 11 个数值字段与 API 基线一致，元数据在新版启动后已更新。
- 15:31:40 UTC 最终检查：健康为 `UP`，PID 未变，systemd `NRestarts=0`。WebSocket 实时更新持续处理，失败和背压为 0；采样时有 1 条 forming 消息在队列中。

进程于 15:26:07 UTC 启动，Tomcat 于 15:26:20 UTC 开始提供服务。小时线从磁盘恢复，日线从上游补齐；完整数据核验用时约 **251 秒**。日线等待时间与之前部署约 247 秒的恢复过程相近，这不是整点数据就绪延迟的测量。

部署前 Java 21 全量 `./mvnw -o verify` 通过：216 项通过，1 项需手动启用的性能基准跳过。安装包中的 class 和配置资源已与验证过的本地构建产物核对一致。

新安装包 SHA-256：`4b88080749301f9556067db6ca4b3b3e0e72465844af81a92247172de8111a4a`。

源码快照基于 `29fb85d0239d4331337f4e19a908a8a785d45b51` 及当前工作区，SHA-256 为 `b0ca6439684121dcdcf58014035bc4e1f79e2ac235fd7f5fab6e754ef3df1c1a`。旧包、unit、配置、停机后一致的缓存副本、源码快照、测试日志和部署脚本保留在实例的 `/opt/kline-proxy/deployments/20260914_lowrisk_4b8808074930`。

[部署与数据核验记录](../research/closed-bar-ingress/evidence/lowrisk-deployment-20260914/deployment.json)、[最终运行及落盘检查](../research/closed-bar-ingress/evidence/lowrisk-deployment-20260914/final-health.json)、[公网请求检查](../research/closed-bar-ingress/evidence/lowrisk-deployment-20260914/public-smoke.json)。本次验收时尚未经过新版启动后的第一个整点，因此没有宣称整点提前幅度。
