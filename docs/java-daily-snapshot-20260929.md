# Java 日线快照与部署验证

2026-09-29 已在 `market.feng.dog`（`192.0.2.30`）启用现货、合约 `1d` 自动落盘和启动恢复。运行版本为 Java **1.8.3**，包含单币价格优先读取内存的修改。最终 PID 为 `795641`；健康检查 `UP`，systemd 自动重启次数为 0。

生产配置 `/opt/kline-proxy/application.yaml` 的有效持久化部分如下：

```yaml
kline:
  persistence:
    enabled: true
    loadOnStartup: true
    dumpOnShutdown: true
    dumpIntervalSeconds: 300
    rootDir: /opt/kline-proxy/data/kline-cache
    spot:
      intervalConfigs:
        "1h":
          maxStoreCount: 1000
        "1d":
          maxStoreCount: 1000
    future:
      intervalConfigs:
        "1h":
          maxStoreCount: 1000
        "1d":
          maxStoreCount: 1000
```

两市场日线的 `minMaintainCount` 都是 1000；落盘上限与维护窗口一致。快照只保存可持久化的已收盘历史。窗口含当日形成中的一根，因此本次最长日线快照为 999 根；新上市币种的历史更短。

继续使用已有的周期边界前后各 30 秒落盘避让、原子文件写入及停机落盘机制。此次日线变更仅调整配置，没有增加请求路径上的 REST 调用。仓库示例 `src/main/resources/application.yaml` 也增加了两市场的 `1d` 白名单；示例的全局 `enabled: false` 保持原有开发默认值，生产使用上面的外置配置。

实测验证均在 Java 实例完成，时间为 UTC：

| 项目 | 现货 | 合约 |
|---|---:|---:|
| 日线序列数 | 496 | 732 |
| 首轮自动落盘日线根数 | 333,022 | 367,173 |
| 每序列最少 / 最多根数 | 5 / 999 | 1 / 999 |
| 日线磁盘占用，`du -sh` | 1.3 GiB | 1.5 GiB |
| 启动日志恢复序列数，含 1h 和 1d | 992 | 1,464 |
| 启动日志记录的两周期合计恢复耗时 | 28.506 秒 | 36.809 秒 |
| 全部币种最近最多 10 根已收盘日线：磁盘 / 重启前 / 重启后 | 一致 | 一致 |

03:50:34 确认首轮自动落盘完整；03:51:29 完成所有日线序列的磁盘与内存核对；随后正常停机约 20.9 秒，验证停机落盘。03:51:50 启动，约 **75.8 秒**后健康端点返回 `UP`，启动后约 81.1 秒完成全部 1228 个币种的最近日线核对。启动日志明确记录了全部 2456 个小时线与日线序列从磁盘恢复，未发现持久化读取、写入或恢复错误。

本次验证是同一实例的进程重启，系统页缓存未清空。此前没有日线快照的启动需约 4 分钟才能完成全部合约日线基线核对。当前存储仍按日期拆文件，本次约有 70 万个日线数据文件；同步恢复在 HTTP 服务启动前完成，因此健康端点可用时间从仅小时快照时约 17 秒增加到约 76 秒。这两个就绪口径不同，不能将日线历史更早恢复描述为所有接口更早开始服务。

最终公网检查从 Tokyo 测试机 `192.0.2.10` 经校验证书的 HTTPS 发起：Java / Rust 两市场、两周期、各 5 个币种的 20 组最近已收盘 K 线完全一致；Java bulk 的 GET / POST、包含 / 排除当前 bar 的 4 组检查通过。单币价格每市场 20 次预热、500 次顺序 keep-alive 采样、请求间隔至少 10 ms：

| Java 单币价格 | p50 | p99 | 最大值 |
|---|---:|---:|---:|
| 合约 | 2.56 ms | 7.33 ms | 9.93 ms |
| 现货 | 2.52 ms | 7.55 ms | 49.81 ms |

采样发生在 03:53:32—03:53:43，属于重启后的公网冒烟测试，不能代替整点或 200 客户端并发测试。4 次 bulk 检查用于验证行为，不用于计算延迟分位数。

可复查证据：

- [Java 1.8.3 上线记录](../research/single-ticker-deploy-20260929/activation.json)
- [首轮自动日线落盘记录](../research/single-ticker-deploy-20260929/daily-snapshot.json)
- [全币种恢复校验记录](../research/single-ticker-deploy-20260929/restore-verification.json) 与 [启动日志](../research/single-ticker-deploy-20260929/restore-startup.log)
- [公网接口检查和原始延迟样本](../research/single-ticker-deploy-20260929/public-smoke.json)
- [最终进程、配置、磁盘及健康状态](../research/single-ticker-deploy-20260929/final-state.json)

旧版本、原配置及部署前数据保留在 `/opt/kline-proxy/deployments/1.8.3-memory-20260929T033439Z`。日线配置变更前后的配置和恢复验证记录保留在 `/opt/kline-proxy/deployments/1.8.3-daily-snapshot-20260929T034422Z`。日线配置回退只需恢复该目录的 `application.before.yaml` 并重启，已有日线快照可以保留。

最终 JAR SHA-256：`e88eeac0769ba763958a600d1c0c72162022f8c7dc888b6c4a6528f8a26046da`。最终生产配置 SHA-256：`aa4453be4d53a551db828a1ff3794b514837a23ad41f96b874f8548ea1aa51e4`。
