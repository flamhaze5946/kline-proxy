# K 线落盘的周期边界避让

生产实例已于 2026-09-13 部署，同时按要求启用 HTTP 虚拟线程，详见[部署记录](kline-persistence-deployment-20260913.md)。

启用 `kline.persistence.enabled` 后，后台落盘默认避开程序中所有已启用 interval 的新周期开始前后各 30 秒。扫描范围是 `kline.binance.spot.intervalSyncConfigs` 和 `kline.binance.future.intervalSyncConfigs` 的并集，忽略 `enabled: false` 的市场；即使某个 interval 没有配置持久化，也会保护它的周期边界，因为现货、合约共享 CPU。

```yaml
kline:
  persistence:
    enabled: true
    dumpIntervalSeconds: 300
    boundaryGuardBeforeMs: 30000
    boundaryGuardAfterMs: 30000
```

默认保护区间是 `[周期开始 - 30 秒, 周期开始 + 30 秒)`。例如启用 `1h` 后，`12:59:30.000` 到 `13:00:29.999` 暂停后台落盘，从 `13:00:30.000` 起允许恢复。时钟采用对应服务的交易所校准时间，按 UTC 计算边界。

- 正常完成一次落盘后，再等待 `dumpIntervalSeconds`。每秒的调度检查在尚未到期时直接返回。
- 到期任务遇到保护窗口时不复制待落盘集合、不生成快照、不格式化历史 K 线；保留 dirty 标记，每秒检查，离开所有保护窗口后继续。避免落盘间隔与 `5m` 等 interval 相同时反复命中边界。
- 批量落盘在每个交易对/interval 取得写锁后重新读取时间；进入窗口后停止后续写入，未完成的数据留待下一次检查。
- 启动预热后的持久化校准也使用相同避让规则，被延后的数据交给定时任务处理。
- 已经开始处理的单个交易对/interval 会完成，因此这是按序列协作暂停，不是硬实时中断。关机时 `dumpOnShutdown` 的最终落盘绕过窗口；下线交易对的磁盘清理仍按原逻辑执行。

两侧配置小于等于 0 时分别禁用对应侧；两侧都为 0 时完全关闭避让。两侧有效时长之和必须小于最短启用 interval，才能留出后台落盘时间。例如 `1m` 配合默认前后各 30 秒会覆盖整个周期，程序会在启动时记录警告并持续延后后台落盘；可改为前后各 `10000` 毫秒。`1s` 则需要更短的窗口。不会自动缩短用户配置的保护时间。

边界计算覆盖固定分钟/小时/日周期、周一 UTC 零点的周线、自然月月初，以及 `3d` 的交易所起点偏移。回归用例中的 `3d` 边界（例如 2024-01-01）已与 Binance [现货历史 K 线](https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=3d&startTime=1704067200000&limit=2)及 [USD-M 历史 K 线](https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=3d&startTime=1704067200000&limit=2)的开盘时间核对；周线、闰年月线也有边界测试。

本改动调整 CPU 开销发生的时间，不减少每次序列化的总工作量。
