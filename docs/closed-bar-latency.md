# 收盘消息分段耗时日志

`kline.diagnostics.closedBarLatencyEnabled` 默认 `true`。普通 K 线和连续 K 线共用诊断；只有 `x=true` 收集服务处理阶段，REST、补数和未收盘更新不进入统计。

每个市场、周期、收盘边界保留最多 4096 个币的首条已处理收盘消息。现有维护任务每 5 秒检查一次，在整点后至少 30 秒输出：

- `CLOSED_BAR_LATENCY`：各阶段 p50、p90、最大值，`n` 为采样币数，`ev_n` 为含有效 `E` 的样本数，`overflow` 为容量不足丢弃的样本数。
- `CLOSED_BAR_LATENCY_CLIENT`：按 WebSocket 连接汇总接收、排队、数据可用时间和队列大小。
- `CLOSED_BAR_LATENCY_DETAIL`：数据最晚可用的 5 个币，加上处理线程耗时最长的 5 个币，去重后最多 10 行；一行中的阶段属于同一条消息。

详细日志不在整点逐币同步写入。每个周期只保留最新边界，重复消息、旧边界、该边界输出后的迟到消息不再采样。`n` 包含收到消息的非 TRADING 币，不能直接当作预期币池；完整性继续查看 `CLOSED_BAR_SETTLED` / `CLOSED_BAR_SETTLE_INCOMPLETE`。这两条既有日志新增 `service` 字段区分现货和合约，原字段含义保持不变。

| 字段（单位 ms） | 测量范围 |
| --- | --- |
| `event_offset_ms` | Binance 事件时间 `E` 减收盘边界；缺失或非法的 E 不参与此项 |
| `receive_offset_ms` | Netty 收到完整 WebSocket 帧的回调时间减边界，按代理的交易所时钟偏移修正 |
| `event_to_receive_ms` | 同一消息从 E 到上述回调的时间差 |
| `frame_to_enqueue_ms` | 帧回调至准备提交队列，含转字符串 / 二进制解压及轻量 header 分类 |
| `queue_ms` | 准备提交至处理任务开始；优化后的有界队列若已满，也包含生产者等待容量的时间 |
| `json_ms` | 任务开始至 JSON 树及消息封装解析结束 |
| `dispatch_ms` | JSON 解析结束至 K 线协议解码开始，含 worker 状态采样；已分类帧的 topic 心跳在收帧时记录 |
| `decode_ms` | 协议对象解码、符号映射、Kline 转换至缓存更新开始 |
| `cache_ms` | 原子更新数据及 final 标记、按需补洞、裁剪和脏数据标记 |
| `finalize_ms` | 通知等待该 bar 的 bulk 请求，含通知锁等待；final 标记与数据在 cache 阶段一起提交 |
| `settle_ms` | 边界级收齐计数；首次创建、完成前复核和维护时才扫描币池/缓存，汇总时排序及写日志 |
| `monitor_ms` | 收齐统计结束至消息指标计数完成 |
| `processing_ms` | 处理任务开始至消息指标计数完成 |
| `ready_offset_ms` | 收盘标记和通知完成，相对边界的时间 |
| `done_offset_ms` | 消息指标计数完成，相对边界的时间；不包含新诊断样本入缓冲区及延后写日志 |

`queued_at_receive` / `queued_at_start` 是提交前 / 开始处理时的近似队列长度。已分类 K 线取所有分片的 final + forming 队列数量，不含正在执行或等待容量的生产者；回退路径取通用消息池的队列长度。`active_threads`、`pool_size` 在收盘消息 JSON 解析后采样，分别对应实际使用的 dispatcher 或通用池。现货与合约共用行情 dispatcher，通过市场与序列键区分。

当前两条路径都没有队列丢弃策略，兼容字段 `dropped_total` 为 0；它不是处理成功的保证。需结合 `websocket_kline_ingress_received_total{closed="true"}`、`processed_total{closed="true"}`、`failures_total` 和背压指标检查处理情况。旧版本 `1025516` 的该字段曾表示共享池启动以来的累计丢弃数。诊断中的 `overflow` 只代表丢弃诊断样本，不是丢行情。

本地各段时长使用 `System.nanoTime()`。`clock_offset_ms` 在服务处理完成后采样，为交易所校正时间减本机时间（取前后本机读时的中点）；`clock_sample_span_ms` 记录此读时跨度，跨度大表示比较 E 时的不确定性增大。`received_wall_ms` 保留原始接收墙上时间。`ready_offset_ms` / `done_offset_ms` 从同一接收基准加单调时钟时长重建，避免本地阶段被时钟跳变污染；时钟偏移估算误差仍会影响与 E 的比较。

E 是事件时间，不保证是实际发送时刻；Netty 回调也不是网卡或内核收到数据的时刻。因此 `event_to_receive_ms` 包含上游排队、网络、系统/Netty 调度、TLS/帧解码及校时误差，不能直接解释成纯网络延迟。各列的最大值可能来自不同币，不能相加；拆解尾部延迟应使用同一条 `DETAIL`。既有 `CLOSED_BAR_SETTLED.max_ms` 在收盘通知之后、收齐统计内部采样，可能略晚于新的 `ready_offset_ms`。
