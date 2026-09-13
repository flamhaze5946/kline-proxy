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
| `frame_to_enqueue_ms` | 帧回调至提交线程池，含转字符串 / 二进制解压 |
| `queue_ms` | 提交线程池至处理任务开始 |
| `json_ms` | 任务开始至 JSON 树及消息封装解析结束 |
| `dispatch_ms` | JSON 解析结束至 K 线协议解码开始，含主题心跳和线程池状态采样 |
| `decode_ms` | 协议对象解码、符号映射、Kline 转换至缓存更新开始 |
| `cache_ms` | 缓存更新、补洞、裁剪和脏数据标记 |
| `finalize_ms` | 收盘标记及唤醒等待中的 bulk 请求，含锁等待 |
| `settle_ms` | 既有收齐统计，包括 TRADING 币池查询、扫描缓存和整点汇总日志 |
| `monitor_ms` | 收齐统计结束至消息指标计数完成 |
| `processing_ms` | 处理任务开始至消息指标计数完成 |
| `ready_offset_ms` | 收盘标记和通知完成，相对边界的时间 |
| `done_offset_ms` | 消息指标计数完成，相对边界的时间；不包含新诊断样本入缓冲区及延后写日志 |

`queued_at_receive` / `queued_at_start` 是提交前 / 开始处理时共享消息线程池的队列长度近似快照；`active_threads`、`pool_size` 在收盘消息 JSON 解析后采样。现货与合约共用该线程池。`dropped_total` 是该线程池从进程启动以来的累计丢弃数，不是本次边界增量。

本地各段时长使用 `System.nanoTime()`。`clock_offset_ms` 在服务处理完成后采样，为交易所校正时间减本机时间（取前后本机读时的中点）；`clock_sample_span_ms` 记录此读时跨度，跨度大表示比较 E 时的不确定性增大。`received_wall_ms` 保留原始接收墙上时间。`ready_offset_ms` / `done_offset_ms` 从同一接收基准加单调时钟时长重建，避免本地阶段被时钟跳变污染；时钟偏移估算误差仍会影响与 E 的比较。

E 是事件时间，不保证是实际发送时刻；Netty 回调也不是网卡或内核收到数据的时刻。因此 `event_to_receive_ms` 包含上游排队、网络、系统/Netty 调度、TLS/帧解码及校时误差，不能直接解释成纯网络延迟。各列的最大值可能来自不同币，不能相加；拆解尾部延迟应使用同一条 `DETAIL`。既有 `CLOSED_BAR_SETTLED.max_ms` 在收盘通知之后、收齐统计内部采样，可能略晚于新的 `ready_offset_ms`。
