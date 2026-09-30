**Java 性能优化及 VPS 复测｜2026-09-30（迪拜时间）**

已将 Java 从 1.8.3 优化并部署到 **1.8.9**。200 客户端、每客户端 5 个分散 symbol、最近 10 根 closed 1h 的 bulk p99 从 103.55 ms 降至 **70.98 ms**，降低 **31.4%**；中位数降低 37.1%。普通接口已接近 Rust，高并发 bulk 仍慢于 Rust 同轮的 **27.66 ms**，尚未全面追平。

测量发生在 2026-09-29 UTC；最终基础复测为 20:25–20:28 UTC（迪拜 9 月 30 日 00:25–00:28）。Java 为 `market.feng.dog`，Rust 为 `market-mirror.feng.dog` 0.4.22，客户端位于独立东京测试机。

**最终基础结果**

| 指标 | Java 优化前 1.8.3 | Java 最终 1.8.9 | Rust 同最终轮次 |
|---|---:|---:|---:|
| 200 并发 bulk p50，ms | 50.48 | 31.74 | 13.94 |
| 200 并发 bulk p99，ms | 103.55 | 70.98 | 27.66 |
| 200 并发 bulk 最大，ms | 110.35 | 81.00 | 30.81 |
| 常规负载平均 CPU，单核＝100% | 14.23 | 12.73 | 17.34 |
| 常规负载平均 RSS，MiB | 1533.85 | 1501.88 | 646.09 |

最终 CPU 和 RSS 取 100.23 秒常规负载窗口，优化前取 98.47 秒；这些不是 200 并发瞬间的峰值。CPU 包含 JIT、GC、WebSocket 和后台任务，没有扣除估计成本。Java 本轮 CPU、RSS 均低于优化前，但这是两台线上 VPS 的连续测量，不能把差值视为纯代码或语言收益。

| 常规接口，每类 500 次，ms | Java p50 | Rust p50 | Java p99 | Rust p99 |
|---|---:|---:|---:|---:|
| 合约单币价格 | 4.84 | 4.74 | 7.51 | 7.80 |
| 合约全市场价格 | 4.52 | 4.40 | 7.22 | 7.19 |
| 现货单币价格 | 4.25 | 3.91 | 6.52 | 6.79 |
| 现货全市场价格 | 4.54 | 3.76 | 8.72 | 7.83 |
| 合约 closed 1h × 10 | 4.55 | 4.26 | 7.13 | 6.92 |
| 合约 closed 1d × 10 | 4.39 | 4.09 | 7.06 | 6.80 |
| 现货 closed 1h × 10 | 4.13 | 3.75 | 7.24 | 7.23 |
| 现货 closed 1d × 10 | 3.95 | 3.62 | 8.08 | 8.03 |
| 常规 bulk 5 币 × 10 根 1h | 3.77 | 3.51 | 8.33 | 8.64 |
| 常规 bulk 5 币 × 10 根 1d | 3.33 | 3.00 | 7.82 | 7.91 |

基础复测共 14,000 个计时请求，全部成功；3,000 对常规 closed 数据、2,000 对 bulk closed 数据一致。动态价格因请求时刻不同，不要求逐字相等。Java 本轮普通请求仍有约 30–46 ms 的最大样本，不能用 p99 接近来声称已经没有尾延迟。

**模拟实盘请求链**

200 个客户端，每个选 5 个分散 symbol，依次请求「资金费率 → bulk → 3 个单币 closed K 线」。10 轮交替访问两服务，每服务 2,000 条完整链、10,000 次请求；资金费率查询最近 4 小时已缓存范围，K 线取最近 10 根。这个场景检验混合请求竞争，但发生在非整点，不能替代新 closed bar 到达时的观测。

| 请求链指标，ms | Java 1.8.9 | Rust 0.4.22 |
|---|---:|---:|
| 资金费率 p99 | 81.74 | 45.17 |
| bulk p99 | 80.83 | 39.09 |
| 第 1 个单币 K 线 p99 | 119.68 | 26.64 |
| 第 2 个单币 K 线 p99 | 118.30 | 27.59 |
| 第 3 个单币 K 线 p99 | 86.86 | 22.81 |
| 完整链 p50 | 220.20 | 80.65 |
| 完整链 p99 | 373.39 | 105.32 |
| 完整链最大 | 380.20 | 106.51 |

另测「资金费率 → bulk」两步链，每服务 4,000 次请求：

| 两步链指标，ms | Java 1.8.9 | Rust 0.4.22 |
|---|---:|---:|
| 资金费率 p99 | 56.85 | 45.94 |
| bulk p99 | 59.55 | 30.29 |
| 完整链 p99 | 107.98 | 54.74 |

最终三类场景合计 **42,000 个计时请求，0 错误；19,000 对资金费率及 closed K 线结果全部一致**。资金费率命中路径优化前的 1.8.5 两步链 p99 为 182.01 ms，最终为 107.98 ms；其中 funding p99 从 167.84 ms 降至 56.85 ms。这是连续阶段的部署结果，不是逐项优化的独立因果估计。

**本轮实际保留的优化**

1. **接入与连接复用**：Tomcat backlog 100→1,024；Nginx upstream 空闲连接缓存 64→256/worker，worker connections 768→2,048，协调两端 keepalive 超时与请求数，并启用 TLS session cache。解决已有证据支持的连接排队问题。
2. **closed K 线按序列共享展示与 UTF-8 编码**：不同 5 币组合复用同一 symbol 的已收盘窗口，普通 closed 查询也共享格式化结果。修订、裁剪、序列替换、时间边界均检查失效；缓存有界。
3. **元数据投影复用**：TRADING 集合与 symbol 校验集合按 exchange 发布对象身份复用，避免每个请求扫描全市场；后续移除了 Caffeine 频率统计触发的整个元数据对象 hashCode。
4. **隔离同步冷加载**：资金费率、元数据、CMS、统计和图表冷缓存加载放入有界平台线程池。命中的资金费率时间片直接读取强引用，不进入限额执行器，也避免检查命中后被逐出而意外回源。
5. **收盘等待按请求完成条件唤醒**：等待的 bar 全部就绪后才唤醒；处理注册竞态、删除和裁剪通知，移除 25 ms 轮询，保留原截止时间。
6. **WebSocket 直接类型解码**：已分类的普通 K 线跳过 DOM 树；扩展处理器保留惰性树接口，歧义包装和数值强制转换边界继续使用兼容路径。
7. **全市场 ticker 缓存 UTF-8 字节**：避免命中缓存后重复字符串编码；HTTP 字节级回归覆盖现货、合约及 24hr FULL/MINI。共享行对程序调用者返回私有数组，防止修改污染其他响应。

K 线查询没有增加 REST 回填；启动、重连、新 symbol 和原有定时订正路径保留。spot / futures 临时零成交量 bar 仍必须等上一根真实 WebSocket `x=true` 后才能构建。价格精度、JSON 结构、排序、时间边界、资金费率发布宽限和 1h/1d snapshot 语义保留。

**整点验证：仍有差距**

| UTC 整点 | Java 版本 | 合约 closed 全部可读 | 现货 closed 全部可读 | Java bulk p99 | Rust bulk p99 |
|---|---|---:|---:|---:|---:|
| 19:00 | 1.8.4 | T+194.98 ms | T+120.52 ms | 581.32 ms | 126.37 ms |
| 20:00 | 1.8.8 | T+211.20 ms | T+163.88 ms | 545.10 ms | 120.88 ms |

两个整点的 200 对 bulk closed 数据均一致，无请求错误；两边均未出现监听队列溢出/丢弃。20:00 Java 最后一个测试响应为 T+569.81 ms，Rust 为 T+142.87 ms；Java 首分钟另外承接 958 个生产请求，Rust 没有这些额外请求。因此这是线上服务表现，不能当成同负载语言基准。

20:00 第一秒没有 GC 暂停；Java 合约消息最后接收于 T+137 ms，全部可读为 T+211.20 ms，最大任务排队 74.60 ms。剩余问题包含消息与 HTTP 竞争 CPU、就绪后请求调度和输出成本，不能继续将这次整点差距归因于 GC、REST 限流或监听队列。Rust 就绪检查 334 次均返回 200，未见 stale_tail。最终 1.8.9 的自然整点尚未复测：20:00 数据对应 1.8.8，之后只增加程序调用者数组保护并恢复默认 G1，不能把该整点冒充 1.8.9 实测。

**试验、复核与停止继续微调的依据**

| 方案 | 观察到的结果 | 最终处理 |
|---|---|---|
| Caffeine weak-key 元数据集合缓存 | 后台频率统计仍调用整个 exchange 的 hashCode；约 100 秒窗口 common-pool CPU 4.26 s | 改为单发布对象身份缓存；后续同类窗口约 0.11–0.12 s |
| Generational ZGC，1–2 GiB 堆 | GC 暂停很短、纯 bulk p99 68.62 ms；RSS 约 2,152 MiB，两步链 p99 199.89 ms | 未保留；混合链与资源代价不划算 |
| G1，MaxGCPauseMillis=20 | 纯 bulk p99 96.68 ms，未优于默认 G1 | 未保留；最终为默认 G1 |
| DecimalFormat 原型克隆微测试 | 8 个数字约 1.241→1.179 µs；收益极小且只是本地微测试 | 不引入额外格式化实现 |

以上 GC 试验是连续线上阶段，存在编译、背景流量及市场流量变化，不能当成严格单变量实验或保证最优参数。

单独 JFR 诊断与正式计时分开。最终架构的混合链诊断中，524 个 HTTP 执行样本有 364 个包含 Spring MVC HandlerAdapter、137 个包含响应转换；这些是包含下层调用的重叠采样，不能相加或直接折算延迟收益。数字格式化已降至 2 个样本，未发现 VirtualThreadPinned 事件。另有 313 次 Tomcat Poller 的 EPoll 锁等待，合计 699 ms / 100 s，单次最长 9.09 ms；这是残余开销，解释不了整点数百毫秒的全部差距。最终基础计时窗口 GC 暂停最大 36.42 ms，尾延迟仍会受运行时和宿主调度影响。

最终常规查询最慢的 9 个样本（约 30–46 ms）与两次 25.31 / 36.42 ms GC 暂停时间重叠；这支持 GC 是这些样本的贡献因素，不能将整段延迟全归给 GC。最终 200 并发 bulk 阶段没有 GC 暂停，p99 仍为 70.98 ms，因此仅换收集器不足以解决主要并发差距。Nginx 上游响应 p99 为 Java 69 ms / Rust 24 ms，连接建立 p99 为 Java 3 ms / Rust 0 ms（约 1 ms 日志分辨率）。[慢样本与 GC 对齐](../research/java-optimization-20260929/evidence/final-slow-samples.json)、[Java Nginx](../research/java-optimization-20260929/evidence/final-nginx-java.json)、[Rust Nginx](../research/java-optimization-20260929/evidence/final-nginx-rust.json)。

当前有直接热点证据、能保持数据与 API 语义的局部优化已完成并复测。剩余主要方向是 Spring MVC / Servlet 请求管线与通用序列化、JDK/容器升级或整点负载隔离；这些需要单独的架构或运行时对照验证，当前证据不足以确认继续堆叠补丁的净收益，也不能声称不存在任何进一步优化空间。

**部署与证据**

Java artifact：`/opt/kline-proxy/kline-proxy-1.8.9-performance-8353b503e7db.jar`；SHA-256：`8353b503e7dbed8e3ffc1f95d64d3f170ca5a136fd8ddd61cb9abca4685d4773`。保留 JDK 21.0.2、`-Xms1g -Xmx2g -XX:+UseG1GC`，未保留 20 ms pause target 或 ZGC。自动回退和之前的 snapshot 备份保留。

本地完整测试：266 项，265 通过、1 项跳过，0 失败、0 错误。部署前后核对 739 个合约 1h、732 个合约 1d 序列及选定现货数据，验证 snapshot 恢复与结果一致。最终运行状态、进程、hash 和重启计数见运行证据。

方法边界：每轮使用验证证书的 HTTP/1.1 TLS 长连接，固定目标 IP；从发出请求计时至读完响应体，JSON 解析校验在计时外。两服务交替测试顺序；所有计时尝试均计入，p99 使用 nearest-rank。优化前可用池为 732 个 symbol，最终池为 739 个；各轮同一组请求在 Java/Rust 之间配对，跨版本并非逐条相同的历史请求。冷 TLS 及外部 REST 未命中路径不属于本轮延迟上限结论。

最终基础请求窗口，Java 另有 1428 个非测试请求，Rust 为 0 个；连接预热 `/time` 单独统计。最终常规窗口宿主 CPU steal：Java 0.04%，Rust 1.14%。这类差异与 Java 持续生产流量决定了报告描述的是部署效果，不能证明语言本身的成本。

早期诊断驱动有两条未转义中文 symbol URL，已修正编码；修正后的 1.8.8 重跑与 JFR 文件提取短暂重叠，只用于正确性检查。最终 1.8.9 驱动使用修正版，不包含这些干扰。最终链路第一次启动因输出目录已存在退出，尚未发送请求；保留错误记录并修复驱动包装后重跑，没有删除慢请求。

可复核入口：[完整统计](../research/java-optimization-20260929/evidence/analysis.json)、[最终基础原始请求](../research/java-optimization-20260929/evidence/final-repeat/)、[五步链](../research/java-optimization-20260929/evidence/realistic189/analysis.json)、[两步链](../research/java-optimization-20260929/evidence/mixed189/analysis.json)、[20:00 整点](../research/java-optimization-20260929/evidence/hour20-summary.json)、[Java 运行身份](../research/java-optimization-20260929/evidence/final-java-runtime.json)、[Rust 运行身份](../research/java-optimization-20260929/evidence/final-rust-runtime.json)、[GC](../research/java-optimization-20260929/evidence/final-gc.json)、[JFR 概览](../research/java-optimization-20260929/evidence/realistic188-jfr-summary.json)、[JFR 锁等待](../research/java-optimization-20260929/evidence/realistic188-jfr-locks.json)、[部署记录](../research/java-optimization-20260929/evidence/activation-189.json)、[实现与约束](../research/java-optimization-20260929/implementation-notes.md)。
