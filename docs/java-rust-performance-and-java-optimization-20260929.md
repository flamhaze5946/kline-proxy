**Java 与 Rust 性能复测及 Java 优化可行性｜2026-09-29**

Java 的普通请求已经与 Rust 接近，CPU 使用率在本轮还更低。明显差距集中在 200 个客户端同时查询 bulk，以及整点连接接入。Java 存在有直接证据支持的优化空间，但尚未实施这些优化，不能把“有希望接近”写成“已经可以达到 Rust”。

正式结果采用 17:23–17:25 UTC（迪拜时间 21:23–21:25）的补测，JFR 录制已结束，测试期间没有诊断 attach、类重定义、应用部署或重启。最初一轮受到诊断初始化干扰，保留原始证据，但不用于稳态比较。

Java 为线上 `market.feng.dog` 的 1.8.3，JDK 21.0.2；Rust 为 `market-mirror.feng.dog` 的 0.4.22。两台均为 2 vCPU Skylake VPS。东京测试机验证 TLS、固定目标 IP、使用 HTTP/1.1 连接复用，计时截至完整响应体读取结束，JSON 校验在计时外执行。

每服务测量 10 类常规请求、每类 500 次，另测 10 轮 200 并发 bulk；每客户端选择 5 个分散 symbol，取最近 10 根 closed 1h。共同可用池 732 个 symbol，交替服务测试顺序。共 14,000 个计时请求，无 HTTP 或内容校验错误；3,000 对常规 closed 数据及 2,000 对 bulk closed 数据全部一致。动态价格没有被要求在不同请求时刻逐字相同。

| 常规请求，单位 ms | Java p50 | Rust p50 | Java p99 | Rust p99 |
|---|---:|---:|---:|---:|
| 合约单币价格 | 4.17 | 4.08 | 6.80 | 7.26 |
| 合约全市场价格 | 4.02 | 3.74 | 7.72 | 7.23 |
| 现货单币价格 | 3.99 | 3.28 | 7.49 | 6.45 |
| 现货全市场价格 | 4.87 | 3.14 | 10.05 | 6.88 |
| 合约最近 10 根 closed 1h | 3.97 | 3.61 | 6.85 | 6.67 |
| 合约最近 10 根 closed 1d | 3.90 | 3.41 | 7.03 | 6.70 |
| 现货最近 10 根 closed 1h | 3.97 | 3.11 | 8.33 | 6.64 |
| 现货最近 10 根 closed 1d | 3.86 | 2.94 | 8.55 | 6.65 |
| 常规 bulk：5 symbol × 10 根 1h | 4.10 | 2.84 | 8.78 | 7.37 |
| 常规 bulk：5 symbol × 10 根 1d | 3.90 | 2.47 | 8.06 | 7.06 |

| 200 客户端 × 5 symbol bulk | Java 1.8.3 | Rust 0.4.22 |
|---|---:|---:|
| p50 | 65.28 ms | 15.17 ms |
| p95 | 124.86 ms | 24.91 ms |
| p99 | 140.80 ms | 27.26 ms |
| 最大 | 146.57 ms | 29.42 ms |
| 成功且数据正确 | 2,000 / 2,000 | 2,000 / 2,000 |
| TLS 连接复用 | 2,000 / 2,000 | 2,000 / 2,000 |
| Nginx upstream response p99 | 134 ms | 22 ms |
| Nginx upstream connect p99 | 17 ms | 0 ms（日志分辨率约 1 ms） |

本轮 bulk p99 的线上差距约 5.16 倍，Rust 延迟低约 80.6%。普通合约 K 线只有约 0.18 ms 的 p99 差距，合约单币价格则是 Java 略快。因此不能说 Rust 所有接口均快于 Java。

| 同一常规负载窗口，100.22 秒 | Java | Rust |
|---|---:|---:|
| 应用平均 CPU，单核＝100% | 14.08% | 18.85% |
| 进程平均 RSS | 1,525.26 MiB | 606.94 MiB |
| 宿主 CPU steal | 0.02% | 1.61% |

这是线上部署的表现。17:23:15–17:25:32 UTC 的日志窗口中，Java 另承接 1,466 个非测试请求，其中 1,458 个普通 K 线查询、8 个 exchangeInfo；Rust 没有对应额外流量。每服务另有 2,000 个测试驱动的 `/time` 连接预热请求，已单独分类，不能误算成生产流量。CPU 包含应用后台任务，内存是当前进程驻留量，不能当成语言最低内存或每请求 CPU。Java 虽承接额外流量，平均 CPU 仍较低；Rust 内存低约 60.2%。

对比主证据：[完整统计](../research/java-rust-optimization-20260929/results/analysis.json)、[正式原始请求](../research/java-rust-optimization-20260929/results/warm-validation/)、[进程采样](../research/java-rust-optimization-20260929/results/profiles/)、[Java Nginx](../research/java-rust-optimization-20260929/results/java-nginx-warm/)、[Rust Nginx](../research/java-rust-optimization-20260929/results/rust-nginx-warm/)。

**Java 的耗时已定位到具体路径。**

另在 17:19–17:20 UTC 单独录制 100 秒 JFR，并发送 6,000 个 Java bulk 请求。该诊断窗口与正式对比分开。取得 1,101 个 Java 执行栈样本，其中 716 个带 HTTP 调用栈。

| 热点 | HTTP 执行栈样本占比 | 代码行为及含义 |
|---|---:|---|
| K 线转换为展示数组和数字字符串 | 301 / 716，42.0% | `ConvertUtil.convertToDisplayKline → doubleToString`，重复格式化历史 closed 数据 |
| 获取并构造 TRADING symbol 集合 | 123 / 716，17.2% | `tradingSymbolsOrNull → querySymbols`，5 币查询也重新筛选全市场、构造集合 |
| 响应转换和 JSON 输出 | 103 / 716，14.4% | Spring/Jackson 每请求输出对象图，命中对象缓存也仍需编码 |

这是包含下层调用的采样归因，存在嵌套，不能相加当成精确 CPU 百分比，更不能直接换算成 p99 收益。没有把 allocation sample 的权重总和当成精确分配字节数。

当前 bulk 缓存只保留 64 个完整组合、有效期 1 秒。200 个分散组合不能全部驻留，不同组合之间也不能共享同一 symbol 的格式化结果。即使命中缓存，Controller 返回的仍是对象，由 Jackson 序列化。数字格式化使用 `ThreadLocal<DecimalFormat>`；虚拟线程按请求创建，不能像长寿命工作线程一样跨请求复用 formatter。JFR 同时采到了 formatter 初始化和大量格式化执行栈。

对应源码：[bulk 缓存、构建与币种集合](../src/main/java/com/zx/quant/klineproxy/service/impl/AbstractKlineService.java)、[数字转换](../src/main/java/com/zx/quant/klineproxy/util/ConvertUtil.java)、[交易币种过滤及 funding 缓存](../src/main/java/com/zx/quant/klineproxy/service/impl/BinanceFutureExchangeServiceImpl.java)、[Controller](../src/main/java/com/zx/quant/klineproxy/controller/BinanceFutureController.java)。诊断结果及口径见 [JFR 摘要](../research/java-rust-optimization-20260929/results/jfr-summary.json)，可用 [分析脚本](../research/java-rust-optimization-20260929/scripts/analyze_diagnostics.py) 重算。

**整点还有连接和入库排队两类问题。**

沿用同日 17:00 UTC、同一部署版本的整点证据，不将其冒充本次非整点复测：

| 17:00 UTC 实测 | Java | Rust |
|---|---:|---:|
| 合约 closed 全部可读 | T+148.76 ms | T+132.00 ms |
| 现货 closed 全部可读 | T+156.21 ms | T+137.02 ms |
| 200×5 bulk p99 | 1,023.19 ms | 115.40 ms |
| 最后一个 bulk 完整响应 | T+1,049.56 ms | T+138.03 ms |

Java 最慢请求的 Nginx upstream connect 为 1,018 ms，且出现 17 次监听队列溢出/丢弃。当前复核 Java 的监听 backlog 仍是 100，Rust 为 1,024。这条秒级尾延迟有连接接入/重传证据，不应归为 JSON 计算或 JIT。该整点 Java 首分钟另有 928 个生产请求，Rust 没有，不能用上表宣称语言本身相差约 9 倍。

Java 合约最后消息接收到达同为 T+132 ms，但消息任务排队最大 16.75 ms；现货排队最大 22.12 ms。当前 Java dispatcher 已有 closed 优先和 forming 合并，不能再把“增加 closed 优先队列”列为未实现功能。进一步方向是缩短已有提交路径、减少与 HTTP/后台任务的 CPU 争用，再验证分片拥堵。仅凭此轮不能保证从 Java 全部就绪时间中稳定减去 17–22 ms。

整点证据：[原始汇总](../../../../Rust/Personal/kline-proxy-rs/research/results/tail-fix-0419-20260929/hour-17-summary.json)。

**按收益与修改成本，建议如下。收益描述均为优化目标或可消除的开销，尚非改动后的实测结果。**

| 顺序 | 修改项目 | 收益依据 | 难度 |
|---|---|---|---|
| 1 | Tomcat `accept-count` 100→1,024；Nginx upstream `keepalive` 64→256/worker，`worker_connections` 768→2,048；协调两端连接空闲超时和请求数上限 | 优先消除已观察到的约 1 秒后端连接尾延迟；本轮普通 burst connect p99 还有 17 ms | 低，配置修改，但 Tomcat 参数需重启生效 |
| 2 | 交易所元数据更新时生成不可变 TRADING symbol Set，请求直接复用 | 直接针对约 17.2% 的 HTTP 执行栈热点，改动范围小 | 低 |
| 3 | 按 symbol / interval / limit / 已收盘修订版本缓存不可变展示窗口，优先最近 10 根 | 跨 200 个不同组合复用数据，避免重复数字格式化；约 42.0% 样本涉及该路径 | 中，需要正确失效与并发回归 |
| 4 | 复用已编码的 closed 行或窗口，bulk 只组装响应外层；限制大请求构建并发 | 进一步减少约 14.4% 响应转换热点及对象分配；需保留动态 `ts/pending/waited_ms` 语义 | 中 |
| 5 | funding 的同步冷加载放到有界独立执行器；single-flight 只在短临界区注册 Future | 保护 Java 21 虚拟线程承载线程，避免 funding 冷加载牵连 K 线；本次 K 线专项 JFR 未重现 funding 阻塞，属于仍存在的代码风险与旧证据 | 中 |
| 6 | 在减少分配后评估 GC、JDK 升级及 WS 提交调度，最后单独优化内存 | 正式窗口 GC 暂停最大 34.46 ms，已有 live heap 约 515–518 MiB；盲目缩堆可能增加 GC 尾延迟 | 中至高，需独立 A/B |

Nginx 的 `keepalive` 是每 worker 的空闲连接缓存大小，不是后端总并发上限。Java 目前 timeout 15 秒、requests 90；Tomcat 的 keep-alive 生命周期需要一并协调，不能只延长 Nginx 一端。长期保留连接的数量和内存需要实测，不能直接照搬 Rust 超时参数。依据：[Nginx upstream 文档](https://nginx.org/en/docs/http/ngx_http_upstream_module.html)、[Tomcat HTTP Connector 文档](https://tomcat.apache.org/tomcat-10.1-doc/config/http.html)。

当前 JDK 21.0.2 的同步阻塞风险可通过隔离冷加载处理；JDK 24 引入了减少 synchronized 场景虚拟线程 pinning 的改进，可作为后续升级验证方向，但升级不会自动去掉重复格式化，也不能在未做 Spring Boot/依赖兼容测试时承诺收益。依据：[Oracle JDK 24 变更说明](https://docs.oracle.com/en/java/javase/24/migrate/significant-changes-jdk-24.html)。

缓存优化必须复用当前数值格式与精度，不能直接替换为 `Double.toString()`。closed 修订、定时订正、整点切换、symbol 状态变化均要正确失效；forming 窗口与已收盘窗口分别维护版本。保持现有约束：不增加查询触发的 K 线 REST 回填，只有真实 WS `x=true` 之后才允许构造当前零成交量 bar。

**关于首轮异常和结论边界。**

首轮 17:16–17:18 的常规请求中，Java 出现最长约 1.07 秒响应；17:17:07 GC/safepoint 日志记录 `RedefineClasses`，暂停约 127.96 ms，之后 C2 编译线程在常规窗口消耗约 31.28 CPU 秒。本次诊断执行了 `JFR.check`；实例 JDK 源码显示该命令经 `getRecordings()` 调用 `FlightRecorder.getFlightRecorder()`，会进入 recorder 初始化路径。首轮按诊断初始化扰动处理，不当作自然发生的 Java 稳态故障。

正式补测没有 `RedefineClasses`，C2 同口径 CPU 约 0.75 秒；Java 平均 CPU 从首轮的 61.37% 恢复至 14.08%，普通请求 p99 回到 6–10 ms。保留两轮数据，是为了避免只展示对 Rust 有利的异常窗口。[两轮编译线程统计](../research/java-rust-optimization-20260929/results/java-thread-cpu.json)、[正式 GC 日志分析](../research/java-rust-optimization-20260929/results/warm-gc-analysis.json)。

因此，Java 的普通接口已处于相近性能范围，bulk 有明确且较大的工程优化空间；是否能把 140.8 ms 的 p99 压到 Rust 本轮 27.3 ms，需要按上表逐项改动并做 A/B，当前证据不能保证。验收应同时检查 200×5 请求、整点 closed 全部就绪、后台 funding 冷加载混合流量、至少多个自然整点，以及完整数据一致性，不能只测试一个热门 symbol 或提高缓存 TTL 隐藏数据更新。

本轮完成了测量、采样和代码排查，未修改 Java/Rust 应用及部署配置。Java PID 795641、Rust PID 225270，均无服务重启；最后 Rust 2470/2470 序列 ready。脚本及原始证据位于 [本次研究目录](../research/java-rust-optimization-20260929/)，原始 JFR 留在 Java VPS `/tmp/kline-cpu-diag-diagnostic.jfr`。
