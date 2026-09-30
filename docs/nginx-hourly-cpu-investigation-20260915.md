# Nginx 整点 CPU 排查：2026-09-15

**已抓到自然整点的直接证据：13:00 迪拜时间第一秒出现 383 次不使用会话恢复的 TLS 1.3 握手，同时完成 384 次 bulk 请求。约 550ms 内，Nginx 消耗 463ms CPU，约一半 CPU 样本的调用栈包含 TLS 握手。** 集中建立短连接，使握手和 TCP 处理与 Java 收盘计算同时争用双核，是本轮 Nginx 高峰的主要解释。

本轮在生产实例上测量 Nginx worker 的 CPU 时间、采集调用栈，并比较相同 HTTPS 工作量使用不同连接方式的成本。证据目录：[nginx-cpu-review-20260915](../research/closed-bar-ingress/evidence/nginx-cpu-review-20260915/)。

## 13:00 自然整点：直接观测

| 指标 | 13:00 迪拜时间 / 09:00 UTC |
|---|---:|
| 第一秒 K 线 bulk / 资金费率 bulk 响应数 | 192 / 192 |
| 第一秒普通 K 线响应数 | 11 |
| 第一秒新 TCP 连接、ClientHello、ServerHello 数 | 各 **383** |
| 上述 TLS 握手使用会话恢复 | **0 / 383** |
| ClientHello 声明支持 HTTP/2 / 提供 TLS 1.3 PSK | **383 / 0** |
| 同秒由客户端先发出 TCP 关闭信号的新 TLS 连接 | **378 / 383** |
| 约 T 至 T+550ms，Nginx CPU | **463.31ms** |
| 同窗口 JVM CPU | **490ms** |
| 同窗口整机 CPU 忙碌比例 | **90.8%** |
| 同窗口握手调用栈样本 | **46 / 92，约 50%** |
| 本小时 K 线 / 资金费率 bulk p99 | **127 / 26ms** |

整点前 45 秒未观察到新 TLS 握手；第一秒 383 条连接均协商 TLS 1.3、`TLS_AES_256_GCM_SHA384`、X25519，ServerHello 均未选择 PSK。它们不是使用 ML-KEM 的混合密钥交换，因此不能把这次自然高峰归因于 OpenSSL 的后量子默认组。[IANA TLS 组编号](https://www.iana.org/assignments/tls-parameters/tls-parameters.xhtml#tls-parameters-8)。

CPU 窗口实际为 T−7.046 至 T+543.474ms，长 550.520ms。Nginx 平均占用 **0.842 个核**，即双核容量约 **42.1%**；整点前约一秒仅消耗 **4.73ms CPU**。同窗口 `libcrypto` 和 `libssl` 叶子样本合计 41.3%，内核也是 41.3%，libc 未知符号 14.1%。握手调用栈比例包含其栈上的内核执行，不能与内核比例相加。样本数为 92，百分比是近似分布。

这批连接的 ServerHello 发送于 T+31.57 至 T+808.05ms。384 次 bulk 响应与 383 次全新握手在同秒集中出现，且绝大多数连接很快关闭，直接支持“批量短连接触发握手高峰”。378 条同秒关闭的连接，其第一条 TCP FIN/RST 均来自客户端方向。HTTP 日志缺连接 ID，TLS 数据没有解密，不能声称逐条请求已与某条连接一一对应；也尚未定位调用端具体是哪段代码关闭连接或丢弃连接池。

这轮 193 次 K 线 bulk、193 次资金费率 bulk 均为 HTTP 200。K 线 p99 为 127ms，低于 12:00 的 190ms，即使 Nginx CPU 更高；这也说明 Nginx CPU 与 p99 不是简单线性关系，Java 的就绪时序和调度仍会影响结果。

报告整理时复核了百分位口径：既有脚本采用排序后零基下标 `floor((n−1)×0.99)`。本次各 193 个样本按该口径为 127/26ms；若采用最近秩 `ceil(n×0.99)−1`，则为 133/26ms。本文保留既有结果，后续对照需固定同一算法；这里的样本范围是整点前后约 75 秒的采集窗口，并非整小时全部请求。面向实施的汇总见 [Nginx 与 Lambda 优化报告](nginx-lambda-hourly-optimization-report-20260915.md)。

按生产 Nginx 1.26.3 的 HTTP/1.1 处理路径，TLS 握手完成后才进入 HTTP 请求读取并初始化请求计时。因此 access log 的请求耗时不包含前面的 TLS 握手全过程；客户端端到端时间还会包含建连与握手。握手造成的 CPU 争用可以间接延长 HTTP 请求阶段，不能把握手 CPU 时间直接加到某条 bulk p99 上。[Nginx 1.26.3 请求处理源码](https://github.com/nginx/nginx/blob/release-1.26.3/src/http/ngx_http_request.c#L753)。

采集了 12,361 个包，无内核丢包报告、无解析跳过包、无 TLS 解析错误；抓包末尾约一秒可能仍有缓冲中的包，目标整点完整覆盖。perf 未报告丢失样本。CPU 采样器自身同窗口消耗约 50ms CPU；perf、tcpdump 的附加开销没有单独隔离，不能将 463ms 精确当作完全无观测时的数值。观测未产生诊断 HTTP 请求。

## 已完成的对照

12:24–12:26 迪拜时间（08:24–08:26 UTC），从实例外访问真实 HTTPS 入口，限制同时活动的连接为 16。每轮 bulk 包含 192 次 K 线和 192 次资金费率查询；查询已完成的 K 线和已缓存的近期资金费率窗口。两种模式各跑两轮，四轮返回体均为 **767,834 字节**，请求均为 HTTP 200。K 线响应均 finalized、无 pending、`waited_ms=0`，每次 6 币 × 4 根。

| 指标 | 每次请求新建 TLS 连接 | 复用已建立的 16 条 TLS 连接 |
|---|---:|---:|
| 每轮 bulk 请求数 | 384 | 384 |
| 每轮返回体字节数 | 767,834 | 767,834 |
| Nginx CPU，第一轮 | 690.38ms | 143.20ms |
| Nginx CPU，第二轮 | 654.08ms | 119.25ms |
| Nginx CPU，平均 | **672.23ms** | **131.23ms** |
| 估算扣除背景后的 CPU，平均 | **571.46ms** | **109.14ms** |
| TLS 库占 CPU 样本比例 | **47.3%** | **7.1%** |
| 可直接追到 TLS 握手调用栈的样本比例 | **40.3%** | **0%** |

复用连接时，原始 CPU 均值下降 **80.5%**；按测试前后背景速率估算校正后下降 **80.9%**。新建连接两轮实测耗时约 20 秒，复用连接约 4 秒，因此原始 CPU 不能忽略背景流量。校正采用两次约 2.3 秒观测窗口的平均背景速率 **4.96ms CPU/秒**，只是估算，不能当作完全隔离的对照。

复用模式的初始 16 次握手发生在 CPU 计量窗口之外；结果表示“请求使用已建立连接”的成本，不表示建立连接免费。新建模式刻意不恢复 TLS 会话，全部为 TLS 1.3 / `TLS_AES_256_GCM_SHA384`。它证明连接方式的成本差异，不能独自证明真实客户端在自然整点采用同样方式。

## CPU 具体花在哪里

用 Linux perf 对两个生产 Nginx worker 按 499Hz 采样，以 Debian 对应 build ID 的 Nginx、OpenSSL 调试符号及内核符号解析调用栈。新建连接两轮共有 653 个样本，复用两轮 127 个；没有报告丢失样本。

新建模式的样本中，`libcrypto` 为 39.8%、`libssl` 为 7.5%、内核为 33.8%。调用栈直接出现 `ngx_ssl_handshake`，包括密钥交换、签名、哈希及握手消息处理；内核栈则包含 TCP 收发、socket 创建/关闭、连接跟踪、软中断与虚拟网卡发送。连接复用会同时减少握手和连接生命周期工作，不能把约 81% 的 CPU 差全部算作加密算法本身。

复用模式下内核的相对占比升到 59.1%，但总 CPU 已大幅降低，不能据此说内核工作变多。Nginx 到 Java 当前也没有配置 upstream keepalive，是另一项可独立验证的连接成本。

最初 perf 未显式设置时钟，时间戳与 Python monotonic 相差约 15.906 秒。分组使用事后 monotonic/raw 校准，剔除各组边缘 100ms，并做正负 100ms 对齐敏感性检查：握手占比仍约 40.2%–40.3%，复用组仍无握手样本。自然整点采样显式使用 `CLOCK_MONOTONIC`，避免此问题。

运行中的旧 libc 映射已从文件系统删除；虽能读取原 ELF，但相应调试文件不可用。该库的未知符号保留为未知，没有用邻近导出函数冒充精确解析。解析只替换原有帧的名称，不补造原先未展开的调用栈。因此握手调用栈占比是可直接识别的部分。

## 与此前自然整点的关系

此前 10:00、11:00、12:00 迪拜时间，在约 T 至 T+550ms 的窗口内，Nginx 分别消耗约 **390、410、420ms CPU**。12:00 同窗口 JVM 为约 **560ms CPU**，整机忙碌比例 **95.4%**。420ms / 550ms 相当于平均占用约 **0.76 个核**，约为这台双核实例总容量的 **38%**。JVM 的 CPU 时间已经包含 JIT，不能再把编译时间加一次。

12:00 第一个完成秒有 **394 次 HTTP 响应**，其中 192 次 K 线 bulk、192 次资金费率 bulk、10 次普通 K 线查询；返回体合计 **834,864 字节**。日志是请求完成时间，并非精确到达时间。整点批次会把原本可以分散执行的代理、TLS 和网络工作集中起来，与 Java 的收盘处理争用两个核。

旧 access log 没有连接 ID、每连接请求数或 TLS 会话恢复字段，不能从它反推那一小时有多少次完整握手。为补足这一点，已完成 **13:00 迪拜时间 / 09:00 UTC** 的被动观测：T−45 至 T+30 秒抓取服务入口的加密 TCP 流量及 Nginx 栈，同时在整点附近按 20ms 采样 CPU，结果见本文开头。不能把 13:00 的握手计数直接当作此前每小时的计数。

## 已核实的配置与排除项

- Nginx **1.26.3**，两个 worker，各绑定一个 CPU；JVM 与它们共用双核实例。增加 worker 数不会增加 CPU 容量。
- OpenSSL **3.5.7**，证书为 **ECDSA / P-256**，不是大 RSA 私钥运算。
- 虽然配置有 `gzip on`，但 bulk 返回 `application/json`，实测声明 `Accept-Encoding: gzip` 时也没有 `Content-Encoding`，采样没有显示压缩是主要开销。
- 当前未显式设置 `ssl_session_cache`，但两次只读探针中，第二次成功恢复 TLS 1.3 会话。客户端的 `session_reused=true` 与被动解析 ServerHello 的 PSK 选择结果一致。**不能把“没有共享 session cache”解释为“服务端不支持会话恢复”。**
- 重新解析自然整点的 ClientHello：383 次全部提供 ALPN `h2,http/1.1`，没有一次提供 TLS 1.3 `pre_shared_key` 扩展。因此这批握手没有恢复会话，并非仅能解释为服务端拒绝了客户端提供的 ticket。Nginx 已编译 HTTP/2 模块，但有效配置只有 `listen 443 ssl;`，没有启用 HTTP/2。解析无不完整 ClientHello 或错误；汇总见 [`client-hello-capabilities.json`](../research/closed-bar-ingress/evidence/nginx-cpu-review-20260915/natural-hour09/client-hello-capabilities.json)。
- 当前无 upstream keepalive、未指定 `proxy_http_version`。生产版本 1.26.3 需要显式设置上游连接池、HTTP/1.1 和适当的 Connection 头，不能套用 Nginx 1.29.7 的新默认值。[Nginx 上游连接文档](https://nginx.org/en/docs/http/ngx_http_upstream_module.html#keepalive)。

Nginx 官方同样将握手列为 HTTPS 的主要 CPU 成本，并建议复用连接和会话；实际收益仍取决于客户端是否复用。[HTTPS 优化说明](https://nginx.org/en/docs/http/configuring_https_servers.html#optimization)、[SSL session cache 语义](https://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_session_cache)。

## 优化优先级

用户补充：调用端是一组整点启动的 AWS Lambda 函数。因此连接复用建议必须限定在同一个执行环境内；不能让不同 Lambda 执行环境共享一个进程内连接池，也不能依赖上个小时的 TCP 连接仍然存活。Lambda 的执行环境可能复用，空闲连接也可能失效；整点被调用不等于每次都冷启动，冷启动需用调用端的初始化记录确认。[AWS Lambda 连接复用建议](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)、[执行环境与并发](https://docs.aws.amazon.com/lambda/latest/dg/concepts-how-lambda-runs-code.html)。

自然整点的握手和关闭证据仍成立，但它们不足以证明调用端错误地销毁 Client：很多独立函数分别发起少量请求，本身就会建立很多独立连接。控制回放的 16 个长期复用连接不能直接代替整组 Lambda 的执行模型。

新的优先方向是验证 HTTP/2：捕获的 383 次 ClientHello 均声明支持它，服务端模块也已具备。如果每个 Lambda 并行请求 K 线和资金费率，HTTP/1.1 下即使共享 Client，也可能为两个并发请求分别建连；同一条 HTTP/2 连接可承载两个并行流。192 次 K 线加 192 次资金费率请求与“192 个函数各发两个请求”的模式吻合，但请求与 Lambda 调用还未一一关联，本仓库也没有调用端实现。需验证实际 Client 生命周期和首次并发建连行为，不能仅凭 ALPN 声明保证两条连接自动合并。[Nginx HTTP/2 模块与启用方式](https://nginx.org/en/docs/http/ngx_http_v2_module.html)。

| 优先级 | 方向 | 本轮证据与预期 |
|---|---|---|
| 1，若每个函数请求多个接口 | 先验证 Nginx 开启 HTTP/2，并确认同一 Lambda 的请求共享 Client 和连接 | 客户端已声明支持，服务端已有模块；若每个函数原来为两个请求各建一条连接，实际合并后可将该函数的握手数从两次降为一次。首次并发请求的建连行为需要实测；这不是 CPU 或 p99 减半的承诺。 |
| 2，可独立从实例侧验证 | 为 Nginx → Java 显式配置并验证 upstream keepalive | 当前 1.26.3 配置没有上游连接池；减少重复本机 TCP 建连、关闭。不能替不同 Lambda 复用其到 Nginx 的 TLS 连接，此项独立收益尚未测量。 |
| 3，若允许提前触发 | 提前启动同一次 Lambda 调用，在整点前预建业务连接，再在函数内等到整点 | 握手移出收盘高峰；预建和业务必须使用同一次调用中的同一连接池。等待会增加调用时长与费用。另一次预热调用不能保证命中后续调用的执行环境。 |
| 4，补充措施 | 在执行环境初始化阶段创建可复用的 Client，验证 session/ticket，再按需调整服务端有效期 | 同一环境后续调用可能受益，但跨小时连接和执行环境都不作存活保证。服务端已经可以恢复会话；仅增加服务端 cache 不能替客户端保存并发送 ticket。 |

如果客户端不能有效复用 HTTP/2，可再评估合并 K 线和资金费率接口；合并需要保留各自的数据就绪、快照和错误语义。验证 HTTP/2 时，应同时检查协商协议、每条连接的请求数、握手数、Nginx CPU 与调用端端到端延迟，而不是仅看 HTTP access log 的 p99。

如果触发器使用 EventBridge Scheduler，其调度精度是 60 秒，不能把 cron 直接当作毫秒级整点触发器；提前触发方案需要留足启动余量，再在同一次函数执行中对齐业务时间。[EventBridge Scheduler 调度精度](https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html)。用户尚未说明实际触发器，因此这里是条件说明。

Provisioned Concurrency 可提前完成 Lambda 执行环境初始化，但不会自动建立并保持到本服务的 TLS 连接。是否需要购买应依据 Lambda 自身的冷启动与端到端延迟记录判断，不能仅因本服务的握手 CPU 高就认定它能解决问题。[AWS 预置并发说明](https://docs.aws.amazon.com/lambda/latest/dg/provisioned-concurrency.html)。

控制回放使用 16 并发、提前建立连接，时间分布与生产整点突发不同；生产客户端也使用不同的 TLS 密钥交换组。约 81% 是本轮对照的 CPU 降幅，不是对线上 p99 或整点 CPU 降幅的承诺。已确认客户端声明支持 HTTP/2，但生产并发、每个函数的请求数及连接池行为仍需结合调用端验证。

## 方法、证据与边界

CPU 来自两个单线程 worker 的 `/proc/PID/schedstat` 实际运行纳秒增量，而非按请求耗时推算。HTTP 的响应时间、跨核累计 CPU 时间和整机利用率分开报告。初始对照额外包含两轮各 192 次 `/fapi/v1/time` 请求，全部对照计量请求合计 1,920 次，日志数量与客户端逐组一致。准备连接的请求另计。

调用栈文本、匿名请求元数据、可重算脚本及文件哈希保存在证据目录。仓库不保存原始 perf 栈内存或原始 pcap；网络证据只输出时间、计数和公开 TLS 协商字段。被动 TLS 解析器已用一次完整握手和一次恢复会话与客户端状态交叉验证，35 个包中无跳过包和解析错误。解析遵循 [TLS 1.3 ServerHello / PSK 结构](https://www.rfc-editor.org/rfc/rfc8446)。

本轮未修改生产 Java、Nginx 配置或进程参数；采样开销没有完全隔离。观察任务于 13:00:47 完成；13:04 已核实观察器、perf 和 tcpdump 均退出，Java 与 Nginx 原进程继续运行，健康检查为 UP。本文解释的是 Nginx 的 CPU 消耗机制；没有把它直接等同为此前所有 500–600ms bulk p99 的唯一原因。
