# Java master 合并记录

2026-09-30 将本地尚未提交的 Java 查询性能优化、收盘确认后的零成交量 bar、持久化边界保护、日线快照配置及回归测试，和远端 master 的 9 个 ticker 价格簿修复提交整合。

- 本地原始 master：`29fb85d`。
- 本地工作保存提交：`f41ea69`。
- 本次纳入的远端 master：`d24d4e1`。
- 合并提交：`6f8be4a`。

唯一产生文本冲突的文件是 `AbstractKlineService.java`，涉及 import 和价格查询入口。保留远端价格簿的时间戳排序、无价格状态、停牌、断流回退和 spot miniTicker 订阅；保留本轮 closed K 线展示/编码缓存、final-bar 精确唤醒、直接解码、阻塞冷加载隔离和 snapshot 改动。

原先从任意 K 线 close 直接返回 ticker 的捷径由价格簿命中路径接替，避免陈旧 K 线绕过远端的无价格/停牌保护。`SingleTickerMemoryFirstTest` 改为使用真实 spot/futures service 的 ticker 事件处理器更新价格簿，验证内存命中零 REST/限流、后续事件立即可见、K 线尚未初始化时仍可返回价格，以及旧 K 线不能覆盖价格簿。保留未命中时的调用顺序、权重、错误和响应格式检查。

JDK 21.0.8 完整 `mvn -B -ntp test`：317 项，316 通过、1 项既有跳过，0 失败、0 错误。随后执行 `mvn -B -ntp -DskipTests package`；测试计数和构建结果保存于 [验证摘要](../research/master-merge-20260930/verification.json)。

本次提交的性能报告保留各自测量时的源码/二进制身份。例如同机对照使用线上 Java JAR 的 SHA-256 `8353b503e7dbed8e3ffc1f95d64d3f170ca5a136fd8ddd61cb9abca4685d4773`；新增的 ticker 价格簿整合未参与该次性能测量，不能把此前数字视为合并后整体程序的实测。

报告直接引用的必要证据、分析与复现脚本一并保存。完整同机请求记录以压缩包保存，使用方法见 [实验说明](../research/isolated-current-20260930/README.md)。运行时依赖、编译产物、重复的原始大文件和历史工作目录仍保留在本地，不随此合并提交。
