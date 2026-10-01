**生产 Java / Rust 实盘请求成对重放｜2026-10-01（UTC）**

车队和扫描器在生产 Java 上的真实请求，同时发给生产 Java（`market.feng.dog`）和生产 Rust（`market-mirror.feng.dog`）逐条比较：已收盘的 K 线、资金费率和合约元数据全部一致。

## 收集

- **nginx 访问日志**（生产 Java，2026-10-01 00:09–09:07）：

  | 请求 | 次数 |
  |---|---:|
  | `GET /fapi/v1/klines` | 307,618 |
  | `POST /fapi/v1/klines/bulk` | 2,880 |
  | `POST /fapi/v1/fundingRate/bulk` | 2,880 |
  | `GET /fapi/v1/exchangeInfo` | 1,981 |

  GET klines 几乎都是 `symbol=…&interval=1h&limit=10&startTime=…`，去重后 84,646 种，其中 1,090 种的币种名是中文（如 `哈基米USDT`、`龙虾USDT`）。日志不含 POST 请求体。

- **10:00 整点抓包**：在生产 Java 的 `lo` 上用 tcpdump 只抓发往 1888 端口的请求方向（09:59:36–10:02:06，被动、不改配置），解析出 1,715 个请求，请求体全部完整：车队 192 个分片的 192 个 `klines/bulk` 和 192 个 `fundingRate/bulk`（都在整点后 5–198 ms 到达），另有 1,322 个扫描器 GET 和 9 个 `exchangeInfo`。nginx 加的 `X-Real-IP`、`X-Forwarded-For` 在解析时丢弃；原始 pcap 不在仓库中。

  车队的请求体：
  - `klines/bulk`：`{"interval":"1h","limit":5,"closed_only":true,"symbols":[6 个币]}`
  - `fundingRate/bulk`：`{"symbols":[5 个币],"since_ms":…,"until_ms":整点+60001,"limit":7}`

## 比较方法

同一条请求几乎同时发给两边（`paired_replay.py`，8 路并发、每秒 10 对），比较状态码和解析后的 JSON；每次调用都会变的 `serverTime`、`ts_ms` 不计。车队请求在 10:06:30 重放：资金费率时间窗是固定的，K 线只取已收盘的 bar，11:00 之前重放结果与整点时相同；资金费率缓存块在 :05 刷新后再发。GET 样本在 09:15 重放：日志中的中文币种全部保留，其余去重查询串固定种子抽 2,000 条。

## 结果

| 请求 | 对数 | 数据一致 | 其余差异 |
|---|---:|---:|---|
| `POST klines/bulk`（车队 10:00 全部分片） | 192 | 192 | Rust 多一个 `data_status` 字段 |
| `POST fundingRate/bulk`（车队 10:00 全部分片） | 192 | 192 | — |
| `GET klines`（10:00 扫描器） | 1,322 | 1,303 | 19 对只差正在形成的最后一根 bar |
| `GET klines`（日志抽样，含 1,090 个中文币种） | 3,090 | 3,080 | 9 对只差最后一根 bar；1 次本机超时 |
| `GET exchangeInfo` | 1 | 920 个合约逐字段相同 | JSON 键顺序不同，因此字节数不同 |

- `klines/bulk` 的 `klines`、`finalized`、`interval`、`not_trading`、`pending`、`waited_ms` 全部相同。
- Rust 的 `data_status`（`window_finalized`、`nonfinal_symbols`、`missing_latest`）是额外字段。nos-rs 的 `KlineProxyKlinesResponse`（`crates/nos-trading/src/kline_proxy_client.rs`）只声明 `interval`、`ts_ms`、`klines`，没有 `deny_unknown_fields`，解析时忽略它。
- 最后一根 bar 是当前小时正在形成的 bar，两次查询相隔几毫秒就可能不同（例如成交量 340904 对 341178）。

## 延迟只作参考

重放从开发机发出（不在东京），Rust 的客户端延迟中位 360 ms，Java 155 ms；1 MB 的 `exchangeInfo` 在 8 路并发下有 3 次超过 30 s。Rust 机器的 nginx 记录显示服务端处理 3,090 个 klines 请求 p99 为 1 ms，`exchangeInfo` 的响应头 0–1 ms 内发出，慢在本机到 `market-mirror` 的链路带宽。东京客户端到两台生产机的整点延迟见 [整点评估](java-rust-hour-20260930.md) 第 1 节。

## 材料

- 脚本：[`research/java-rust-replay-20261001/scripts/`](../research/java-rust-replay-20261001/scripts/)
  - `pcap_http.py`：从抓包还原 HTTP 请求。
  - `paired_replay.py`：成对重放与比较。车队重放时超时为 90 s，GET 样本重放时为 30 s。
  - `summarize.py`：按接口汇总。
- 输入：`input/`（车队请求、GET 样本、当日去重查询串）。
- 结果：`out/*/results.jsonl`，不一致的成对响应体在 `out/*/bodies/`。
