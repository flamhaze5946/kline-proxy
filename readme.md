### Requirement
at least 500MB free memory, depends on your configuration.

### Configuration
refer to src/main/resources/application.yaml

K 线落盘避让配置见 [周期边界避让说明](docs/kline-persistence-boundary-guard.md)。

Java 现货、合约日线的快照配置与实机重启验证见 [日线快照记录](docs/java-daily-snapshot-20260929.md)。

价格查询使用全市场 WebSocket 更新的内存价格簿，流正常且价格已缓存时不获取 REST 额度。
现货使用 `!miniTicker@arr`，合约使用 `!ticker@arr`；断流或缺失价格时保留带时间戳保护的 REST 回退，
历史 K 线不会覆盖停牌、无价格响应的判定。与 bulk、收盘缓存优化的整合和验证见 [master 合并记录](docs/master-merge-20260930.md)。

收到上一根 WebSocket `x=true` 后的临时补零规则见 [当前周期占位说明](docs/current-zero-bar-20260928.md)。

Java 1.8.9 的性能优化、VPS 部署和 Rust 对比见 [性能复测报告](docs/java-performance-optimization-20260930.md)。

### How to build

```shell
mvn clean package

```

### How to launch

```shell
java $JAVA_OPTS -jar kline-proxy-1.7.11.jar --spring.config.location=file:/path/application.yaml
```

### How to use

#### browser
##### spot klines
http://localhost:8888/api/v3/exchangeInfo
http://localhost:8888/api/v3/time
http://localhost:8888/api/v3/ticker/24hr
http://localhost:8888/api/v3/ticker/price
http://localhost:8888/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=100

##### future klines
http://localhost:8888/fapi/v1/exchangeInfo
http://localhost:8888/fapi/v1/time
http://localhost:8888/fapi/v1/fundingRate
http://localhost:8888/fapi/v1/premiumIndex
http://localhost:8888/fapi/v1/ticker/24hr
http://localhost:8888/fapi/v1/ticker/price
http://localhost:8888/fapi/v1/klines?symbol=BTCUSDT&interval=1d&limit=100

#### composite functions
http://localhost:8888/bapi/composite/v1/public/cms/article/catalog/list/query

#### python.ccxt
```python
import ccxt

if __name__ == '__main__':
    binance = ccxt.binance({
        'urls': {
            'api': {
                'public': 'http://localhost:8888/api/v3',
                'fapiPublic': 'http://localhost:8888/fapi/v1'
            }
        }
    })
    params = {
        'symbol': 'BTCUSDT',
        'interval': '1d',
        'limit': 100
    }
    for i in range(500000):
        klines = binance.fapiPublicGetKlines(params)
        print(klines)
        
    for i in range(500000):
        klines = binance.publicGetKlines(params)
        print(klines)
```
