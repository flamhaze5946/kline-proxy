### Requirement
at least 500MB free memory, depends on your configuration.

### Configuration
refer to src/main/resources/application.yaml

### How to build

```shell
mvn clean package

```

### How to launch

```shell
java $JAVA_OPTS -jar kline-proxy-1.4.6.jar --spring.config.location=file:/path/application.yaml
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
