### How to build

```shell
mvn clean package

```

### How to launch

```shell
java $JAVA_OPTS -jar kline-proxy-1.0.0.jar --spring.config.location=file:/path/application.yaml
```

### How to use

#### browser
http://localhost:8888/fapi/v1/klines?symbol=BTCUSDT&interval=1d&limit=100
http://localhost:8888/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=100

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