"""Read-only spot check of the captured futures close against Binance's ordinary REST klines."""
import concurrent.futures
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import re
import urllib.parse
import urllib.request

ROOT = Path('/opt/kline-proxy/deployments/20260913_ingress_407afae_retry')
observation = json.loads((ROOT / 'observation-1800.json').read_text())
body = json.loads((ROOT / 'loopback-all-1800.json').read_text())
boundary = observation['boundary_ms']
tails = []
for line in observation['closed_bar_logs']:
    if 'CLOSED_BAR_LATENCY_DETAIL service=binanceFuture ' in line:
        fields = dict(re.findall(r'(\w+)=([^\s]+)', line))
        tails.append((float(fields['ready_offset_ms']), fields['symbol']))
assert tails
symbols = sorted({'BTCUSDT', 'ETHUSDT', 'ZKUSDT', 'UNITREEUSDT', max(tails)[1]})


def check(symbol):
    query = urllib.parse.urlencode(dict(symbol=symbol, interval='1h', startTime=boundary - 3_600_000,
                                       endTime=boundary - 1, limit=1))
    url = 'https://fapi.binance.com/fapi/v1/klines?' + query
    result = dict(symbol=symbol, endpoint='GET /fapi/v1/klines', query=query)
    try:
        with urllib.request.urlopen(url, timeout=12) as response:
            rows = json.load(response)
            result['status'] = response.status
        assert len(rows) == 1 and rows[0][0] == boundary - 3_600_000, rows
        actual = body['klines'][symbol][-1]
        expected = rows[0]
        differences = [dict(column=i, proxy=actual[i], rest=expected[i]) for i in range(11)
                       if Decimal(str(actual[i])) != Decimal(str(expected[i]))]
        result.update(proxy=actual, rest=expected, differences=differences, exact_numeric_equal=not differences)
    except Exception as error:
        result['error'] = str(error)
    return result


with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    results = list(executor.map(check, symbols))
output = dict(captured_utc=datetime.now(timezone.utc).isoformat(), boundary_ms=boundary,
              method='Compare first 11 numeric fields; ignore reserved column 11. Sample only, not all symbols.',
              results=results)
(ROOT / 'rest-verification.json').write_text(json.dumps(output, indent=2) + '\n')
print(json.dumps(output, indent=2))
assert all(r.get('exact_numeric_equal') for r in results), 'Inspect saved differences/errors before making a parity claim'
