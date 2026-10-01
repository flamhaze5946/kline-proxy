"""Request-path warm-up for a freshly restarted kline-proxy JVM (runs ON the proxy host).

The fleet sends POST /fapi/v1/fundingRate/bulk then POST /fapi/v1/klines/bulk only at the hour
(~192 concurrent chains, 3 symbols each). This replays that request shape off the hour against the
application port (no nginx/TLS), so those request paths have been exercised before the next
natural boundary.

That this lets the JIT compile the boundary-burst paths before :00 is a HYPOTHESIS, not a measured
result. Background (2026-09-30): after a restart, compiler threads used 37% of JVM CPU in the first
0.6 s of the third boundary, and fleet bulk p50 was 510 -> 291 -> 208 -> 104 ms over the following
hours. The warm-up cannot reproduce new-hour cache invalidation, the final-bar wait/wake-up, the
pre-boundary wait, funding publication grace, or nginx/TLS work, and per-symbol closed-kline views
become cache hits after their first request. Check compilation and fleet latency at the next
natural boundary before relying on it.

Side effects: read-only queries. Klines are served from in-memory caches (symbol discovery builds
one all-market, one-row response; no kline history is backfilled). The funding window spans five
hour chunks including the current hour; chunks not already cached (startup normally preloads
them), and exchange-metadata cache misses, are fetched from Binance REST through the shared
futures rate limiter. Otherwise the warm-up is cache-only. A single preflight chain runs first, so
an upstream problem shows up as one failed request instead of a 192-chain burst.

Bounds:
  * starts only inside [hh:05:00, hh:45:00) and re-checks that window before every round;
  * one absolute deadline for the whole run, discovery included: --deadline-s (default 120 s),
    and never past hh:45:00; per-request socket timeouts are clipped to the time left;
  * stops at the first failed chain (a failed funding request skips its kline request, and chains
    not yet started are skipped), or when a round's funding or bulk p99 exceeds --p99-limit-ms;
  * --rounds/--clients/--symbols must be positive and bounded; --symbols <= discovered symbols
    (an empty symbol list would turn every request into an all-market query).
Responses are validated: funding JSON must carry a fundingRates list for every requested symbol;
bulk JSON must carry klines for every requested symbol, finalized=true and no pending symbols.

    python3 proxy_warmup.py [--port 1888] [--rounds 40] [--clients 192] [--symbols 3]
                            [--deadline-s 120] [--p99-limit-ms 2000] [--timeout-s 10]

stdout: one JSON line per round, then one summary line ({"summary": true, ...}) that
proxy_deploy.sh parses. Exit status: 0 all rounds completed; 1 refused, failed or stopped early
(the service itself is never touched); 2 invalid arguments.
"""
import argparse
import concurrent.futures
import http.client
import json
import math
import random
import sys
import threading
import time

HOUR_S = 3600
HOUR_MS = HOUR_S * 1000
WINDOW_START_S = 5 * 60   # hh:05:00
WINDOW_END_S = 45 * 60    # hh:45:00 -- the run never goes past this
ROUND_PAUSE_S = 0.2
DISCOVERY_PATH = '/fapi/v1/klines/bulk?interval=1h&limit=1&closed_only=true'
FUNDING_PATH = '/fapi/v1/fundingRate/bulk'
BULK_PATH = '/fapi/v1/klines/bulk'


def wall_clock():
    """UTC epoch seconds (a seam for tests)."""
    return time.time()


def window_remaining_s(now):
    """Seconds left before hh:45:00, or None when `now` is outside [hh:05:00, hh:45:00)."""
    into_hour = now % HOUR_S
    if WINDOW_START_S <= into_hour < WINDOW_END_S:
        return WINDOW_END_S - into_hour
    return None


def bounded(kind, low, high):
    def parse(text):
        try:
            value = kind(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f'not a valid {kind.__name__}: {text!r}')
        if not low <= value <= high:  # also rejects NaN
            raise argparse.ArgumentTypeError(f'must be within [{low}, {high}], got {text}')
        return value
    return parse


def parse_args(argv):
    parser = argparse.ArgumentParser(description='Request-path warm-up for kline-proxy (see module docstring).')
    parser.add_argument('--port', type=bounded(int, 1, 65535), default=1888)
    parser.add_argument('--rounds', type=bounded(int, 1, 200), default=40)
    parser.add_argument('--clients', type=bounded(int, 1, 256), default=192, help='concurrent chains per round')
    parser.add_argument('--symbols', type=bounded(int, 1, 20), default=3,
                        help='symbols per chain; must not exceed the number of discovered symbols')
    parser.add_argument('--deadline-s', type=bounded(float, 1, 600), default=120.0,
                        help='absolute limit for the whole run, discovery included; also capped at hh:45:00')
    parser.add_argument('--p99-limit-ms', type=bounded(float, 1, 60000), default=2000.0,
                        help="stop after a round whose funding or bulk p99 exceeds this")
    parser.add_argument('--timeout-s', type=bounded(float, 0.1, 60), default=10.0,
                        help='per-request socket timeout (clipped to the time left before the deadline)')
    parser.add_argument('--force-window', action='store_true',
                        help='tests only: skip the :05-:45 window checks and the :45 cap')
    return parser.parse_args(argv)


class Client:
    """Thread-local keep-alive connections to 127.0.0.1; every request is bounded by the deadline."""

    def __init__(self, port, timeout_s, deadline):
        self.port = port
        self.timeout_s = timeout_s
        self.deadline = deadline
        self.local = threading.local()
        self.sent = 0
        self.sent_lock = threading.Lock()

    def request(self, method, path, body=None):
        """(status, elapsed_ms, payload); status 0 = transport failure. None = deadline reached first."""
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            return None
        timeout = min(self.timeout_s, remaining)
        connection = getattr(self.local, 'connection', None)
        if connection is None:
            connection = self.local.connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=timeout)
        else:
            connection.timeout = timeout
            if connection.sock is not None:
                connection.sock.settimeout(timeout)
        data = None if body is None else json.dumps(body).encode()
        headers = {'User-Agent': 'kp-warmup'}
        if data is not None:
            headers['Content-Type'] = 'application/json'
        with self.sent_lock:
            self.sent += 1
        started = time.perf_counter()
        try:
            connection.request(method, path, body=data, headers=headers)
            response = connection.getresponse()
            payload = response.read()
            status = response.status
        except (OSError, http.client.HTTPException):
            self.local.connection = None
            connection.close()
            if time.monotonic() >= self.deadline:
                return None  # cut by the deadline, not a service failure
            return 0, (time.perf_counter() - started) * 1000, b''
        return status, (time.perf_counter() - started) * 1000, payload


def parse_object(payload):
    try:
        doc = json.loads(payload)
    except ValueError:  # includes JSONDecodeError and UnicodeDecodeError
        return None
    return doc if isinstance(doc, dict) else None


def funding_problem(payload, group):
    doc = parse_object(payload)
    rates = doc.get('fundingRates') if doc else None
    if not isinstance(rates, dict):
        return 'funding: response has no fundingRates object'
    missing = [s for s in group if not isinstance(rates.get(s), list)]
    return f'funding: no fundingRates list for {missing}' if missing else None


def bulk_problem(payload, group):
    doc = parse_object(payload)
    klines = doc.get('klines') if doc else None
    if not isinstance(klines, dict):
        return 'bulk: response has no klines object'
    empty = [s for s in group if not (isinstance(klines.get(s), list) and klines.get(s))]
    if empty:
        return f'bulk: no klines for {empty}'
    if doc.get('finalized') is not True:
        return f"bulk: finalized={doc.get('finalized')!r}"
    if doc.get('pending'):
        return f"bulk: pending={doc.get('pending')!r}"
    return None


def status_problem(name, status):
    if status == 200:
        return None
    return f'{name}: transport error' if status == 0 else f'{name}: HTTP {status}'


def run_chain(client, group, hour_ms, stop):
    """One fleet-shaped chain: funding, then klines only if funding succeeded and nothing asked to stop.

    Returns funding_ms / bulk_ms (None when not completed), error (str or None) and not_sent
    (requests of this chain that were never completed because of a stop or the deadline).
    """
    out = {'funding_ms': None, 'bulk_ms': None, 'error': None, 'not_sent': 2}
    if stop.is_set():
        return out
    funding = client.request('POST', FUNDING_PATH,
                             {'symbols': group, 'since_ms': hour_ms - 4 * HOUR_MS, 'until_ms': hour_ms + 1, 'limit': 100})
    if funding is None:
        return out
    status, out['funding_ms'], payload = funding
    out['not_sent'] = 1
    problem = status_problem('funding', status) or funding_problem(payload, group)
    if problem:
        out['error'] = f'{problem} (symbols {group})'
        stop.set()
        return out
    if stop.is_set():
        return out
    klines = client.request('POST', BULK_PATH, {'symbols': group, 'interval': '1h', 'limit': 10, 'closed_only': True})
    if klines is None:
        return out
    status, out['bulk_ms'], payload = klines
    out['not_sent'] = 0
    problem = status_problem('bulk', status) or bulk_problem(payload, group)
    if problem:
        out['error'] = f'{problem} (symbols {group})'
        stop.set()
    return out


def q(values, p):
    if not values:
        return None
    values = sorted(values)
    return round(values[max(0, math.ceil(len(values) * p) - 1)], 2)


def run_round(number, pool, client, groups, hour_ms):
    stop = threading.Event()
    started = time.perf_counter()
    results = list(pool.map(lambda group: run_chain(client, group, hour_ms, stop), groups))
    wall_ms = (time.perf_counter() - started) * 1000
    funding = [r['funding_ms'] for r in results if r['funding_ms'] is not None]
    bulk = [r['bulk_ms'] for r in results if r['bulk_ms'] is not None]
    errors = [r['error'] for r in results if r['error']]
    row = {'round': number, 'wall_ms': round(wall_ms, 1), 'chains': len(groups), 'errors': len(errors),
           'not_sent': sum(r['not_sent'] for r in results),
           'funding_n': len(funding), 'funding_p50_ms': q(funding, .5), 'funding_p99_ms': q(funding, .99),
           'bulk_n': len(bulk), 'bulk_p50_ms': q(bulk, .5), 'bulk_p99_ms': q(bulk, .99)}
    if errors:
        row['error_sample'] = errors[0]
    return row


def run(args, client, deadline, state):
    """Returns (stop_reason, detail); (None, None) when every round completed."""
    response = client.request('GET', DISCOVERY_PATH)
    if response is None:
        return 'deadline', 'no time left for symbol discovery'
    status, _, payload = response
    if status != 200:
        return 'discovery', f'symbol discovery failed: HTTP {status}'
    doc = parse_object(payload)
    klines = doc.get('klines') if doc else None
    if not isinstance(klines, dict):
        return 'discovery', 'symbol discovery: response has no klines object'
    symbols = sorted(s for s, bars in klines.items() if isinstance(bars, list) and bars)
    state['symbols'] = len(symbols)
    if len(symbols) < args.symbols:
        return 'discovery', f'only {len(symbols)} symbols with data, --symbols is {args.symbols}'
    hour_ms = int(wall_clock()) // HOUR_S * HOUR_MS
    deck = random.Random(20261001)

    group = sorted(deck.sample(symbols, args.symbols))
    preflight = run_chain(client, group, hour_ms, threading.Event())
    state['preflight'] = {'symbols': group, 'funding_ms': preflight['funding_ms'] and round(preflight['funding_ms'], 1),
                          'bulk_ms': preflight['bulk_ms'] and round(preflight['bulk_ms'], 1), 'error': preflight['error']}
    if preflight['error']:
        return 'preflight', preflight['error']
    if preflight['bulk_ms'] is None:
        return 'deadline', 'deadline reached during the preflight chain'

    rounds = state['rounds']
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
        for number in range(args.rounds):
            if not args.force_window and window_remaining_s(wall_clock()) is None:
                return 'window', f'minute {time.gmtime(wall_clock()).tm_min} is outside :05-:45 before round {number}'
            remaining = deadline - time.monotonic()
            last_wall_s = rounds[-1]['wall_ms'] / 1000 if rounds else 0.0
            if remaining <= last_wall_s:
                return 'deadline', f'{remaining:.1f}s left before round {number}, last round took {last_wall_s:.1f}s'
            shuffled = symbols[:]
            deck.shuffle(shuffled)
            groups = [sorted(shuffled[(i * args.symbols + j) % len(shuffled)] for j in range(args.symbols))
                      for i in range(args.clients)]
            row = run_round(number, pool, client, groups, hour_ms)
            rounds.append(row)
            print(json.dumps(row), flush=True)
            if row['errors']:
                return 'errors', f"{row['errors']} failed chains in round {number}: {row['error_sample']}"
            if row['not_sent']:
                return 'deadline', f"deadline reached during round {number} ({row['not_sent']} requests not completed)"
            worst = max(p for p in (row['funding_p99_ms'], row['bulk_p99_ms']) if p is not None)
            if worst > args.p99_limit_ms:
                return 'p99', f'round {number} p99 {worst} ms > {args.p99_limit_ms} ms'
            time.sleep(max(0.0, min(ROUND_PAUSE_S, deadline - time.monotonic())))
    return None, None


def main(argv=None):
    args = parse_args(argv)
    started = time.monotonic()
    budget = args.deadline_s
    state = {'symbols': 0, 'preflight': None, 'rounds': []}
    client = None
    if not args.force_window:
        left = window_remaining_s(wall_clock())
        budget = 0.0 if left is None else min(budget, left)
    if budget <= 0:
        reason, detail = 'window', f'refusing to warm up at minute {time.gmtime(wall_clock()).tm_min}: allowed window is :05-:45'
    else:
        client = Client(args.port, args.timeout_s, started + budget)
        reason, detail = run(args, client, started + budget, state)
    rounds = state['rounds']
    print(json.dumps({'summary': True, 'completed': reason is None, 'stop_reason': reason, 'detail': detail,
                      'rounds': len(rounds), 'rounds_planned': args.rounds,
                      'requests': client.sent if client else 0,
                      'elapsed_s': round(time.monotonic() - started, 1), 'budget_s': round(budget, 1),
                      'p99_limit_ms': args.p99_limit_ms, 'symbols': state['symbols'],
                      'preflight': state['preflight'],
                      'first_round': rounds[0] if rounds else None,
                      'last_round': rounds[-1] if rounds else None}), flush=True)
    if reason:
        print(f'warm-up stopped ({reason}): {detail}', file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
