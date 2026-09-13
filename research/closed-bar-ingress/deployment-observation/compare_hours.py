"""Validate both live hour observations, probe payloads, ingress accounting and CPU units."""
import argparse
import json
from pathlib import Path
import re

from analyze import analyze
from summarize_cpu import thread_window


def metric_values(raw):
    values = {}
    for line in raw.splitlines():
        if not line.startswith('websocket_kline_ingress_'):
            continue
        token, value = line.split()
        match = re.fullmatch(r'([^{]+)(?:\{(.*)\})?', token)
        assert match, token
        name, labels = match.groups()
        parsed = tuple(sorted(re.findall(r'(\w+)="([^"]*)"', labels or '')))
        values[(name, parsed)] = float(value)
    return values


def ingress_accounting(observation):
    before = metric_values(observation['metrics_before'])
    after = metric_values(observation['metrics_after'])
    if not after:
        return dict(available=False)

    def value(name, labels=()):
        return after[('websocket_kline_ingress_' + name, tuple(sorted(labels)))]

    assert value('failures_total') == 0
    queued_final = value('queue', (('closed', 'true'),))
    queued_forming = value('queue', (('closed', 'false'),))
    active = value('active_workers')
    final_in = value('received_total', (('closed', 'true'),))
    final_out = value('processed_total', (('closed', 'true'),))
    assert final_in == final_out
    forming_in = value('received_total', (('closed', 'false'),))
    forming_out = value('processed_total', (('closed', 'false'),))
    superseded = value('coalesced_total', (('reason', 'superseded'),))
    stale = value('coalesced_total', (('reason', 'stale'),))
    forming_unaccounted = forming_in - forming_out - superseded - stale
    delta = [{'metric': name, 'labels': dict(labels), 'delta': end - before[(name, labels)]}
             for (name, labels), end in after.items() if name.endswith('_total')]
    return dict(available=True, final_received=final_in, final_processed=final_out,
                forming_received=forming_in, forming_processed=forming_out,
                superseded=superseded, stale=stale, failures=0,
                queued_final=queued_final, queued_forming=queued_forming, active_workers=active,
                forming_not_in_completed_or_coalesced=forming_unaccounted,
                backpressure_count=value('backpressure_total'), deltas=delta,
                scope='Non-atomic metrics snapshots: forming can be queued, running or concurrently admitted. Final completion equality and zero failures are separately asserted.')


def validate_hour(directory, hour):
    stamp = str(hour) + '00'
    observation = json.loads((directory / ('observation-' + stamp + '.json')).read_text())
    result = analyze(observation)
    result['thread_cpu'] = thread_window(observation)
    result['ingress'] = ingress_accounting(observation)
    public = json.loads((directory / 'public-summary.json').read_text())
    assert len(public) == 2
    loopback = observation['http_probes']
    assert len(loopback) == 2
    payloads = {}
    for probe in loopback + public:
        label = probe['label']
        assert probe.get('http_status', probe.get('status')) == 200, probe
        assert probe['finalized'] and not probe['pending'] and not probe['wrong_last_open_times'], probe
        filename = label + ('-' + stamp + '.json' if label.startswith('loopback') else '-body.json')
        body = json.loads((directory / filename).read_text())
        assert body['finalized'] and not body['pending'] and not body['not_trading']
        assert len(body['klines']) == probe['symbols']
        assert all(len(rows) == 2 and rows[-1][0] == observation['boundary_ms'] - 3_600_000
                   for rows in body['klines'].values())
        payloads[label] = body['klines']
    assert payloads['loopback-all'] == payloads['public-all']
    assert payloads['loopback-six'] == payloads['public-six']
    assert all(payloads['loopback-all'][s] == rows for s, rows in payloads['loopback-six'].items())
    result['probes'] = loopback + public
    result['boundary_ms'] = observation['boundary_ms']
    result['probe_data_verified_equal'] = True
    result['expected_symbols'] = sorted(payloads['loopback-all'])
    result['scope'] = ('One live hour, not a controlled repeated A/B. Server readiness and full HTTP response receipt have different meanings. '
                       'Public probe clock correction is a midpoint estimate; retain half-RTT uncertainty.')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    old = validate_hour(args.root / 'baseline-1700', 17)
    new = validate_hour(args.root / 'optimized-1800', 18)
    assert old['expected_symbols'] == new['expected_symbols'], 'Symbol cohort changed between hours'
    expected = sum(m['summary']['n'] for m in new['markets'].values())
    final_delta = next(r['delta'] for r in new['ingress']['deltas']
                       if r['metric'].endswith('received_total') and r['labels'] == {'closed': 'true'})
    assert final_delta == expected
    comparison = {}
    for market in old['markets']:
        a, b = (v['markets'][market]['summary'] for v in (old, new))
        comparison[market] = {}
        for metric in ('event_offset_ms_max', 'receive_offset_ms_max', 'queue_ms_p50', 'queue_ms_max',
                       'ready_offset_ms_max', 'done_offset_ms_max'):
            if metric in a and metric in b:
                comparison[market][metric] = dict(old=a[metric], new=b[metric],
                                                  difference_ms=b[metric] - a[metric],
                                                  reduction_percent=(a[metric] - b[metric]) / a[metric] * 100)
    output = dict(old=old, new=new, comparison=comparison)
    (args.root / 'hour-comparison.json').write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps(dict(comparison=comparison, ingress=new['ingress']), indent=2))
