"""Validate and decompose the observed hour, preserving raw log provenance."""
import json
from pathlib import Path
import re

ROOT = Path(__file__).parent

def parse(line):
    match = re.search(r'\b(CLOSED_BAR_\w+)\s+(.*)', line)
    assert match
    fields = dict(re.findall(r'(\w+)=([^\s]+)', match[2]))
    for key, value in fields.items():
        try:
            fields[key] = float(value) if '.' in value else int(value)
        except ValueError:
            pass
    fields['kind'] = match[1]
    return fields

def analyze(source):
    records = [parse(line) for line in source['closed_bar_logs']]
    results = {}
    for market in ('binanceFuture', 'binanceSpot'):
        group = [row for row in records if row.get('service') == market and row.get('interval') == '1h']
        summaries = [row for row in group if row['kind'] == 'CLOSED_BAR_LATENCY']
        settles = [row for row in group if row['kind'] == 'CLOSED_BAR_SETTLED']
        assert len(summaries) == len(settles) == 1, (market, summaries, settles)
        summary, settled = summaries[0], settles[0]
        assert summary['boundary'] == settled['boundary'] == source['boundary_ms']
        assert summary['n'] == summary['ev_n'] == settled['arrived'] == settled['expected']
        assert summary['overflow'] == 0
        clients = [row for row in group if row['kind'] == 'CLOSED_BAR_LATENCY_CLIENT']
        assert sum(row['n'] for row in clients) == summary['n']
        details = [row for row in group if row['kind'] == 'CLOSED_BAR_LATENCY_DETAIL']
        assert 1 <= len(details) <= 10
        assert len({row['symbol'] for row in details}) == len(details)
        before_ready = ['frame_to_enqueue_ms', 'queue_ms', 'json_ms', 'dispatch_ms', 'decode_ms', 'cache_ms', 'finalize_ms']
        for row in details:
            assert all(row[key] >= 0 for key in before_ready + ['settle_ms', 'monitor_ms'])
            assert abs(row['receive_offset_ms'] - row['event_offset_ms'] - row['event_to_receive_ms']) <= .01
            assert abs(row['receive_offset_ms'] + sum(row[key] for key in before_ready) - row['ready_offset_ms']) <= .02
            assert abs(row['ready_offset_ms'] + row['settle_ms'] + row['monitor_ms'] - row['done_offset_ms']) <= .02
        tail = max(details, key=lambda row: row['ready_offset_ms'])
        assert abs(tail['ready_offset_ms'] - summary['ready_offset_ms_max']) <= .01
        results[market] = {'summary': summary, 'settled': settled, 'last_ready_sample': tail, 'client_summaries': clients,
                           'highest_worker_time_samples': sorted(details, key=lambda row: row['processing_ms'], reverse=True)[:5]}
    ticks = source['clock_ticks_per_second']
    cpu = []
    for before, after in zip(source['cpu_samples'], source['cpu_samples'][1:]):
        seconds = (after['monotonic_ns'] - before['monotonic_ns']) / 1e9
        changes = [b - a for a, b in zip(before['cpu'], after['cpu'])]
        total = sum(changes[:8])  # guest counters already included in user/nice
        proc_before, proc_after = before['process'], after['process']
        used = proc_after['utime'] + proc_after['stime'] - proc_before['utime'] - proc_before['stime']
        cpu.append({'offset_seconds': (after['wall_ms'] - source['boundary_ms']) / 1000,
                    'host_busy_percent': 100 * (total - changes[3] - changes[4]) / total,
                    'host_steal_percent': 100 * changes[7] / total,
                    'jvm_cpu_cores': used / ticks / seconds})
    thread_cpu = []
    for tid, after in source['threads_after'].items():
        before = source['threads_before'].get(tid)
        if before:
            used = after['utime'] + after['stime'] - before['utime'] - before['stime']
            thread_cpu.append({'tid': tid, 'comm': after['comm'], 'cpu_seconds': used / ticks})
    return {'markets': results, 'cpu_intervals': cpu,
            'thread_cpu_seconds_over_observation_window': sorted(thread_cpu, key=lambda row: row['cpu_seconds'], reverse=True)[:30],
            'scope': 'One post-deployment hour; phase values are per-message elapsed times, not CPU time. E-to-receive is not pure network latency.'}

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    source = json.loads(args.source.read_text())
    result = analyze(source)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
