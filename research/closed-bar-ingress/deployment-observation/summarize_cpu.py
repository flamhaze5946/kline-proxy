"""Recompute CPU/I/O intervals from the bounded /proc observer (no third-party deps)."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path


def category(name):
    if 'Compiler' in name:
        return 'jit'
    if name.startswith(('GC Thread', 'G1 ')):
        return 'gc'
    if name.startswith('websocket-messa'):
        return 'kline_workers'
    if name.startswith(('BinanceFutureWe', 'BinanceSpotWebS')):
        return 'websocket_receive'
    if name.startswith('http-nio'):
        return 'http'
    if name.startswith('symbols-sync'):
        return 'scheduled_tasks'
    if name.startswith(('kline-fetch', 'kline-manage', 'OkHttp')):
        return 'rest_and_management'
    if name.startswith(('JFR', 'Attach Listener')):
        return 'profiler'
    return 'other'


def summarize(raw, ticks, cores):
    result = []
    for before, after in zip(raw, raw[1:]):
        seconds = (after['monotonic_ns'] - before['monotonic_ns']) / 1e9
        assert seconds > 0
        proc = sum(after['process'][k] - before['process'][k] for k in ('user', 'system')) / ticks
        observer = sum(after['observer'][k] - before['observer'][k] for k in ('user', 'system')) / ticks
        host = [b - a for a, b in zip(before['host_cpu'], after['host_cpu'])]
        total = sum(host[:8])
        by_name, by_category = Counter(), Counter()
        new_threads = 0
        for tid, current in after['threads'].items():
            previous = before['threads'].get(tid)
            if previous is None:
                new_threads += 1
                continue
            cpu = sum(current[k] - previous[k] for k in ('user', 'system')) / ticks
            assert cpu >= 0
            if cpu:
                by_name[current['name']] += cpu
                by_category[category(current['name'])] += cpu
        result.append(dict(
            utc=datetime.fromtimestamp(after['wall_ms'] / 1000, timezone.utc).isoformat(),
            wall_ms=after['wall_ms'], interval_start_ms=before['wall_ms'], seconds=seconds,
            jvm_cpu_seconds=proc, jvm_cpu_percent=proc / seconds / cores * 100,
            observer_cpu_seconds=observer, observer_cpu_percent=observer / seconds / cores * 100,
            host_busy_percent=(total - host[3] - host[4]) / total * 100,
            host_iowait_percent=host[4] / total * 100, host_steal_percent=host[7] / total * 100,
            jvm_written_MiB=(after['io']['write_bytes'] - before['io']['write_bytes']) / 2**20,
            jvm_read_MiB=(after['io']['read_bytes'] - before['io']['read_bytes']) / 2**20,
            thread_cpu_seconds=dict(by_name), category_cpu_seconds=dict(by_category),
            process_minus_tracked_threads_cpu_seconds=proc - sum(by_name.values()),
            newly_seen_threads=new_threads, collection_ms=after['collection_ms']))
    return result


def thread_window(source):
    """Shares use ALL matched threads in this window, not the narrower process sample window."""
    categories = Counter()
    threads = []
    for tid, after in source['threads_after'].items():
        before = source['threads_before'].get(tid)
        if before is None:
            continue
        used = sum(after[k] - before[k] for k in ('utime', 'stime')) / source['clock_ticks_per_second']
        assert used >= 0
        categories[category(after['comm'])] += used
        if used:
            threads.append(dict(tid=tid, name=after['comm'], cpu_seconds=used))
    total = sum(categories.values())
    return dict(
        window='approximately T-5 through T+12 seconds; thread snapshots are not atomic',
        tracked_thread_cpu_seconds=total,
        categories={k: dict(cpu_seconds=v, percent_of_tracked_thread_cpu=v / total * 100)
                    for k, v in categories.most_common()},
        threads=sorted(threads, key=lambda r: -r['cpu_seconds']),
        excluded_threads_new=len(source['threads_after'].keys() - source['threads_before'].keys()),
        excluded_threads_exited=len(source['threads_before'].keys() - source['threads_after'].keys()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('metadata', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    rows = [json.loads(line) for line in args.source.open()]
    meta = json.loads(args.metadata.read_text())
    summary = summarize(rows, meta['ticks_per_second'], meta['cpu_count'])
    args.output.write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(dict(intervals=len(summary), seconds=sum(r['seconds'] for r in summary),
                         observer_cpu_seconds=sum(r['observer_cpu_seconds'] for r in summary),
                         jvm_cpu_seconds=sum(r['jvm_cpu_seconds'] for r in summary))))
