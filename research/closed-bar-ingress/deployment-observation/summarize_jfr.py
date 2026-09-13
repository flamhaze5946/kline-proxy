"""Summarize whitelisted JFR events; Java samples are NOT whole-JVM CPU percentages."""
import argparse
from collections import Counter
import json
from pathlib import Path


def summarize(events, begin, end):
    counts, leaves, inclusive, threads, categories, allocated = (Counter() for _ in range(6))
    writes, cpu, pauses, locks = [], [], [], []
    persistence_samples = persistence_formatting = persistence_allocations = 0
    for row in events:
        if not begin <= row['at_ms'] < end:
            continue
        kind = row['type']
        counts[kind] += 1
        stack = row.get('stack', [])
        persistence = any('.dumpPersistedKline' in name for name in stack)
        if kind == 'jdk.ExecutionSample':
            thread = row.get('thread') or 'unknown'
            threads[thread] += 1
            leaves[stack[0] if stack else '<no stack>'] += 1
            inclusive.update(set(name for name in stack if name.startswith('com.zx.quant.')))
            if persistence:
                category = 'persistence'
                persistence_samples += 1
                persistence_formatting += any('ConvertUtil.doubleToString' in name for name in stack)
            elif thread.startswith('websocket-message-handler'):
                category = 'kline_workers'
            elif thread.startswith('http-nio'):
                category = 'http'
            elif thread.startswith(('BinanceFutureWebSocket', 'BinanceSpotWebSocket')):
                category = 'websocket_receive'
            else:
                category = 'other'
            categories[category] += 1
        elif kind == 'jdk.ObjectAllocationSample':
            allocated[row.get('object_class', 'unknown')] += row['weight']
            if persistence:
                persistence_allocations += row['weight']
        elif kind == 'jdk.FileWrite' and '/data/kline-cache/' in (row.get('path') or ''):
            writes.append({k: row[k] for k in ('at_ms', 'duration_ms', 'bytes', 'thread', 'path') if k in row})
        elif kind == 'jdk.CPULoad':
            cpu.append({k: row[k] for k in ('at_ms', 'jvm_user', 'jvm_system', 'machine')})
        elif kind == 'jdk.GCPhasePause':
            pauses.append({k: row[k] for k in ('at_ms', 'duration_ms')})
        elif kind == 'jdk.JavaMonitorEnter':
            locks.append(row)
    return dict(
        begin_ms=begin, end_ms=end, event_counts=dict(counts),
        java_execution_samples=dict(total=counts['jdk.ExecutionSample'], categories=dict(categories),
                                    threads=threads.most_common(), top_leaves=leaves.most_common(30),
                                    application_inclusive=inclusive.most_common(30)),
        persistence_samples=persistence_samples, persistence_formatting_samples=persistence_formatting,
        allocation_weight_bytes=sum(allocated.values()), allocation_classes=allocated.most_common(20),
        persistence_allocation_weight_bytes=persistence_allocations,
        cache_write_summary=dict(calls=len(writes), bytes=sum(w['bytes'] for w in writes),
                                 duration_ms_sum=sum(w['duration_ms'] for w in writes)),
        cache_writes=writes, cpu_load=sorted(cpu, key=lambda r: r['at_ms']),
        gc_pauses=sorted(pauses, key=lambda r: r['at_ms']), monitor_entries=locks,
        methodology='ExecutionSample covers sampled Java stacks, not native JIT/GC CPU; allocation weights are estimates. FileWrite durations exclude other filesystem work and device durability.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('events', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--begin-ms', type=int, required=True)
    parser.add_argument('--end-ms', type=int, required=True)
    args = parser.parse_args()
    result = summarize((json.loads(line) for line in args.events.open()), args.begin_ms, args.end_ms)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k in ('event_counts', 'cache_write_summary', 'persistence_samples')}))
