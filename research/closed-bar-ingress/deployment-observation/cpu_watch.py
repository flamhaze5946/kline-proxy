"""Bounded /proc CPU and I/O sampler, including observer overhead and per-thread ticks."""
import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path

p=argparse.ArgumentParser();p.add_argument('--pid',required=True);p.add_argument('--output',required=True)
args=p.parse_args();root=Path('/proc')/args.pid;out=Path(args.output)
deadline=dt.datetime(2026,9,13,18,2,tzinfo=dt.timezone.utc).timestamp()

def stat(path):
    raw=path.read_text();end=raw.rfind(')');f=raw[end+2:].split()
    return dict(name=raw[raw.find('(')+1:end],user=int(f[11]),system=int(f[12]),threads=int(f[17]))

def io(path):
    return {a:int(b) for line in path.read_text().splitlines() for a,b in [line.split(':',1)]}

meta=dict(pid=args.pid,observer_pid=os.getpid(),ticks_per_second=os.sysconf('SC_CLK_TCK'),cpu_count=os.cpu_count(),
          start_utc=dt.datetime.now(dt.timezone.utc).isoformat(),end_utc='2026-09-13T18:02:00Z',period_seconds=1)
out.with_suffix('.meta.json').write_text(json.dumps(meta,indent=2)+'\n')
next_sample=time.monotonic()
with out.open('w',buffering=1) as stream:
    while time.time()<deadline:
        if not root.exists():
            print(json.dumps(dict(error='observed JVM exited',pid=args.pid)),flush=True);break
        begin=time.monotonic_ns()
        threads={}
        for path in (root/'task').glob('*/stat'):
            try:threads[path.parent.name]=stat(path)
            except FileNotFoundError:pass
        row=dict(wall_ms=time.time_ns()//1_000_000,monotonic_ns=time.monotonic_ns(),
                 host_cpu=[int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:]],
                 process=stat(root/'stat'),threads=threads,io=io(root/'io'),
                 observer=stat(Path('/proc/self/stat')),
                 collection_ms=(time.monotonic_ns()-begin)/1e6)
        stream.write(json.dumps(row,separators=(',',':'))+'\n')
        next_sample+=1
        time.sleep(max(.001,next_sample-time.monotonic()))
print(json.dumps(dict(status='complete',output=str(out))),flush=True)
