"""Aggregate the existing nginx timing log; omit client addresses and raw URLs."""
import argparse
import collections
import datetime as dt
import json
import re
import statistics
import urllib.parse
from pathlib import Path

parser=argparse.ArgumentParser();parser.add_argument('--hour',type=int,required=True);parser.add_argument('--output',required=True)
args=parser.parse_args();path=Path('/var/log/nginx/access.log');size=path.stat().st_size
with path.open('rb') as f:
    f.seek(max(0,size-32*1024*1024));lines=f.read().decode(errors='replace').splitlines()
pattern=re.compile(r'\[13/Sep/2026:'+str(args.hour)+r':00:(\d+) ([+-]\d+)\] "(GET|POST) ([^ ]+) HTTP/[^\"]+" (\d+) (\d+).* rt=([\d.]+) uct=(\S+) uht=(\S+) urt=(\S+)')
rows=[]
for line in lines:
    m=pattern.search(line)
    if not m:continue
    url=urllib.parse.urlsplit(m[4])
    if url.path!='/fapi/v1/klines/bulk':continue
    query=urllib.parse.parse_qs(url.query)
    def number(s):
        try:return float(s)
        except ValueError:return None
    rows.append(dict(completed_second=int(m[1]),timezone=m[2],method=m[3],status=int(m[5]),bytes=int(m[6]),
                     request_seconds=float(m[7]),upstream_connect_seconds=number(m[8]),
                     upstream_header_seconds=number(m[9]),upstream_response_seconds=number(m[10]),
                     interval=query.get('interval',['unknown'])[0],limit=query.get('limit',['unknown'])[0]))
def stats(values):
    values=sorted(v for v in values if v is not None)
    if not values:return None
    return dict(n=len(values),min=values[0],median=statistics.median(values),p90=values[int((len(values)-1)*.9)],max=values[-1])
result=dict(hour_utc=args.hour,date='2026-09-13',source=str(path),source_bytes=size,
            scanned_tail_bytes=min(size,32*1024*1024),scope='All bulk kline requests completed during this minute, including observer probes; timestamps have one-second resolution.',
            count=len(rows),status_counts=dict(collections.Counter(str(r['status']) for r in rows)),
            completed_second_counts=dict(sorted(collections.Counter(r['completed_second'] for r in rows).items())),
            request_seconds=stats([r['request_seconds'] for r in rows]),
            upstream_header_seconds=stats([r['upstream_header_seconds'] for r in rows]),rows=rows)
Path(args.output).write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k!='rows'}))
