"""Measure complete HTTPS responses from this Mac, separately from server-side readiness."""
import argparse
import concurrent.futures
import datetime as dt
import http.client
import json
import time
import urllib.parse
from pathlib import Path

p=argparse.ArgumentParser();p.add_argument('--hour',type=int,required=True);p.add_argument('--output',required=True)
args=p.parse_args();out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
boundary=int(dt.datetime(2026,9,13,args.hour,tzinfo=dt.timezone.utc).timestamp())

def wait_until(target):
    while time.time()<target:time.sleep(min(1,max(.0001,target-time.time())))

def probe(label,symbols):
    wait_until(boundary-10)
    conn=http.client.HTTPSConnection('market.feng.dog',timeout=20)
    clocks=[]
    for _ in range(5):
        t0=time.time_ns()/1e6
        conn.request('GET','/fapi/v1/time');r=conn.getresponse();body=json.loads(r.read())
        t1=time.time_ns()/1e6
        clocks.append(dict(rtt_ms=t1-t0,offset_ms=body['serverTime']-(t0+t1)/2))
    best=min(clocks,key=lambda x:x['rtt_ms']);offset=best['offset_ms']
    query=urllib.parse.urlencode(dict(interval='1h',limit=2,closed_only='true',**({'symbols':','.join(symbols)} if symbols else {})))
    wait_until(boundary+.020-offset/1000)
    wall=time.time_ns()/1e6;start=time.monotonic_ns()
    conn.request('GET','/fapi/v1/klines/bulk?'+query)
    r=conn.getresponse();headers=time.monotonic_ns();raw=r.read();done=time.monotonic_ns();body=json.loads(raw)
    result=dict(label=label,measurement_location='local Mac via public HTTPS',boundary_ms=boundary*1000,
                request_wall_offset_ms=wall-boundary*1000,request_corrected_offset_ms=wall+offset-boundary*1000,
                complete_response_corrected_offset_ms=wall+offset-boundary*1000+(done-start)/1e6,
                duration_ms=(done-start)/1e6,ttfb_ms=(headers-start)/1e6,status=r.status,bytes=len(raw),
                clock_samples=clocks,clock_offset_ms=offset,clock_half_rtt_ms=best['rtt_ms']/2,
                finalized=body.get('finalized'),pending=body.get('pending'),waited_ms=body.get('waited_ms'),
                symbols=len(body.get('klines',{})),wrong_last_open_times=[s for s,rows in body.get('klines',{}).items()
                 if not rows or rows[-1][0]!=(boundary-3600)*1000])
    (out/(label+'-body.json')).write_text(json.dumps(body)+'\n')
    (out/(label+'-timing.json')).write_text(json.dumps(result,indent=2)+'\n')
    conn.close();print(json.dumps(result),flush=True)
    return result

with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
    futures=[pool.submit(probe,'public-all',None),
             pool.submit(probe,'public-six',['BTCUSDT','ETHUSDT','BNBUSDT','DOGEUSDT','ZKUSDT','UNITREEUSDT'])]
    results=[f.result() for f in futures]
(out/'public-summary.json').write_text(json.dumps(results,indent=2)+'\n')
