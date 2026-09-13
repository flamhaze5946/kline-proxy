"""Read-only production observation: periodic health and one measured hour boundary."""
import concurrent.futures
import argparse
import datetime as dt
import http.client
import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path('/opt/kline-proxy')
parser=argparse.ArgumentParser()
parser.add_argument('--output',default=str(ROOT/'deployments/20260913_ingress_407afae'))
parser.add_argument('--hour',type=int,default=17)
args=parser.parse_args()
OUT = Path(args.output)
BOUNDARY = int(dt.datetime(2026,9,13,args.hour,tzinfo=dt.timezone.utc).timestamp())
STAMP=str(args.hour)+'00'
manifest = json.loads((OUT/'deployment.json').read_text())
assert manifest['status'] in ('verified','baseline')
pid=str(manifest['new_pid'])

def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()

def fetch(path):
    with urllib.request.urlopen('http://127.0.0.1:1888'+path,timeout=12) as r:
        return r.read().decode()

def metrics():
    return '\n'.join(line for line in fetch('/actuator/prometheus').splitlines()
                     if not line.startswith('#') and any(k in line for k in
                     ('websocket_kline_ingress','websocket_client','websocket_message','process_cpu_usage',
                      'system_cpu_usage','process_uptime','jvm_gc_pause','jvm_memory_used_bytes')))

def ticks(path):
    raw=path.read_text();end=raw.rfind(')');f=raw[end+2:].split()
    return dict(comm=raw[raw.find('(')+1:end],utime=int(f[11]),stime=int(f[12]),threads=int(f[17]))

def threads():
    result={}
    for p in Path('/proc/'+pid+'/task').glob('*/stat'):
        try:result[p.parent.name]=ticks(p)
        except FileNotFoundError:pass
    return result

def wait_until(deadline):
    while time.time()<deadline:
        time.sleep(min(1,max(0.0001,deadline-time.time())))

def probe(label,symbols,offset_ms):
    connection=http.client.HTTPConnection('127.0.0.1',1888,timeout=15)
    connection.request('GET','/fapi/v1/time')
    r=connection.getresponse();r.read()
    query=urllib.parse.urlencode(dict(interval='1h',limit=2,closed_only='true',**({'symbols':','.join(symbols)} if symbols else {})))
    wait_until(BOUNDARY+0.020-offset_ms/1000)
    start_ns=time.time_ns();mono=time.monotonic_ns()
    try:
        connection.request('GET','/fapi/v1/klines/bulk?'+query)
        response=connection.getresponse();headers_ns=time.monotonic_ns()
        raw=response.read();done_ns=time.monotonic_ns();body=json.loads(raw)
        result=dict(label=label,http_status=response.status,
                    request_offset_ms=(start_ns/1e6+offset_ms-BOUNDARY*1000),
                    first_headers_offset_ms=(start_ns/1e6+offset_ms-BOUNDARY*1000)+(headers_ns-mono)/1e6,
                    body_received_offset_ms=(start_ns/1e6+offset_ms-BOUNDARY*1000)+(done_ns-mono)/1e6,
                    duration_ms=(done_ns-mono)/1e6,bytes=len(raw),
                    finalized=body.get('finalized'),pending=body.get('pending'),waited_ms=body.get('waited_ms'),
                    not_trading=body.get('not_trading'),symbols=len(body.get('klines',{})),
                    wrong_last_open_times=[s for s,rows in body.get('klines',{}).items()
                                           if not rows or rows[-1][0]!=(BOUNDARY-3600)*1000])
        (OUT/(label+'-'+STAMP+'.json')).write_text(json.dumps(body)+'\n')
    except Exception as error:
        result=dict(label=label,error=str(error))
    finally:connection.close()
    print(json.dumps(dict(stage='http_probe',utc=utc(),**result)),flush=True)
    return result

health=[]
while time.time()<BOUNDARY-60:
    sample=dict(utc=utc(),pid_present=Path('/proc/'+pid).exists(),metrics=metrics())
    health.append(sample)
    (OUT/'health.json').write_text(json.dumps(health,indent=2)+'\n')
    print(json.dumps(dict(stage='health',utc=sample['utc'],pid=pid,
                         ingress=[line for line in sample['metrics'].splitlines() if 'websocket_kline_ingress' in line])),flush=True)
    wait_until(min(BOUNDARY-60,time.time()+20))

clock=[]
for _ in range(5):
    start=time.time_ns()/1e6
    body=json.loads(fetch('/fapi/v1/time'))
    end=time.time_ns()/1e6
    clock.append(dict(rtt_ms=end-start,offset_ms=body['serverTime']-(start+end)/2))
    time.sleep(.1)
offset=min(clock,key=lambda x:x['rtt_ms'])['offset_ms']
pool=concurrent.futures.ThreadPoolExecutor(max_workers=2)
probes=[pool.submit(probe,'loopback-all',None,offset),
        pool.submit(probe,'loopback-six',['BTCUSDT','ETHUSDT','BNBUSDT','DOGEUSDT','ZKUSDT','UNITREEUSDT'],offset)]
wait_until(BOUNDARY-5)
assert Path('/proc/'+pid).exists(),'JVM changed before boundary'
before_metrics=metrics();before_threads=threads();samples=[]
while time.time()<BOUNDARY+12:
    samples.append(dict(wall_ms=time.time_ns()//1_000_000,monotonic_ns=time.monotonic_ns(),
                        cpu=[int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:]],
                        process=ticks(Path('/proc/'+pid+'/stat')),loadavg=Path('/proc/loadavg').read_text().strip()))
    time.sleep(1)
after_threads=threads();after_metrics=metrics()
probe_results=[future.result(timeout=20) for future in probes]
pool.shutdown()
wait_until(BOUNDARY+42)
with (ROOT/'kline-proxy.log').open('rb') as stream:
    stream.seek(manifest['log_offset']);lines=stream.read().decode(errors='replace').splitlines()
boundary_text='boundary='+str(BOUNDARY*1000)
closed=[line for line in lines if 'CLOSED_BAR_' in line and boundary_text in line]
bulk=[line for line in lines if 'BULK_FINAL_WAIT' in line and boundary_text in line]
errors=[line for line in lines if ' ERROR ' in line or ' WARN ' in line]
gc=[]
for path in ROOT.glob('gc.log*'):
    if path.stat().st_mtime<BOUNDARY-10:continue
    gc.extend(line for line in path.read_text(errors='replace').splitlines()
              if ('2026-09-13T'+str(args.hour)+':00:') in line or ('2026-09-13T'+str(args.hour-1)+':59:5') in line)
result=dict(source_host='kline-proxy',source_log=str(ROOT/'kline-proxy.log'),captured_utc=utc(),
            deployment=manifest,boundary_ms=BOUNDARY*1000,clock_ticks_per_second=os.sysconf('SC_CLK_TCK'),
            cpu_count=os.cpu_count(),cpu_samples=samples,threads_before=before_threads,threads_after=after_threads,
            metrics_before=before_metrics,metrics_after=after_metrics,clock_samples=clock,
            probe_clock_offset_ms=offset,http_probes=probe_results,closed_bar_logs=closed,bulk_logs=bulk,
            warning_error_logs=errors,gc_logs=gc,
            systemd=subprocess.run(['systemctl','show','kline-proxy','-p','MainPID','-p','NRestarts','-p','ActiveState'],
                                   check=True,capture_output=True,text=True).stdout)
(OUT/('observation-'+STAMP+'.json')).write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(dict(stage='observed',utc=utc(),output=str(OUT/('observation-'+STAMP+'.json')),
                     closed_lines=len(closed),bulk_lines=len(bulk),warning_error_lines=len(errors))),flush=True)
assert any('CLOSED_BAR_LATENCY service=binanceFuture ' in line for line in closed),'missing futures latency'
