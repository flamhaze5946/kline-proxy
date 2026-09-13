from pathlib import Path
from collections import Counter
from datetime import datetime
import json, re, subprocess, sys, os, shutil

ROOT=Path(sys.argv[1]).resolve()
JFR=str(Path(os.environ['JAVA_HOME'])/'bin/jfr') if os.environ.get('JAVA_HOME') else shutil.which('jfr')
if not JFR:
    raise SystemExit('Set JAVA_HOME to a JDK 21 installation before running this script')
def duration(text):
    m=re.fullmatch(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?',text)
    return float(m[1] or 0)*3600+float(m[2] or 0)*60+float(m[3] or 0) if m else 0
def ts(text): return datetime.fromisoformat(text).timestamp()
def frames(value):
    return [f['method']['type']['name'].replace('/','.')+'.'+f['method']['name'] for f in (value.get('stackTrace') or {}).get('frames',[])]

output={}
for name in ['baseline-warm','cached-warm','mailbox-warm','baseline-bulk96-gated','cached-bulk96-gated','poll-bulk96-gated','mailbox-flood']:
    raw=ROOT/f'jfr/{name}-events.json'
    with raw.open('w') as f:
        subprocess.run([JFR,'print','--json','--stack-depth','128','--events','jdk.ExecutionSample,jdk.ObjectAllocationSample,jdk.JavaMonitorEnter,jdk.ThreadPark,proxy.ReplayRound',str(ROOT/f'jfr/{name}.jfr')],stdout=f,check=True)
    events=json.loads(raw.read_text())['recording']['events']
    windows=[(ts(v['startTime']),ts(v['startTime'])+duration(v['duration'])) for e in events if e['type']=='proxy.ReplayRound' and not (v:=e['values'])['warmup']]
    def measured(v):return any(start<=ts(v['startTime'])<=end for start,end in windows)
    cpu=Counter();leaf=Counter();alloc=Counter();alloc_class=Counter();locks=Counter();parks=Counter();categories=Counter();threads=Counter();samples=0;worker_samples=0;worker_alloc=0
    for e in events:
        v=e['values']
        if not measured(v):continue
        stack=frames(v);thread=(v.get('sampledThread') or v.get('eventThread') or {}).get('javaName','')
        worker=thread.startswith(('websocket-message-handler','mailbox-'))
        if e['type']=='jdk.ExecutionSample':
            samples+=1;threads[thread]+=1
            if worker:
                worker_samples+=1
                leaf[stack[0] if stack else '?']+=1
                for method in set(stack):cpu[method]+=1
                if any('recordClosedBarArrival' in x for x in stack):categories['settle']+=1
                elif any('awaitJustClosedBarsFinal' in x for x in stack):categories['bulk_wait']+=1
                elif any('updateKlinesInternal' in x for x in stack):categories['cache']+=1
                elif any('Serializer.readTree' in x or 'parseMessage' in x for x in stack):categories['parse_json']+=1
                elif any('Serializer.fromJson' in x or 'Serializer.treeToValue' in x or 'decodeKline' in x for x in stack):categories['decode']+=1
                elif any('ClosedBarLatencyRecorder' in x for x in stack):categories['latency_recorder']+=1
                else:categories['other']+=1
        elif e['type']=='jdk.ObjectAllocationSample' and worker:
            weight=v['weight'];worker_alloc+=weight;alloc_class[v['objectClass']['name']]+=weight
            for method in set(stack):alloc[method]+=weight
        elif e['type']=='jdk.JavaMonitorEnter':
            app=next((x for x in stack if x.startswith('com.zx.') and 'ReplayHarness' not in x),stack[0] if stack else '?')
            locks[(v['monitorClass']['name'],app)]+=duration(v['duration'])*1000
        elif e['type']=='jdk.ThreadPark':
            app=next((x for x in stack if x.startswith('com.zx.') and 'ReplayHarness' not in x),stack[-1] if stack else '?')
            parks[(thread.split('-')[0],app)]+=duration(v['duration'])*1000
    data={'all_execution_samples':samples,'worker_samples':worker_samples,'worker_exclusive_categories':dict(categories),'worker_top_leaf':leaf.most_common(12),
          'worker_inclusive_application':[(k,v) for k,v in cpu.most_common() if k.startswith('com.zx.') and 'ReplayHarness' not in k][:25],
          'worker_sampled_alloc_weight_bytes':worker_alloc,'worker_allocation_classes':alloc_class.most_common(12),
          'worker_inclusive_alloc_application':[(k,v) for k,v in alloc.most_common() if k.startswith('com.zx.') and 'ReplayHarness' not in k][:20],
          'monitor_wait_ms':[(list(k),v) for k,v in locks.most_common(10)],'park_ms':[(list(k),v) for k,v in parks.most_common(10)]}
    output[name]=data
    print(name,'worker_samples',worker_samples,'categories',dict(categories),'allocMiB',round(worker_alloc/2**20,1),flush=True)
(ROOT/'results/jfr-analysis.json').write_text(json.dumps(output,indent=2))
