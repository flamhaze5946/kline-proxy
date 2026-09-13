"""Start a bounded JFR well before the hour, then exit; JFR stops itself after three minutes."""
import datetime as dt
import json
import subprocess
import time
from pathlib import Path

out=Path('/opt/kline-proxy/deployments/20260913_ingress_407afae_retry')
target=dt.datetime(2026,9,13,17,58,30,tzinfo=dt.timezone.utc).timestamp()
while time.time()<target:time.sleep(min(20,max(.001,target-time.time())))
pid=subprocess.check_output(['systemctl','show','kline-proxy','-p','MainPID','--value'],text=True).strip()
assert pid=='517468','observed process changed'
args=['/usr/lib/jvm/jdk-21-oracle-x64/bin/jcmd',pid,'JFR.start','name=ingress_hour',
      'settings=profile','duration=3m','maxsize=192m','dumponexit=true',
      'filename='+str(out/'hour-1800-profile.jfr'),'jdk.FileWrite#threshold=0ms',
      'jdk.ThreadCPULoad#period=1s','jdk.CPULoad#period=100ms']
result=subprocess.run(args,check=True,capture_output=True,text=True)
assert 'Started recording' in result.stdout,result.stdout+result.stderr
(out/'jfr-hour-start.json').write_text(json.dumps(dict(utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                                                   args=args,stdout=result.stdout,stderr=result.stderr),indent=2)+'\n')
print(result.stdout,flush=True)
