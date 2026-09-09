#!/bin/bash
# kline-proxy 1.8.0 换装:Undertow → Tomcat + Spring Boot 3.2.12,虚拟线程**关**。
# 分两步的第一步 —— 这一步只验容器与框架,线程模型不动。
# 窗口 :05–:50(避开整点的 fleet 突发);失败即回装 1.7.16 + 原 yaml + 原 unit。
set -uo pipefail
H=kline-proxy; TAG=$(date -u +%Y%m%d_%H%M)
JAR_NEW=kline-proxy-1.8.0.jar; JAR_OLD=kline-proxy-1.7.16.jar
LOCAL_JAR="$HOME/Workspace/Java/Personal/kline-proxy/target/$JAR_NEW"

m=$((10#$(date -u +%M)))
{ [ $m -ge 5 ] && [ $m -le 50 ]; } || { echo "ABORT: 分钟 $m 不在 :05-:50 窗口"; exit 2; }
[ -s "$LOCAL_JAR" ] || { echo "ABORT: 本地 jar 不存在 $LOCAL_JAR"; exit 2; }

echo "=== [1] 上传 jar — $(date -u +%T)Z"
scp -q "$LOCAL_JAR" $H:/opt/kline-proxy/ || { echo "ABORT: scp 失败"; exit 1; }

ssh $H "bash -s" <<EOSSH
set -uo pipefail
cd /opt/kline-proxy
TAG=$TAG; JAR_NEW=$JAR_NEW; JAR_OLD=$JAR_OLD

echo "=== [2] 备份 yaml 与 unit"
cp -a application.yaml application.yaml.rollback_\$TAG || exit 1
cp -a /etc/systemd/system/kline-proxy.service /etc/systemd/system/kline-proxy.service.rollback_\$TAG || exit 1
echo "  application.yaml.rollback_\$TAG / kline-proxy.service.rollback_\$TAG"

echo "=== [3] 改 yaml 的容器线程键(只动这两行,其余原样)"
grep -q "server.undertow.threads.worker" application.yaml || { echo "ABORT: 找不到 undertow 键"; exit 1; }
python3 - <<'EOPY'
from pathlib import Path
p = Path("/opt/kline-proxy/application.yaml"); s = p.read_text()
old_w = "server.undertow.threads.worker: 400"
old_i = "server.undertow.threads.io: 4"
assert old_w in s and old_i in s
new = ("# 1.8.0: container is TOMCAT (undertow starter removed). The swap exists only so\n"
       "# spring.threads.virtual.enabled can be used - Spring Boot's virtual-thread support\n"
       "# covers Tomcat and Jetty, not Undertow (measured 2026-09-09 with a real-server probe).\n"
       "# Keep at/above the requests that block together at the boundary: 289 fleet requests wait\n"
       "# ~2.4s each for the closed bar, so Tomcat's default 200 would queue a third of the fleet.\n"
       "server.tomcat.threads.max: 400\n"
       "# Off for this release: container swap and threading model are separate risks.\n"
       "spring.threads.virtual.enabled: false")
s = s.replace(old_w + "\n" + old_i, new, 1)
assert "server.undertow" not in s, "undertow key still present"
assert "server.tomcat.threads.max: 400" in s and "spring.threads.virtual.enabled: false" in s
p.write_text(s)
print("  yaml ok")
EOPY
[ \$? -eq 0 ] || { echo "ABORT: yaml 改写失败"; exit 1; }

echo "=== [4] 改 systemd ExecStart 指向新 jar"
sed -i "s|\$JAR_OLD|\$JAR_NEW|" /etc/systemd/system/kline-proxy.service
grep -q "\$JAR_NEW" /etc/systemd/system/kline-proxy.service || { echo "ABORT: ExecStart 未改到"; exit 1; }
systemctl daemon-reload

echo "=== [5] 重启 — \$(date -u +%T)Z"
systemctl restart kline-proxy
for i in \$(seq 1 40); do
  sleep 2
  code=\$(curl -s -o /dev/null -w "%{http_code}" -m 5 "http://localhost:1888/fapi/v1/klines/bulk?symbols=BTCUSDT&interval=1h&limit=2" 2>/dev/null)
  [ "\$code" = "200" ] && { echo "  健康 200 于第 \$((i*2)) 秒"; break; }
done
[ "\$code" = "200" ] || {
  echo "ABORT: \$((40*2))s 内未健康(最后 \$code) — 回滚"
  cp -a application.yaml.rollback_\$TAG application.yaml
  cp -a /etc/systemd/system/kline-proxy.service.rollback_\$TAG /etc/systemd/system/kline-proxy.service
  systemctl daemon-reload; systemctl restart kline-proxy
  exit 1
}

echo "=== [6] 核验"
echo -n "  容器线程名: "; grep -m1 -oE "\[[a-zA-Z0-9-]+ ?[a-zA-Z0-9-]*\]" kline-proxy.log | tail -1
tail -400 kline-proxy.log | grep -oE "http-nio-[0-9]+-exec-[0-9]+|XNIO-[0-9]+ task-[0-9]+" | sort -u | head -3
echo -n "  外部 HTTPS: "; curl -s -o /dev/null -w "%{http_code} rt=%{time_total}s\n" -m 10 "https://market.feng.dog/fapi/v1/klines/bulk?symbols=BTCUSDT&interval=1h&limit=2"
echo "  jar: \$(systemctl show kline-proxy -p ExecStart --value | grep -oE 'kline-proxy-[0-9.]+\.jar')"
echo "PROXY_DEPLOY_DONE tag=\$TAG \$(date -u +%T)Z"
EOSSH
