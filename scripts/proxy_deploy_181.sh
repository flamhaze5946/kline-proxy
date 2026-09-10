#!/bin/bash
# kline-proxy 1.8.1 换装:纯日志改动 —— CLOSED_BAR_SETTLED 追加 ev_* 五项
# (Binance 自己打的 E 减整点),用来把「币安发得晚」与「我们收得晚」分开。
# yaml 一个字不动(1.8.0 的容器/线程配置原样保留)。
# 窗口 :05–:50(避开整点的 fleet 突发);失败即回装 1.8.0 + 原 unit。
set -uo pipefail
H=kline-proxy; TAG=$(date -u +%Y%m%d_%H%M)
JAR_NEW=kline-proxy-1.8.1.jar; JAR_OLD=kline-proxy-1.8.0.jar
LOCAL_JAR="$HOME/Workspace/Java/Personal/kline-proxy/target/$JAR_NEW"

m=$((10#$(date -u +%M)))
{ [ $m -ge 5 ] && [ $m -le 50 ]; } || { echo "ABORT: 分钟 $m 不在 :05-:50 窗口"; exit 2; }
[ -s "$LOCAL_JAR" ] || { echo "ABORT: 本地 jar 不存在"; exit 2; }

echo "=== [1] 上传 jar — $(date -u +%T)Z"
scp -q "$LOCAL_JAR" $H:/opt/kline-proxy/ || { echo "ABORT: scp 失败"; exit 1; }

ssh $H "bash -s" <<EOSSH
set -uo pipefail
cd /opt/kline-proxy
TAG=$TAG; JAR_NEW=$JAR_NEW; JAR_OLD=$JAR_OLD

echo "=== [2] 备份 unit(yaml 本次不改,不需备份但一并留一份)"
cp -a /etc/systemd/system/kline-proxy.service /etc/systemd/system/kline-proxy.service.rollback_\$TAG || exit 1
cp -a application.yaml application.yaml.rollback_\$TAG || exit 1
YAML_BEFORE=\$(md5sum application.yaml | cut -d' ' -f1)

echo "=== [3] 改 ExecStart 指向新 jar(通配版本号,防写死旧版本)"
sed -i -E "s|kline-proxy-[0-9]+\.[0-9]+\.[0-9]+\.jar|\$JAR_NEW|g" /etc/systemd/system/kline-proxy.service
grep -q "\$JAR_NEW" /etc/systemd/system/kline-proxy.service || { echo "ABORT: ExecStart 未改到"; exit 1; }
systemctl daemon-reload

echo "=== [4] 重启 — \$(date -u +%T)Z"
systemctl restart kline-proxy
code=""
for i in \$(seq 1 40); do
  sleep 2
  code=\$(curl -s -o /dev/null -w "%{http_code}" -m 5 "http://localhost:1888/fapi/v1/klines/bulk?symbols=BTCUSDT&interval=1h&limit=2" 2>/dev/null)
  [ "\$code" = "200" ] && { echo "  健康 200 于第 \$((i*2)) 秒"; break; }
done
[ "\$code" = "200" ] || {
  echo "ABORT: 80s 内未健康(最后 \$code) — 回滚到 \$JAR_OLD"
  cp -a /etc/systemd/system/kline-proxy.service.rollback_\$TAG /etc/systemd/system/kline-proxy.service
  systemctl daemon-reload; systemctl restart kline-proxy
  exit 1
}

echo "=== [5] 核验"
YAML_AFTER=\$(md5sum application.yaml | cut -d' ' -f1)
[ "\$YAML_BEFORE" = "\$YAML_AFTER" ] && echo "  ✅ yaml 未被改动" || { echo "  🔴 yaml 变了,不该发生"; exit 1; }
echo "  jar: \$(systemctl show kline-proxy -p ExecStart --value | grep -oE 'kline-proxy-[0-9.]+\.jar')"
echo -n "  外部 HTTPS: "; curl -s -o /dev/null -w "%{http_code} rt=%{time_total}s\n" -m 10 "https://market.feng.dog/fapi/v1/klines/bulk?symbols=BTCUSDT&interval=1h&limit=2"
echo "  ⏳ ev_* 字段要等下一个整点的 CLOSED_BAR_SETTLED 才出现"
echo "PROXY_DEPLOY_DONE tag=\$TAG \$(date -u +%T)Z"
EOSSH
