#!/bin/bash
# kline-proxy 换装（通用）：远端预检 → 上传到暂存名 → 加锁复核 → 改 unit → 重启 → 健康检查
# → 请求路径预热 → 核验；重启后不健康即回装旧 unit（旧 jar）并再做健康检查。
# 基于 proxy_deploy_1810.sh（1.8.10 实际上线所用），sha256 双端核对、drop-in 检测、yaml 校验和不变、
# 重启失败回装、进程核对等安全行为保持不变。差异：
#   1. jar 由参数给出。旧 jar 是远端生效 ExecStart 中紧跟 `-jar` 的那个参数（必须唯一、是
#      /opt/kline-proxy/ 下的合法名字），在上传之前读出。jar 名只允许 kline-proxy-[A-Za-z0-9._-]+.jar；
#      新名字不得等于当前 jar，也不得与 /opt/kline-proxy 中任何已有文件同名（发布名不可变，绝不覆盖）。
#      jar 先传到唯一的暂存名，远端核对 sha256 后用 ln 放到正式名（目标已存在即失败）。
#   2. 远端脚本是固定文本（本地不做任何展开）；所有值只作为 `bash -s --` 的位置参数，经 printf %q
#      序列化。ExecStart 改写是旧 jar 全路径的字面替换（不用 sed）。核对生效 ExecStart 和按 NUL 切分的
#      /proc/<pid>/cmdline 中紧跟 `-jar` 的那个元素。
#   3. 远端 flock 部署锁，两次换装不会交错；回滚备份名带 UTC 秒级时间 + PID + 随机后缀，已存在即中止；
#      预检读出的旧 jar 在加锁后必须不变。
#   4. 窗口收紧为 :05–:20（原 :05–:50），且在远端第一处改动之前、重启之前各按远端时钟再查一次（上传可能很慢）。
#      fleet 的 fundingRate/bulk、klines/bulk 只在整点到达；2026-09-30 17:26 换装后，18/19/20/21 点 fleet bulk
#      p50 为 510/291/208/104 ms（基线 33–152 ms），20:00 首 0.6 s 编译线程占 JVM CPU 37%。
#      在 :20 之前换装，离下一个整点至少 40 分钟。
#   5. 健康检查通过后运行 proxy_warmup.py 做请求路径预热：在本机按 fleet 形态发 192 条并发链
#      fundingRate/bulk → klines/bulk（默认 40 轮；先跑单条预检链；总时限 120 s 且不过 :45；
#      任何错误或某轮 p99 > 2000 ms 即停）。它能否让 JIT 在下一个整点前编译好这些路径是待验证的假设，
#      要在下一个自然整点看编译与 fleet 延迟。预热基本只读内存缓存；资金费率窗口里尚未缓存的整点块、
#      元数据缓存未命中会去读 Binance REST。预热失败或中途停止不回滚（服务已健康），只打印警告。
#   6. 保留 1.8.10 的诊断：yaml 是否显式设置 kline.bulk.hostClockBoundary / kline.bulk.preBoundaryWaitMs。
#
# 用法（本机执行，脚本自己 scp/ssh）：
#   bash scripts/proxy_deploy.sh <本地 jar 路径>
#   远端文件名与本地相同；HOST 默认 kline-proxy（~/.ssh/config），演练时可设 HOST=root@<测试机>。
#   EXTERNAL_URL 默认 https://market.feng.dog/...，演练时设为空跳过外部检查。
#
# ---- 回滚 ----
# 远端在改动之前和结束时各打印一次带 tag 的确切回滚命令（PROXY_DEPLOY_DONE tag=<TAG>）。回到旧 jar：
#     cp -a /etc/systemd/system/kline-proxy.service.rollback_<TAG> /etc/systemd/system/kline-proxy.service
#     systemctl daemon-reload && systemctl restart kline-proxy
# 回滚同样是一次重启，也应在 :05–:20 内做，并同样运行预热：
#     python3 /opt/kline-proxy/proxy_warmup.py
# 重启后不健康时脚本已自动回装；新 jar 留在 /opt/kline-proxy 供排查，这个名字不能再次部署（换新名字）。
# 重启之前中止时，本次放置的新 jar 会被删掉，可以原样重试。
set -uo pipefail
ALNUM=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789   # 枚举而非区间，与 locale 无关
NAME_RE="^kline-proxy-[${ALNUM}._-]+\\.jar\$"
H="${HOST:-kline-proxy}"
LOCAL_JAR="${1:?用法: bash scripts/proxy_deploy.sh <本地 jar 路径>}"
EXTERNAL_URL="${EXTERNAL_URL-https://market.feng.dog/fapi/v1/klines/bulk?symbols=BTCUSDT&interval=1h&limit=2}"
HERE="$(cd "$(dirname "$0")" && pwd)"

# 只含可打印 ASCII（0x21–0x7E）：无空白、无控制字符、无非 ASCII 字节
printable_ascii() { local rest; rest=$(printf '%s' "$1" | LC_ALL=C tr -d '!-~'; echo x); [ "$rest" = x ]; }

m=$((10#$(date -u +%M)))
{ [ "$m" -ge 5 ] && [ "$m" -le 20 ]; } || { echo "ABORT: 分钟 $m 不在 :05-:20 窗口"; exit 2; }
[ -s "$LOCAL_JAR" ] || { echo "ABORT: 本地 jar 不存在: $(printf %q "$LOCAL_JAR")"; exit 2; }
[ -s "$HERE/proxy_warmup.py" ] || { echo "ABORT: 缺少 $HERE/proxy_warmup.py"; exit 2; }
JAR_NEW=$(basename -- "$LOCAL_JAR")
[[ $JAR_NEW =~ $NAME_RE ]] || { echo "ABORT: jar 名必须是 kline-proxy-[A-Za-z0-9._-]+.jar: $(printf %q "$JAR_NEW")"; exit 2; }
HOST_RE="^[${ALNUM}_][${ALNUM}._@-]*\$"
[[ $H =~ $HOST_RE ]] || { echo "ABORT: HOST 不合法: $(printf %q "$H")"; exit 2; }
if [ -n "$EXTERNAL_URL" ]; then
  { [[ $EXTERNAL_URL == http://* || $EXTERNAL_URL == https://* ]] && printable_ascii "$EXTERNAL_URL"; } \
    || { echo "ABORT: EXTERNAL_URL 必须是 http(s):// 开头、只含可打印 ASCII 且无空白: $(printf %q "$EXTERNAL_URL")"; exit 2; }
fi
case $LOCAL_JAR in /*) ;; *) LOCAL_JAR=./$LOCAL_JAR ;; esac   # 防止 scp 把 '-' 开头当选项、把 'x:' 当主机
JAR_SHA256=$(shasum -a 256 "$LOCAL_JAR" | cut -d' ' -f1)
[[ $JAR_SHA256 =~ ^[0123456789abcdef]{64}$ ]] || { echo "ABORT: 本地 sha256 计算失败"; exit 2; }
TAG="$(date -u +%Y%m%dT%H%M%SZ)-$$-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
[[ $TAG =~ ^[0123456789]{8}T[0123456789]{6}Z-[0123456789]+-[0123456789abcdef]{8}$ ]] || { echo "ABORT: 生成 tag 失败: $TAG"; exit 2; }
STAGE_JAR=".deploy-$TAG.$JAR_NEW.part"
STAGE_WARM=".deploy-$TAG.proxy_warmup.py.part"
echo "新 jar: $JAR_NEW sha256=$JAR_SHA256 目标: $H tag=$TAG"

# ---------------------------------------------------------------------------------------------
# 远端脚本：固定文本（引号 heredoc，本地不展开）。参数：
#   $1 模式 preflight|deploy|cleanup  $2 新 jar 名  $3 sha256  $4 tag  $5 预检读出的旧 jar  $6 EXTERNAL_URL
# 整个逻辑包在 main 里，最后一行才执行：bash -s 读完整个脚本才开始动作（连接中途断开不会执行半截），
# 且 main 的 stdin 是 /dev/null，子命令不会吃掉 stdin 上的脚本。
IFS= read -r -d '' REMOTE_SCRIPT <<'EOSSH' || true
set -uo pipefail
ALNUM=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789
NAME_RE="^kline-proxy-[${ALNUM}._-]+\\.jar\$"
SHA_RE='^[0123456789abcdef]{64}$'
TAG_RE='^[0123456789]{8}T[0123456789]{6}Z-[0123456789]+-[0123456789abcdef]{8}$'
ROOT=${KP_DEPLOY_TEST_ROOT-}   # 仅供本地模拟测试；生产上为空
APP_DIR=$ROOT/opt/kline-proxy
UNIT=$ROOT/etc/systemd/system/kline-proxy.service
LOCK=$ROOT/run/lock/kline-proxy-deploy.lock
PROC=$ROOT/proc
SVC=kline-proxy
HEALTH_URL="http://localhost:1888/fapi/v1/klines/bulk?symbols=BTCUSDT&interval=1h&limit=2"

die() { echo "ABORT: $*"; exit 1; }

in_window() {
  local m
  m=$(date -u +%M) || return 1
  [[ $m =~ ^[0123456789]{2}$ ]] || return 1
  m=$((10#$m))
  [ "$m" -ge 5 ] && [ "$m" -le 20 ]
}

exec_start() { systemctl show "$SVC" -p ExecStart --value; }

# $1 = `systemctl show -p ExecStart --value` 的输出：必须恰有一个 ExecStart、其中恰有一个 -jar；打印它后面的参数
jar_arg_of_exec_start() {
  local text=$1 rest argv tok prev="" n=0 jar=""
  local -a toks
  rest=${text#*"argv[]="}
  [ "$rest" != "$text" ] || return 1
  case $rest in *"argv[]="*) return 1 ;; esac
  argv=${rest%%" ; "*}
  [ -n "$argv" ] || return 1
  read -r -a toks <<<"$argv"
  [ "${#toks[@]}" -gt 0 ] || return 1
  for tok in "${toks[@]}"; do
    [ "$prev" = "-jar" ] && jar=$tok
    [ "$tok" = "-jar" ] && n=$((n + 1))
    prev=$tok
  done
  [ "$n" -eq 1 ] && [ -n "$jar" ] || return 1
  printf '%s\n' "$jar"
}

# $1 = /proc/<pid>/cmdline（NUL 分隔）：恰有一个 -jar 元素；打印紧跟其后的元素
jar_arg_of_cmdline() {
  local a prev="" n=0 jar=""
  [ -r "$1" ] || return 1
  while IFS= read -r -d '' a || [ -n "$a" ]; do
    [ "$prev" = "-jar" ] && jar=$a
    [ "$a" = "-jar" ] && n=$((n + 1))
    prev=$a
  done <"$1"
  [ "$n" -eq 1 ] && [ -n "$jar" ] || return 1
  printf '%s\n' "$jar"
}

# $1 = 绝对路径；是 $APP_DIR/<合法 jar 名> 时打印名字
app_jar_name() {
  local p=$1 name
  case $p in "$APP_DIR"/*) ;; *) return 1 ;; esac
  name=${p#"$APP_DIR"/}
  [[ $name =~ $NAME_RE ]] || return 1
  printf '%s\n' "$name"
}

# 字面替换：把文件 $1 中所有 $2 换成 $3，写到 stdout。两个操作数都不经过任何模式/转义解释。
replace_literal() {
  local line out
  [ -n "$2" ] || return 1
  while IFS= read -r line || [ -n "$line" ]; do
    out=
    while [[ $line == *"$2"* ]]; do
      out+=${line%%"$2"*}$3
      line=${line#*"$2"}
    done
    printf '%s\n' "$out$line"
  done <"$1"
}

sha256_of() { local s; s=$(sha256sum -- "$1") || return 1; printf '%s\n' "${s%% *}"; }

health() {
  local code="" i
  for i in $(seq 1 40); do
    sleep 2
    code=$(curl -s -o /dev/null -w "%{http_code}" -m 5 "$HEALTH_URL" 2>/dev/null)
    [ "$code" = "200" ] && { echo "  健康 200 于第 $((i * 2)) 秒"; return 0; }
  done
  echo "  80s 内未健康（最后 ${code}）"
  return 1
}

process_jar() {  # 打印主进程 pid 与其 -jar 参数；失败返回 1
  local pid jar
  pid=$(systemctl show "$SVC" -p MainPID --value)
  [[ $pid =~ ^[123456789][0123456789]*$ ]] || { echo "pid=$pid"; return 1; }
  jar=$(jar_arg_of_cmdline "$PROC/$pid/cmdline") || { echo "pid=${pid}（cmdline 中没有唯一的 -jar）"; return 1; }
  printf 'pid=%s jar=%s\n' "$pid" "$jar"
}

print_rollback() {
  echo "  回滚命令（tag=${TAG}，回到 ${JAR_OLD}；同样在 :05-:20 内做，之后运行预热）："
  printf '    cp -a %q %q\n' "$UNIT_BAK" "$UNIT"
  printf '    systemctl daemon-reload && systemctl restart %q\n' "$SVC"
  printf '    python3 %q\n' "$APP_DIR/proxy_warmup.py"
}

# 重启之前中止：还原 unit（已 daemon-reload 过则再 reload），删掉本次放置、从未运行过的新 jar
abort_before_restart() {
  echo "ABORT: $1；还原 unit，服务未重启"
  cp -a "$UNIT_BAK" "$UNIT" || echo "  🔴 还原 unit 失败，需人工处理: cp -a $UNIT_BAK $UNIT"
  [ "$RELOADED" = 1 ] && systemctl daemon-reload
  [ "$LINKED" = 1 ] && rm -f -- "$NEW_PATH" && echo "  已删除本次放置的 $JAR_NEW"
  exit 1
}

main() {
  MODE=${1-}; JAR_NEW=${2-}; JAR_SHA256=${3-}; TAG=${4-}; EXPECT_OLD=${5-}; EXTERNAL_URL=${6-}
  LINKED=0; RELOADED=0
  case $MODE in preflight | deploy | cleanup) ;; *) die "未知模式 $(printf %q "$MODE")" ;; esac
  [[ $JAR_NEW =~ $NAME_RE ]] || die "jar 名不合法: $(printf %q "$JAR_NEW")"
  [[ $JAR_SHA256 =~ $SHA_RE ]] || die "sha256 不合法"
  [[ $TAG =~ $TAG_RE ]] || die "tag 不合法: $(printf %q "$TAG")"
  cd "$APP_DIR" || die "无法进入 $APP_DIR"
  STAGE_JAR=$APP_DIR/.deploy-$TAG.$JAR_NEW.part
  STAGE_WARM=$APP_DIR/.deploy-$TAG.proxy_warmup.py.part
  if [ "$MODE" = cleanup ]; then
    rm -f -- "$STAGE_JAR" "$STAGE_WARM"
    echo "  已清理暂存文件 (tag=$TAG)"
    exit 0
  fi
  [ "$MODE" = deploy ] && trap 'rm -f -- "$STAGE_JAR" "$STAGE_WARM"' EXIT

  local c
  for c in systemctl flock sha256sum md5sum curl python3; do
    command -v "$c" >/dev/null || die "远端缺少 $c"
  done
  exec 9>>"$LOCK" || die "无法打开部署锁 $LOCK"
  flock -n 9 || die "另一个换装正持有 ${LOCK}，稍后再试"

  echo "=== [1b] 远端核对（${MODE}，已加锁）：从生效 ExecStart 读出当前 jar（不满足则不做任何改动）"
  local es active
  es=$(exec_start) || die "systemctl show 失败"
  active=$(jar_arg_of_exec_start "$es") || die "生效 ExecStart 中没有唯一的 -jar 参数: $es"
  JAR_OLD=$(app_jar_name "$active") || die "-jar 参数不是 $APP_DIR/ 下的合法 jar: $active"
  OLD_PATH=$APP_DIR/$JAR_OLD
  NEW_PATH=$APP_DIR/$JAR_NEW
  [ "$JAR_OLD" != "$JAR_NEW" ] || die "当前已是 $JAR_NEW"
  { [ ! -e "$NEW_PATH" ] && [ ! -L "$NEW_PATH" ]; } || die "$NEW_PATH 已存在：发布名不可变，请换一个名字"
  [ -s "$OLD_PATH" ] || die "回滚目标 $JAR_OLD 不在 $APP_DIR"
  grep -qF -- "$OLD_PATH" "$UNIT" || die "unit 文件未指向 ${JAR_OLD}（有 drop-in?）"
  echo "  当前 jar: $JAR_OLD"
  if [ "$MODE" = preflight ]; then
    { [ ! -e "$STAGE_JAR" ] && [ ! -e "$STAGE_WARM" ]; } || die "暂存名已存在 (tag=$TAG)"
    echo "PREFLIGHT_OK active=$JAR_OLD"
    exit 0
  fi

  [[ $EXPECT_OLD =~ $NAME_RE ]] || die "预检旧 jar 名不合法"
  [ "$JAR_OLD" = "$EXPECT_OLD" ] || die "当前 jar $JAR_OLD 与预检时的 $EXPECT_OLD 不同（期间有别的换装?）"
  [ -s "$STAGE_JAR" ] || die "暂存 jar $STAGE_JAR 不在"
  [ "$(sha256_of "$STAGE_JAR")" = "$JAR_SHA256" ] || die "远端暂存 jar sha256 不符"
  [ -s "$STAGE_WARM" ] || die "暂存预热脚本 $STAGE_WARM 不在"
  in_window || die "远端分钟 $(date -u +%M) 不在 :05-:20 窗口（上传后复查），未做任何改动"

  echo "=== [2] 备份 unit 与 yaml（tag=${TAG}，已存在即中止）"
  UNIT_BAK=$UNIT.rollback_$TAG
  local yaml_bak=$APP_DIR/application.yaml.rollback_$TAG
  { [ ! -e "$UNIT_BAK" ] && [ ! -e "$yaml_bak" ]; } || die "备份名已存在 (tag=$TAG)"
  cp -a "$UNIT" "$UNIT_BAK" || die "备份 unit 失败"
  cp -a application.yaml "$yaml_bak" || die "备份 yaml 失败"
  local yaml_before yaml_after
  yaml_before=$(md5sum application.yaml | cut -d' ' -f1)
  print_rollback

  echo "=== [2b] 新 jar 放到正式名（ln：目标已存在即失败，不覆盖）"
  ln -- "$STAGE_JAR" "$NEW_PATH" || die "无法放置 ${NEW_PATH}（已存在?）"
  LINKED=1
  rm -f -- "$STAGE_JAR"
  [ "$(sha256_of "$NEW_PATH")" = "$JAR_SHA256" ] || abort_before_restart "正式名 jar sha256 不符"

  echo "=== [3] 改 ExecStart 指向新 jar（旧 jar 全路径字面替换）"
  replace_literal "$UNIT_BAK" "$OLD_PATH" "$NEW_PATH" >"$UNIT" || abort_before_restart "写 unit 失败"
  local rest
  rest=$(replace_literal "$UNIT" "$NEW_PATH" "")   # 去掉新路径后不应再出现旧 jar 名
  { grep -qF -- "$NEW_PATH" "$UNIT" && ! grep -qF -- "$OLD_PATH" "$UNIT" && [[ $rest != *"$JAR_OLD"* ]]; } \
    || abort_before_restart "ExecStart 未改到"
  systemctl daemon-reload
  RELOADED=1
  es=$(exec_start)
  active=$(jar_arg_of_exec_start "$es")
  { [ "$active" = "$NEW_PATH" ] && [[ $es != *"$OLD_PATH"* ]]; } \
    || abort_before_restart "生效的 ExecStart -jar 参数不是新 jar（${active}）"

  echo "=== [4] 重启 — $(date -u +%T)Z"
  in_window || abort_before_restart "重启前复查：远端分钟 $(date -u +%M) 不在 :05-:20 窗口"
  systemctl restart "$SVC"
  if ! health; then
    echo "ABORT: 新 jar 不健康，回滚到 $JAR_OLD"
    cp -a "$UNIT_BAK" "$UNIT"
    systemctl daemon-reload
    systemctl restart "$SVC"
    if health; then echo "  回滚后已健康（${JAR_OLD}）"; else echo "  🔴 回滚后仍不健康，需人工处理"; fi
    echo "  回滚后进程: $(process_jar)"
    echo "  新 jar $NEW_PATH 留作排查；这个发布名不能再部署"
    exit 1
  fi
  local proc
  proc=$(process_jar) && [ "${proc##* jar=}" = "$NEW_PATH" ] \
    || { echo "  🔴 主进程不是新 jar（${proc}），不预热；需人工判断是否回滚（命令见上）"; exit 1; }
  echo "  ✅ 进程 ${proc%% *} 的 -jar 参数为 $NEW_PATH"

  echo "=== [5] 请求路径预热 — $(date -u +%T)Z（失败不回滚）"
  local warm_log=$APP_DIR/warmup_$TAG.jsonl warm_rc
  if mv -f -- "$STAGE_WARM" "$APP_DIR/proxy_warmup.py"; then
    python3 "$APP_DIR/proxy_warmup.py" >"$warm_log" 2>&1
    warm_rc=$?
    python3 - "$warm_log" "$warm_rc" <<'PY'
import json, sys
summary = None
try:
    for line in open(sys.argv[1], errors="replace"):
        if line.startswith("{"):
            try:
                doc = json.loads(line)
            except ValueError:
                continue
            if isinstance(doc, dict) and doc.get("summary"):
                summary = doc
except OSError:
    pass
ok = sys.argv[2] == "0"
head = "  预热完成" if ok else "  ⚠️ 预热未完成（exit %s；服务已健康，不回滚）" % sys.argv[2]
if summary is None:
    print(head + "：没有 summary 行")
    sys.exit(0)
def wall(row):
    return "%.0f ms" % row["wall_ms"] if isinstance(row, dict) and "wall_ms" in row else "-"
line = "%s：%s/%s 轮 %s 请求 %ss；首轮 %s → 末轮 %s" % (
    head, summary.get("rounds"), summary.get("rounds_planned"), summary.get("requests"),
    summary.get("elapsed_s"), wall(summary.get("first_round")), wall(summary.get("last_round")))
if summary.get("stop_reason"):
    line += "；停止原因 %s: %s" % (summary.get("stop_reason"), summary.get("detail") or "")
print(line)
PY
    [ "$warm_rc" = 0 ] || echo "     日志末行: $(tail -n 1 "$warm_log")"
  else
    echo "  ⚠️ 预热脚本未能放到位，跳过预热（服务已健康，不回滚）"
  fi

  echo "=== [6] 核验"
  yaml_after=$(md5sum application.yaml | cut -d' ' -f1)
  if [ "$yaml_before" = "$yaml_after" ]; then echo "  ✅ yaml 未被改动"; else echo "  🔴 yaml 变了，不该发生"; exit 1; fi
  local bulk_keys='^[[:space:]]*(kline\.bulk\.)?(hostClockBoundary|host-clock-boundary|preBoundaryWaitMs|pre-boundary-wait-ms)[[:space:]]*[:=]'
  if grep -qE "$bulk_keys" application.yaml; then
    echo "  ⚠️ yaml 显式设置了 bulk 边界配置: $(grep -E "$bulk_keys" application.yaml | tr '\n' ' ')"
  else
    echo "  bulk 边界配置未出现在 yaml：hostClockBoundary=true、preBoundaryWaitMs=250（代码默认）"
  fi
  es=$(exec_start)
  [[ $es == *kline.bulk.* ]] && echo "  ⚠️ ExecStart 中有 kline.bulk.* 覆盖: $es"
  if proc=$(process_jar) && [ "${proc##* jar=}" = "$NEW_PATH" ]; then
    echo "  ✅ 进程 ${proc%% *} 运行 $JAR_NEW"
  else
    echo "  🔴 主进程不是新 jar（${proc}）"; exit 1
  fi
  echo "  jar: $(jar_arg_of_exec_start "$es")"
  health || { echo "  🔴 预热后健康检查失败，需人工判断是否回滚（命令见下）"; print_rollback; exit 1; }
  if [ -n "$EXTERNAL_URL" ]; then
    echo -n "  外部 HTTPS: "
    curl -s -o /dev/null -w "%{http_code} rt=%{time_total}s\n" -m 10 "$EXTERNAL_URL"
  fi
  print_rollback
  echo "  ⏳ 下一个整点看 nginx :00 的 fleet rt、编译线程 CPU 与 BULK_FINAL_WAIT / BULK_PRE_BOUNDARY_WAIT（预热收益是待验证的假设）"
  echo "PROXY_DEPLOY_DONE tag=$TAG $(date -u +%T)Z"
}

main "$@" </dev/null
exit $?
EOSSH

# 远端调用：脚本走 stdin；值只作为位置参数（printf %q 序列化，远端 shell 解析回原值）
# shellcheck disable=SC2029  # 有意在本地展开：%q 序列化后的参数正是要交给远端 shell 解析的文本
remote() { ssh "$H" "bash -s -- $(printf '%q ' "$@")" <<<"$REMOTE_SCRIPT"; }

echo "=== [0] 远端预检（加锁读出当前 jar，确认新名字未被占用；不做任何改动）— $(date -u +%T)Z"
PRE_OUT=$(remote preflight "$JAR_NEW" "$JAR_SHA256" "$TAG" "" "$EXTERNAL_URL")
rc=$?
printf '%s\n' "$PRE_OUT"
[ "$rc" -eq 0 ] || { echo "ABORT: 远端预检失败，未上传"; exit 1; }
JAR_OLD=$(printf '%s\n' "$PRE_OUT" | sed -n 's/^PREFLIGHT_OK active=//p' | tail -n 1)
[[ $JAR_OLD =~ $NAME_RE ]] || { echo "ABORT: 预检没有给出当前 jar"; exit 1; }

echo "=== [1] 上传 jar 与预热脚本到暂存名 — $(date -u +%T)Z"
if ! scp -q "$LOCAL_JAR" "$H:/opt/kline-proxy/$STAGE_JAR" || ! scp -q "$HERE/proxy_warmup.py" "$H:/opt/kline-proxy/$STAGE_WARM"; then
  echo "ABORT: scp 失败，清理远端暂存文件"
  remote cleanup "$JAR_NEW" "$JAR_SHA256" "$TAG"
  exit 1
fi

echo "=== [2] 远端换装（加锁；当前 jar 必须仍是 ${JAR_OLD}）— $(date -u +%T)Z"
remote deploy "$JAR_NEW" "$JAR_SHA256" "$TAG" "$JAR_OLD" "$EXTERNAL_URL"
