#!/usr/bin/env bash
# 一条命令启动完整运行环境：
#   Webhook 投递中枢 (hub)  +  模拟外部合作方接收方 (sink)
#
# 用法：
#   ./run.sh            启动 hub:8080 + sink:9000，Ctrl-C 一起退出
#   ./run.sh e2e        启动两个服务并在就绪后自动运行端到端验收，然后退出
#   HUB_PORT=8080 SINK_PORT=9000 DATA_DIR=./data ./run.sh
set -euo pipefail

cd "$(dirname "$0")"

PYTHON="${PYTHON:-python3}"
DATA_DIR="${DATA_DIR:-${TMPDIR:-/tmp}/whub}"
HUB_PORT="${HUB_PORT:-8080}"
SINK_PORT="${SINK_PORT:-9000}"
MODE="${1:-up}"

mkdir -p "$DATA_DIR/log"
export WHUB_PORT="$HUB_PORT"
export WHUB_DB="$DATA_DIR/data.db"
export WHUB_HTTP_TIMEOUT="${WHUB_HTTP_TIMEOUT:-5}"

hub_pid=""; sink_pid=""
cleanup() {
  [ -n "$hub_pid" ]  && kill "$hub_pid"  2>/dev/null || true
  [ -n "$sink_pid" ] && kill "$sink_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "▶ starting sink on :$SINK_PORT …"
"$PYTHON" -m whub sink --port "$SINK_PORT" >"$DATA_DIR/log/sink.log" 2>&1 &
sink_pid=$!

echo "▶ starting webhook-hub on :$HUB_PORT (db=$WHUB_DB) …"
"$PYTHON" -m whub hub >"$DATA_DIR/log/hub.log" 2>&1 &
hub_pid=$!

# 等待健康
for i in $(seq 1 50); do
  curl -sf "http://127.0.0.1:$HUB_PORT/healthz"  >/dev/null 2>&1 \
  && curl -sf "http://127.0.0.1:$SINK_PORT/healthz" >/dev/null 2>&1 \
  && break
  sleep 0.2
done

cat <<EOF

✅ 运行环境已就绪
   hub  : http://127.0.0.1:$HUB_PORT   (日志 $DATA_DIR/log/hub.log)
   sink : http://127.0.0.1:$SINK_PORT  (日志 $DATA_DIR/log/sink.log)

   演示租户 API Key:
     Acme   : whk_demo_acme_key
     Globex : whk_demo_globex_key

   快速体验：
     curl -s -H 'Authorization: Bearer whk_demo_acme_key' \\
       -H 'Content-Type: application/json' \\
       -d '{"url":"http://127.0.0.1:'$SINK_PORT'/hook","parallelism":2}' \\
       http://127.0.0.1:$HUB_PORT/v1/endpoints

EOF

if [ "$MODE" = "e2e" ]; then
  echo "▶ 运行端到端验收 …"
  "$PYTHON" -m whub e2e \
    --hub "http://127.0.0.1:$HUB_PORT" \
    --sink "http://127.0.0.1:$SINK_PORT"
  exit $?
fi

echo "（Ctrl-C 停止）"
wait
