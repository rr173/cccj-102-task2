#!/usr/bin/env bash
# 一条命令拉起 HA 双 worker 形态并运行自动验收：
#   durable store 进程 + worker-a 进程 + worker-b 进程
#   + 故障注入 receiver(sink) 进程 + 8 场景验收程序
#
# 用法：
#   ./run-ha.sh                 # 起集群 → 跑 8 场景验收 → 退出（全新数据库）
#   ./run-ha.sh up              # 只起集群常驻（不跑验收），便于手工 curl
#   TTL=3 ./run-ha.sh           # 缩短 lease TTL（秒）
#   REPORT=out.json ./run-ha.sh # 指定机器可读报告路径
#
# 所有 worker 都是独立 OS 进程，只通过 store 的 JSON/RPC 访问持久层；
# 仲裁只发生在数据库事务里（epoch + fence_id），无任何线程锁/PID 文件/
# 进程内内存表参与跨进程裁决。
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-python3}"
MODE="${1:-acceptance}"
DATA_DIR="${DATA_DIR:-${TMPDIR:-/tmp}/whub-ha}"
TTL="${TTL:-4}"
REPORT="${REPORT:-acceptance-report.json}"

mkdir -p "$DATA_DIR/log"

# acceptance 模式：验收程序自起全新集群（store + worker-a + worker-b + sink，
# 各自独立 OS 进程、全新数据库），跑完即清理。
if [ "$MODE" = "acceptance" ]; then
  echo "▶ 运行 8 场景自动验收（自起集群；真实 kill -9 / STW / store outage 注入）…"
  "$PYTHON" -m whub acceptance --ttl "$TTL" --report "$REPORT"
  rc=$?
  echo "机器可读报告：$(pwd)/$REPORT"
  exit $rc
fi

# ---- up 模式：常驻一套演示集群 -----------------------------------------
STORE_PORT="${STORE_PORT:-8080}"
WA_PORT="${WA_PORT:-8081}"
WB_PORT="${WB_PORT:-8082}"
SINK_PORT="${SINK_PORT:-9000}"

if [ "${KEEP_DB:-0}" != "1" ]; then
  rm -f "$DATA_DIR"/store.db "$DATA_DIR"/store.db-wal "$DATA_DIR"/store.db-shm
fi

# 短 TTL 下的时间边界（生产默认见 config.py：TTL 30s / renew 10s）
export WHUB_LEASE_TTL="$TTL"
export WHUB_RENEW_INTERVAL="${WHUB_RENEW_INTERVAL:-$(python3 -c "print(max($TTL/3,0.3))")}"
export WHUB_HEARTBEAT_INTERVAL="${WHUB_HEARTBEAT_INTERVAL:-$(python3 -c "print(max($TTL/4,0.3))")}"
export WHUB_SWEEP_INTERVAL="${WHUB_SWEEP_INTERVAL:-0.15}"
export WHUB_ACQUIRE_BUDGET="${WHUB_ACQUIRE_BUDGET:-3}"
export WHUB_REBALANCE_BUDGET="${WHUB_REBALANCE_BUDGET:-2}"
export WHUB_RENEW_JITTER="${WHUB_RENEW_JITTER:-0.05}"
export WHUB_HTTP_TIMEOUT="${WHUB_HTTP_TIMEOUT:-3}"
export WHUB_BACKOFF_BASE="${WHUB_BACKOFF_BASE:-0.2}"
export WHUB_BACKOFF_CAP="${WHUB_BACKOFF_CAP:-8}"

sink_pid=""; store_pid=""; wa_pid=""; wb_pid=""
cleanup() {
  for pid in $wa_pid $wb_pid $store_pid $sink_pid; do
    [ -n "$pid" ] && kill -TERM "-$(ps -o pgid= -p "$pid" | tr -d ' ')" 2>/dev/null || true
  done
  sleep 0.5
  for pid in $wa_pid $wb_pid $store_pid $sink_pid; do
    [ -n "$pid" ] && kill -KILL "-$(ps -o pgid= -p "$pid" | tr -d ' ')" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

echo "▶ durable store      :$STORE_PORT (db=$DATA_DIR/store.db)"
"$PYTHON" -m whub store --host 127.0.0.1 --port "$STORE_PORT" \
  >"$DATA_DIR/log/store.log" 2>&1 &
store_pid=$!

echo "▶ fault-inject sink  :$SINK_PORT"
"$PYTHON" -m whub sink --host 127.0.0.1 --port "$SINK_PORT" \
  >"$DATA_DIR/log/sink.log" 2>&1 &
sink_pid=$!

for i in $(seq 1 50); do
  curl -sf "http://127.0.0.1:$STORE_PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.1
done

echo "▶ worker-a           :$WA_PORT   (separate OS process)"
WHUB_STORE_URL="http://127.0.0.1:$STORE_PORT" WHUB_PORT="$WA_PORT" WHUB_SEED=0 \
  "$PYTHON" -m whub worker --worker-id worker-a --host 127.0.0.1 \
  >"$DATA_DIR/log/worker-a.log" 2>&1 &
wa_pid=$!

echo "▶ worker-b           :$WB_PORT   (separate OS process)"
WHUB_STORE_URL="http://127.0.0.1:$STORE_PORT" WHUB_PORT="$WB_PORT" WHUB_SEED=0 \
  "$PYTHON" -m whub worker --worker-id worker-b --host 127.0.0.1 \
  >"$DATA_DIR/log/worker-b.log" 2>&1 &
wb_pid=$!

for port in "$SINK_PORT" "$WA_PORT" "$WB_PORT"; do
  for i in $(seq 1 50); do
    curl -sf "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 && break
    sleep 0.1
  done
done

cat <<EOF

✅ HA 集群已就绪（4 个独立 OS 进程，共用 durable store）
   store    http://127.0.0.1:$STORE_PORT   日志 $DATA_DIR/log/store.log
   worker-a http://127.0.0.1:$WA_PORT   日志 $DATA_DIR/log/worker-a.log
   worker-b http://127.0.0.1:$WB_PORT   日志 $DATA_DIR/log/worker-b.log
   sink     http://127.0.0.1:$SINK_PORT   日志 $DATA_DIR/log/sink.log
   lease TTL=${TTL}s renew=${WHUB_RENEW_INTERVAL}s budget=${WHUB_ACQUIRE_BUDGET}/${WHUB_REBALANCE_BUDGET}

   运维视图：
     curl -s localhost:$STORE_PORT/admin/workers | python3 -m json.tool
     curl -s localhost:$STORE_PORT/admin/leases  | python3 -m json.tool
     curl -s localhost:$STORE_PORT/metrics
EOF

if [ "$MODE" = "up" ]; then
  echo "（常驻；Ctrl-C 退出并清理全部子进程）"
  wait
  exit 0
fi

echo "用法：./run-ha.sh [acceptance|up]" >&2
exit 2
