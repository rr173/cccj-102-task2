#!/usr/bin/env bash
# 单脚本复现 HA 验收：shared store + worker-a + worker-b + 故障注入 receiver
# + 8 个场景的自动验收。每个场景使用全新数据库，退出时清理全部子进程。
#
# 用法：
#   ./run-ha.sh                 # 跑全部 8 个场景（默认 TTL=4s），写 report.json
#   ./run-ha.sh 3               # 只跑场景 3
#   TTL=2 ./run-ha.sh           # 缩短租约（更激进的故障切换）
#   ./run-ha.sh --keep          # 保留各场景的数据库与日志（手工排查）
#   ./run-ha.sh smoke           # 只起常驻环境（store+2 worker+sink），不跑验收
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-python3}"
TTL="${TTL:-4}"
DATA_DIR="${DATA_DIR:-${TMPDIR:-/tmp}/whub-ha}"
mkdir -p "$DATA_DIR"

MODE="${1:-all}"

if [ "$MODE" = "smoke" ]; then
  # 常驻演示：单独 store + worker-a + worker-b + sink（真实 4 个 OS 进程）
  export WHUB_DB="$DATA_DIR/shared.db"
  export WHUB_LEASE_TTL="$TTL"
  export WHUB_RENEW_INTERVAL="$(python3 -c "print($TTL/5)")"
  export WHUB_COORD_INTERVAL=0.3
  export WHUB_REBALANCE_MAX_MOVES=1
  export WHUB_OUTAGE_MARKER="$DATA_DIR/STORE_OUTAGE"
  export WHUB_DEBUG_ADMIN=1
  pids=()
  cleanup() { for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
  trap cleanup EXIT INT TERM
  "$PYTHON" -m whub sink --host 127.0.0.1 --port 19000 \
      >"$DATA_DIR/sink.log" 2>&1 & pids+=($!)
  "$PYTHON" -m whub store --port-file "$DATA_DIR/store.port" --debug-admin \
      >"$DATA_DIR/store.log" 2>&1 & pids+=($!)
  for i in $(seq 1 50); do [ -f "$DATA_DIR/store.port" ] && break; sleep 0.1; done
  SPORT="$(cat "$DATA_DIR/store.port")"
  "$PYTHON" -m whub worker --worker-id worker-a \
      --port-file "$DATA_DIR/a.port" >"$DATA_DIR/worker-a.log" 2>&1 & pids+=($!)
  "$PYTHON" -m whub worker --worker-id worker-b \
      --port-file "$DATA_DIR/b.port" >"$DATA_DIR/worker-b.log" 2>&1 & pids+=($!)
  echo "store : http://127.0.0.1:$SPORT"
  echo "sink  : http://127.0.0.1:19000"
  echo "workers: worker-a worker-b (logs in $DATA_DIR)"
  echo "e.g. curl -s localhost:$SPORT/admin/leases | python3 -m json.tool"
  wait
fi

ARGS=(ha --ttl "$TTL" --data-dir "$DATA_DIR")
if [[ "$MODE" =~ ^[1-8]$ ]]; then
  ARGS+=(--scenario "$MODE")
fi
if [ "${2:-}" = "--keep" ] || [ "${1:-}" = "--keep" ]; then
  ARGS+=(--keep)
fi
exec "$PYTHON" -m whub "${ARGS[@]}"
