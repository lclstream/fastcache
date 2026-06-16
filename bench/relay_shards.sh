#!/usr/bin/env bash
# Sharded loopback relay benchmark with a hard auto-kill.
#
# Splits STREAMS across SHARDS independent relay processes, each with its own
# matching source + sink half on a disjoint port range. The idea: one
# dual-socket relay tops out ~60 Gb/s, but a direct PUSH/PULL pair does ~80.
# If N smaller relays (each like a near-direct pair) sum to more than one big
# relay, the bottleneck was per-process io-thread/socket contention, and the
# production answer is to shard the relay rather than scale one process.
#
# Everything spawned is killed after DURATION -- and on Ctrl-C/error/exit.
# Each source prints its own per-second Gb/s; SUM the shards for total rate.
#
# RELAY_IOT and END_IOT are TOTAL io-thread budgets across all shards, divided
# evenly per process. So `... 32 2 16 16` gives each of the 2 relays 8 io-threads
# (16 total), keeping the comparison against a single 16-io-thread relay fair --
# more shards must NOT mean more total io-threads.
#
# Usage:
#   ./bench/relay_shards.sh [DURATION] [STREAMS] [SHARDS] [RELAY_IOT_TOTAL] [END_IOT_TOTAL] [HWM] [FRAME_KB] [POOL] [RELAY_MODE]
# Defaults:
#   DURATION=10 STREAMS=32 SHARDS=2 RELAY_IOT_TOTAL=16 END_IOT_TOTAL=8 HWM=64 FRAME_KB=8192 POOL=8 RELAY_MODE=node
#
# RELAY_MODE selects the relay implementation per shard:
#   node     --relay-node       ZMQ PUSH/PULL relay (recv+send, one userspace copy)
#   splice   --splice-relay     Linux kernel splice() zero-copy relay (Linux only)
#   zerocopy --zerocopy-relay    Linux recv()+send(MSG_ZEROCOPY) relay (Linux only)
# For splice/zerocopy the relay io-thread budget is ignored (kernel-driven).
#
# POOL = source buffers/stream (--source-pool); models a real producer. POOL=0
# uses the single-reused-buffer cheat. The pool is expected to MATCH the cheat.
#
# Examples:
#   ./bench/relay_shards.sh 10 32 2 16 8   # 2 relays x 16 streams, 8 relay io-threads each
#   ./bench/relay_shards.sh 10 32 4 16 8   # 4 relays x  8 streams, 4 relay io-threads each
#   ./bench/relay_shards.sh 10 32 1 16 8   # 1 relay  x 32 streams, 16 io-threads (baseline)
#   ./bench/relay_shards.sh 10 32 2 16 8 64 8192 8 zerocopy   # MSG_ZEROCOPY shards

set -u

DURATION="${1:-10}"
STREAMS="${2:-32}"
SHARDS="${3:-2}"
RELAY_IOT_TOTAL="${4:-16}"
END_IOT_TOTAL="${5:-8}"
HWM="${6:-64}"
FRAME_KB="${7:-8192}"
POOL="${8:-8}"
RELAY_MODE="${9:-node}"

BIN=./build/fastcache-bench

case "$RELAY_MODE" in
    node)     RELAY_FLAG="--relay-node" ;;
    splice)   RELAY_FLAG="--splice-relay" ;;
    zerocopy) RELAY_FLAG="--zerocopy-relay" ;;
    *) echo "unknown RELAY_MODE='$RELAY_MODE' (use node|splice|zerocopy)" >&2; exit 1 ;;
esac

if (( STREAMS % SHARDS != 0 )); then
    echo "STREAMS ($STREAMS) must be divisible by SHARDS ($SHARDS)" >&2
    exit 1
fi
PER=$(( STREAMS / SHARDS ))

# Divide the total io-thread budgets across shards (min 1 each).
RELAY_IOT=$(( RELAY_IOT_TOTAL / SHARDS )); (( RELAY_IOT < 1 )) && RELAY_IOT=1
END_IOT=$(( END_IOT_TOTAL / SHARDS ));     (( END_IOT < 1 ))   && END_IOT=1

# Disjoint port regions so no two binds collide:
#   shard s frontend base = 5600 + s*100   (relay PULL <- source PUSH)
#   shard s backend  base = 6600 + s*100   (relay PUSH -> sink PULL)
# Up to ~100 streams/shard fit between shards without overlap.
FRONTEND_REGION=5600
BACKEND_REGION=6600

PIDS=()
cleanup() {
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        kill "${PIDS[$i]}" 2>/dev/null
    done
    sleep 0.3
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        kill -9 "${PIDS[$i]}" 2>/dev/null
    done
    wait 2>/dev/null
}
trap cleanup EXIT INT TERM

echo "== sharded relay: mode=${RELAY_MODE} duration=${DURATION}s streams=${STREAMS} shards=${SHARDS} (${PER}/shard) relay_iot=${RELAY_IOT}/shard (${RELAY_IOT_TOTAL} total) end_iot=${END_IOT}/shard (${END_IOT_TOTAL} total) hwm=${HWM} frame=${FRAME_KB}KiB pool=${POOL} =="

# Start all relays first (they bind), then sinks (connect to backends),
# then sources (connect to frontends).
for ((s=0; s<SHARDS; s++)); do
    fe=$(( FRONTEND_REGION + s*100 ))
    be=$(( BACKEND_REGION  + s*100 ))
    "$BIN" "$RELAY_FLAG" --relay-listen "$fe" --relay-backend "$be" \
        --secs 0 --streams "$PER" --io-threads "$RELAY_IOT" --hwm "$HWM" &
    PIDS+=($!)
done
sleep 0.5

for ((s=0; s<SHARDS; s++)); do
    be=$(( BACKEND_REGION + s*100 ))
    "$BIN" --sink --sink-connect "127.0.0.1:${be}" \
        --secs 0 --streams "$PER" --io-threads "$END_IOT" --hwm "$HWM" &
    PIDS+=($!)
done
sleep 0.5

for ((s=0; s<SHARDS; s++)); do
    fe=$(( FRONTEND_REGION + s*100 ))
    echo "  shard $s: source -> 127.0.0.1:${fe} (${PER} streams)"
    "$BIN" --source --source-connect "127.0.0.1:${fe}" \
        --secs 0 --streams "$PER" --io-threads "$END_IOT" --hwm "$HWM" \
        --frame-kb "$FRAME_KB" --source-pool "$POOL" &
    PIDS+=($!)
done

sleep "$DURATION"
echo "== ${DURATION}s elapsed, killing all spawned processes (sum the shard Gb/s above) =="
