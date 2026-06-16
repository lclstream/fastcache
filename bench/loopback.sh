#!/usr/bin/env bash
# Loopback relay benchmark harness with a hard auto-kill.
#
# Launches relay + sink + source on 127.0.0.1, lets them run for DURATION
# seconds, then kills EVERYTHING it started -- including on Ctrl-C, error, or
# normal exit. Nothing is left bound to a port afterwards.
#
# Usage:
#   ./bench/loopback.sh [DURATION] [STREAMS] [RELAY_IOT] [END_IOT] [HWM] [FRAME_KB] [POOL] [RELAY_MODE]
# Defaults:
#   DURATION=10 STREAMS=32 RELAY_IOT=16 END_IOT=8 HWM=64 FRAME_KB=8192 POOL=8 RELAY_MODE=node
#
# RELAY_MODE selects the relay implementation:
#   node     --relay-node       ZMQ PUSH/PULL relay (recv+send, one userspace copy)
#   splice   --splice-relay     Linux kernel splice() zero-copy relay (Linux only)
#   zerocopy --zerocopy-relay    Linux recv()+send(MSG_ZEROCOPY) relay (Linux only)
# For splice/zerocopy the relay's io-threads arg is ignored (kernel-driven).
#
# POOL = source buffers/stream (--source-pool): a recycling pool + per-frame
# memset that models a REAL producer. POOL=0 uses the single-reused-buffer
# cheat (the throughput floor). The pool is expected to MATCH the cheat, not
# beat it -- it proves correctness is free, not that it adds speed.
#
# Examples:
#   ./bench/loopback.sh                 # 10s, relay 16 io-threads, ends 8, pool 8
#   ./bench/loopback.sh 20 32 8 8       # 20s, everyone 8 io-threads
#   ./bench/loopback.sh 15 32 32 8      # relay gets 32 io-threads
#   ./bench/loopback.sh 10 32 16 8 64 8192 0   # single-buffer cheat (POOL=0)
#   ./bench/loopback.sh 10 32 16 8 64 8192 8 splice     # kernel splice relay
#   ./bench/loopback.sh 10 32 16 8 64 8192 8 zerocopy   # MSG_ZEROCOPY relay

set -u

DURATION="${1:-10}"
STREAMS="${2:-32}"
RELAY_IOT="${3:-16}"
END_IOT="${4:-8}"
HWM="${5:-64}"
FRAME_KB="${6:-8192}"
POOL="${7:-8}"
RELAY_MODE="${8:-node}"

BIN=./build/fastcache-bench
FRONTEND=5600
BACKEND=5700

# Track every PID we spawn so cleanup is exact (no killall, no port guessing).
PIDS=()

cleanup() {
    # Kill in reverse order: source, sink, then relay last.
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        kill "${PIDS[$i]}" 2>/dev/null
    done
    # Give them a moment to release sockets, then force-kill stragglers.
    sleep 0.3
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        kill -9 "${PIDS[$i]}" 2>/dev/null
    done
    wait 2>/dev/null
}
# Run cleanup on any exit path: normal end, Ctrl-C (INT), TERM, or error.
trap cleanup EXIT INT TERM

# Map RELAY_MODE to the relay flag.
case "$RELAY_MODE" in
    node)     RELAY_FLAG="--relay-node" ;;
    splice)   RELAY_FLAG="--splice-relay" ;;
    zerocopy) RELAY_FLAG="--zerocopy-relay" ;;
    *) echo "unknown RELAY_MODE='$RELAY_MODE' (use node|splice|zerocopy)" >&2; exit 1 ;;
esac

echo "== loopback: mode=${RELAY_MODE} duration=${DURATION}s streams=${STREAMS} relay_iot=${RELAY_IOT} end_iot=${END_IOT} hwm=${HWM} frame=${FRAME_KB}KiB pool=${POOL} =="

"$BIN" "$RELAY_FLAG" --relay-listen "$FRONTEND" --relay-backend "$BACKEND" \
    --secs 0 --streams "$STREAMS" --io-threads "$RELAY_IOT" --hwm "$HWM" &
PIDS+=($!)
sleep 0.5

"$BIN" --sink --sink-connect "127.0.0.1:${BACKEND}" \
    --secs 0 --streams "$STREAMS" --io-threads "$END_IOT" --hwm "$HWM" &
PIDS+=($!)
sleep 0.5

"$BIN" --source --source-connect "127.0.0.1:${FRONTEND}" \
    --secs 0 --streams "$STREAMS" --io-threads "$END_IOT" --hwm "$HWM" \
    --frame-kb "$FRAME_KB" --source-pool "$POOL" &
PIDS+=($!)

# Let it run, then the EXIT trap kills everything we started.
sleep "$DURATION"
echo "== ${DURATION}s elapsed, killing all spawned processes =="
