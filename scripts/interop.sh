#!/usr/bin/env bash
# Mixed-language cluster test: three real replica processes, some orderer-rust
# (orderer-node) and some orderer-cpp (orderer_node), sharing one cluster,
# under load, with `kill -9` of the current leader several times. Passes only if
# every acknowledged batch survives on every replica, in the same order and
# exactly once, and all replicas (both languages) end at the same state digest.
#
#   scripts/interop.sh                       # nodes: rust rust cpp
#   IMPLS="cpp rust rust" SECS=30 KILLS=6 scripts/interop.sh
#   ORDERER_KEY=secret scripts/interop.sh    # authenticated transport
#
# Needs ../orderer-rust (built here with cargo) and a built orderer_node.
set -euo pipefail

cd "$(dirname "$0")/.."
IMPLS=${IMPLS:-"rust rust cpp"}
SECS=${SECS:-25}
KILLS=${KILLS:-5}
BASE=${BASE_PORT:-47900}
RUST=${RUST_DIR:-../orderer-rust}
CPP_BUILD=${CPP_BUILD:-build}

read -r -a KINDS <<< "$IMPLS"
N=${#KINDS[@]}

(cd "$RUST" && cargo build --release -q -p orderer-cluster --bin orderer-node --bin orderer-load)
cmake -S . -B "$CPP_BUILD" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$CPP_BUILD" --target orderer_node -j >/dev/null
RUST_NODE="$RUST/target/release/orderer-node"
LOAD="$RUST/target/release/orderer-load"
CPP_NODE="$CPP_BUILD/orderer_node"

TMP=$(mktemp -d)
MEMBERS=""
CLIENTS=""
for ((i = 0; i < N; i++)); do
  MEMBERS+="${MEMBERS:+,}$i=127.0.0.1:$((BASE + i))"
  CLIENTS+="${CLIENTS:+,}127.0.0.1:$((BASE + 100 + i))"
done
declare -a PIDS
for ((i = 0; i < N; i++)); do PIDS[i]=0; done

cleanup() {
  for ((i = 0; i < N; i++)); do kill -9 "${PIDS[$i]}" 2>/dev/null || true; done
  [[ -n "${LOAD_PID:-}" ]] && kill "$LOAD_PID" 2>/dev/null || true
  [[ -z "${KEEP:-}" ]] && rm -rf "$TMP"
}
trap cleanup EXIT

start_node() {
  local i=$1 bin
  [[ "${KINDS[$i]}" == "cpp" ]] && bin=$CPP_NODE || bin=$RUST_NODE
  "$bin" --id "$i" --members "$MEMBERS" --client "127.0.0.1:$((BASE + 100 + i))" \
    --dir "$TMP/n$i" --audit --digest-every 64 --fsync on --sm recorder >>"$TMP/n$i.log" 2>&1 &
  PIDS[i]=$!
}

leader() {
  "$LOAD" status --clients "$CLIENTS" 2>/dev/null | grep "role=Leader" | head -1 \
    | sed -E 's/.*id=([0-9]+).*/\1/'
}

for ((i = 0; i < N; i++)); do start_node "$i"; done
echo "started $N nodes (${IMPLS// /, }); logs in $TMP"
for _ in $(seq 1 100); do [[ -n "$(leader)" ]] && break; sleep 0.1; done
[[ -n "$(leader)" ]] || { echo "FAIL: no leader elected"; exit 1; }

"$LOAD" run --clients "$CLIENTS" --secs "$SECS" --acked "$TMP/acked" --maybe "$TMP/maybe" \
  --batch 50 --threads 3 >"$TMP/load.out" 2>&1 &
LOAD_PID=$!

for k in $(seq 1 "$KILLS"); do
  sleep $(( (SECS - 4) / (KILLS + 1) + 1 ))
  L=$(leader || true)
  [[ -z "$L" ]] && { sleep 1; L=$(leader || true); }
  [[ -z "$L" ]] && { echo "FAIL: no leader before kill $k"; exit 1; }
  echo "kill $k/$KILLS: kill -9 leader node $L (${KINDS[$L]})"
  kill -9 "${PIDS[$L]}"
  wait "${PIDS[$L]}" 2>/dev/null || true
  sleep 1
  start_node "$L"
done

wait "$LOAD_PID"
LOAD_PID=
cat "$TMP/load.out"

"$LOAD" settle --clients "$CLIENTS" --timeout 60
APPLIED=""
for ((i = 0; i < N; i++)); do APPLIED+="${APPLIED:+,}$TMP/n$i/applied.log"; done
"$LOAD" verify --acked "$TMP/acked" --maybe "$TMP/maybe" --applied "$APPLIED"
