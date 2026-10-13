#!/usr/bin/env bash
set -euo pipefail

PIDS=()
DATA_DIR="${P2P_DATA_DIR:-$(mktemp -d)}"
BIN="target/debug/p2p-chain-lib-cli"
VALIDATORS=(
  22d10810db610559ff9bb65c36c44244832d024d996ed2fc1a11e34a68618add
  ae415a841259daa98f1bc87c03e7eb8749c17cc1db6e016ff70e57b4aa12d866
  74b1d277044007b071fcf277a3cc5194eaa0bca28548f6621febf3c00810c331
  c3667662e1d8aebb369374c3b854d81a31b2dc540ea44479cfe75542fc7f529f
  738b55353a652a23688ba11369c96121b891c0fe4003a7851eeb41bb1ea44abb
)

cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  if [[ -z "${P2P_DATA_DIR:-}" ]]; then
    rm -rf "$DATA_DIR"
  fi
}

trap cleanup EXIT INT TERM

run_node() {
  local name="$1"
  local port="$2"
  shift 2

  RUST_LOG="${RUST_LOG:-info}" "$BIN" \
    --listen "/ip4/127.0.0.1/tcp/${port}" \
    --node-key-seed "${name}" \
    --chain-path "${DATA_DIR}/${name}" \
    --validator "${VALIDATORS[0]}" \
    --validator "${VALIDATORS[1]}" \
    --validator "${VALIDATORS[2]}" \
    --validator "${VALIDATORS[3]}" \
    --validator "${VALIDATORS[4]}" \
    --produce-block-interval-ms 5000 \
    "$@" &
  PIDS+=("$!")
}

cargo build -p p2p-chain-lib-cli
echo "five-node data directory: ${DATA_DIR}"

run_node node-1 4001
sleep 1
run_node node-2 4002 --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/12D3KooWCAGu6gqDrkDWWcFnjsT9Y8rUzUH8buWjdFcU3TfWRmuN
sleep 1
run_node node-3 4003 --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/12D3KooWCAGu6gqDrkDWWcFnjsT9Y8rUzUH8buWjdFcU3TfWRmuN
sleep 1
run_node node-4 4004 --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/12D3KooWCAGu6gqDrkDWWcFnjsT9Y8rUzUH8buWjdFcU3TfWRmuN
sleep 1
run_node node-5 4005 --bootstrap /ip4/127.0.0.1/tcp/4001/p2p/12D3KooWCAGu6gqDrkDWWcFnjsT9Y8rUzUH8buWjdFcU3TfWRmuN

wait
