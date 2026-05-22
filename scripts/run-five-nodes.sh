#!/usr/bin/env bash
set -euo pipefail

PIDS=()

cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}

trap cleanup EXIT INT TERM

run_node() {
  local name="$1"
  local port="$2"
  shift 2

  RUST_LOG="${RUST_LOG:-info}" cargo run -p p2p-chain-lib-cli -- \
    --listen "/ip4/127.0.0.1/tcp/${port}" \
    --node-key-seed "${name}" \
    --produce-block-interval-ms 5000 \
    "$@" &
  PIDS+=("$!")
}

run_node node-1 4001
sleep 1
run_node node-2 4002 --bootstrap /ip4/127.0.0.1/tcp/4001
run_node node-3 4003 --bootstrap /ip4/127.0.0.1/tcp/4001
run_node node-4 4004 --bootstrap /ip4/127.0.0.1/tcp/4001
run_node node-5 4005 --bootstrap /ip4/127.0.0.1/tcp/4001

wait
