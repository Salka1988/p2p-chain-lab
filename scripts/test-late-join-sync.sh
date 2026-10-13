#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
cargo test -p p2p-chain-lib-cli --locked --test late_join_sync -- --ignored --nocapture
