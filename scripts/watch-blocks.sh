#!/usr/bin/env bash
set -euo pipefail

docker compose logs -f --no-log-prefix \
  | sed '/^[[:space:]]*$/d' \
  | grep --line-buffered -E 'block produced|block announcement received|block sync requested|block request received|block received|WARN|ERROR'
