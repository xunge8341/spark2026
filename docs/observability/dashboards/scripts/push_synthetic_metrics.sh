#!/usr/bin/env bash
# 调用 Python 版的推送脚本，便于直接执行。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${SCRIPT_DIR}/push_synthetic_metrics.py"
