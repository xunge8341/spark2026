#!/usr/bin/env bash
# 教案说明：运行 fuzz 极限样本回归，兼容 CI 与本地环境。
#
# - Why：确保 `protocol_regression` harness 在语料与短时间 fuzz 下均无崩溃，
#   防止历史缺陷复发。
# - How：先执行 `cargo test` 重放语料，再使用 `cargo fuzz run` 在限定时间内
#   验证 harness 构建与语料执行链路。
# - What：脚本依赖已安装的 `cargo fuzz` 与 nightly toolchain，可通过
#   `FUZZ_TOOLCHAIN` 覆盖默认版本。
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
CRATE_DIR="$ROOT_DIR/crates/3p/spark2026-fuzz"
FUZZ_TOOLCHAIN=${FUZZ_TOOLCHAIN:-nightly-2024-12-31}
FUZZ_TARGETS=(protocol_regression)

if ! cargo fuzz --help >/dev/null 2>&1; then
    echo "cargo fuzz 未安装：请先运行 'cargo install cargo-fuzz'" >&2
    exit 1
fi

pushd "$CRATE_DIR" >/dev/null

cargo test --tests

for target in "${FUZZ_TARGETS[@]}"; do
    echo "[fuzz-regression] run target=$target"
    cargo +"$FUZZ_TOOLCHAIN" fuzz run "$target" -- -max_total_time=5
    echo "[fuzz-regression] completed target=$target"
done

popd >/dev/null
