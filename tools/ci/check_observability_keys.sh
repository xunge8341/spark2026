#!/usr/bin/env bash
# 教案级注释：可观测性键名 SOT 同步守门脚本
#
# 目标（Why）：
# - 保证 `contracts/observability_keys.toml` 与生成产物 `crates/spark-core/src/governance/observability/keys.rs`、
#   `docs/observability-contract.md` 时刻保持一致，避免指标/日志/追踪键名出现多处漂移；
# - 在 CI 阶段及早阻断“只改合约未生成代码/文档”的情况，减轻评审与回滚成本；
# - 为后续仪表盘治理提供稳定约束，确保所有引用均来自单一事实来源（Single Source of Truth）。
#
# 契约（What）：
# - 输入：工作目录需位于仓库根目录；脚本会读取 Git 工作区状态并执行生成工具；
# - 前置条件：本地可运行 `cargo`，且已启用 `std` + `observability_contract_doc` feature；
# - 后置条件：若检测到生成产物与仓库快照不一致，脚本以非零状态退出并给出修复提示。
#
# 运行流程（How）：
# 1. 通过 `cargo run` 执行文档生成器，覆盖写入 `docs/observability-contract.md`；
# 2. 编译 `spark-core` 以触发构建脚本写出 `observability/keys.rs`；
# 3. 使用 `cargo fmt` 对生成产物进行一次格式化（避免 rustfmt 与代码生成器之间的风格差异反复打架）；
# 4. 使用 `git diff` 检查上述两个产物是否存在未提交的差异；
# 5. 如有差异，输出详细说明并返回失败，提示开发者重新生成后提交。
#
# 风险与权衡（Trade-offs）：
# - 选择直接编译 `spark-core` 触发构建脚本，虽然耗时略高，但避免实现额外的重复生成逻辑；
# - 若仓库存在其他未提交的更改，脚本仍会正常工作，但建议在干净工作区运行以便阅读 diff。
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "${ROOT_DIR}" || exit 1

cargo run --quiet -p spark-core --bin gen_observability_doc \
  --features std,observability_contract_doc
cargo build --quiet -p spark-core
cargo fmt --quiet -- \
  crates/spark-core/src/governance/configuration/events.rs \
  crates/spark-core/src/governance/observability/keys.rs

if ! git diff --quiet -- crates/spark-core/src/governance/observability/keys.rs; then
  echo "可观测性键名生成文件与合约不同步：请运行构建并提交 \"crates/spark-core/src/governance/observability/keys.rs\"。" >&2
  exit 1
fi

if ! git diff --quiet -- docs/observability-contract.md; then
  echo "可观测性键名文档未同步更新：请运行生成器并提交 \"docs/observability-contract.md\"。" >&2
  exit 1
fi

exit 0
