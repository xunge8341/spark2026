#!/usr/bin/env bash
# 教案级注释：配置事件契约 SOT 同步守门脚本
#
# 目标（Why）：
# - 保证 `contracts/config_events.toml` 与生成产物 `crates/spark-core/src/governance/configuration/events.rs`、
#   `docs/configuration-events.md` 时刻保持一致，避免控制面事件语义与审计文档出现漂移；
# - 在 CI 阶段快速发现遗漏的生成步骤，降低回滚和手工排查成本。
#
# 契约（What）：
# - 输入：仓库根目录，脚本会调用构建器与文档生成器；
# - 前置条件：启用 `std` 与 `configuration_event_doc` feature 以访问文件系统与 `toml`；
# - 后置条件：若检测到未提交的差异则以非零状态退出并提示修复方式。
#
# 实现方式（How）：
# 1. 运行 `gen_config_events_doc` 生成 Markdown 文档；
# 2. 编译 `spark-core`，触发 `build.rs` 写出最新的 `events.rs`；
# 3. 使用 `git diff` 校验上述产物是否已纳入提交。

set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "${ROOT_DIR}" || exit 1

cargo run --quiet -p spark-core --bin gen_config_events_doc \
  --features std,configuration_event_doc
cargo build --quiet -p spark-core

if ! git diff --quiet -- crates/spark-core/src/governance/configuration/events.rs; then
  echo "配置事件生成文件与合约不同步：请提交 \"crates/spark-core/src/governance/configuration/events.rs\"。" >&2
  exit 1
fi

if ! git diff --quiet -- docs/configuration-events.md; then
  echo "配置事件文档未同步更新：请运行生成器并提交 \"docs/configuration-events.md\"。" >&2
  exit 1
fi

exit 0
