#!/usr/bin/env bash
set -euo pipefail

# == task_local! 禁用巡检（教案级注释） ==
#
# ## 意图 (Why)
# 1. 确保运行时语法糖始终以显式 [`CallContext`] 传递上下文，防止贡献者为了便利重新引入 `task_local!`
#    隐式全局，破坏取消/截止语义的一致性；
# 2. 将约束固化在 CI 中，一旦检测到违例立即失败，避免在 code review 阶段才发现“暗藏全局状态”。
#
# ## 所在位置与架构作用 (Where)
# - 位于 `tools/ci/`，与其它守护脚本并列；
# - 适用于 `make ci-*` 与手工执行，建议在提交前运行以确保改动符合契约。
#
# ## 核心策略 (How)
# - 基于 `git ls-files` 列出受版本控制的源文件，避免扫描到构建产物；
# - 通过 `ripgrep` 精确匹配 `task_local!` 宏调用；
# - 若命中，打印违规文件与提示说明后退出非零码。
#
# ## 契约说明 (What)
# - **前置条件**：环境需安装 `git` 与 `rg`；
# - **输出**：
#   - 成功：无输出且退出码为 0；
#   - 失败：列出违规路径并提示使用显式上下文替代方案；
# - **后置条件**：如遇违规，脚本立即终止后续 CI 流程。
#
# ## 风险与注意事项 (Trade-offs & Gotchas)
# - 扫描基于关键字匹配，若未来引入字符串常量或文档示例需要保留 `task_local!`，
#   应在脚本中补充 allowlist；
# - 脚本仅覆盖已追踪文件，提交前请确保 `git add` 以纳入检查范围。

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd "$REPO_ROOT"

violations=$(git ls-files | rg --files-with-matches 'task_local!' || true)

if [[ -n "$violations" ]]; then
    printf '检测到禁止使用的 task_local! 宏：\n%s\n' "$violations" >&2
    printf '请改用显式传递 CallContext 或运行时能力引用，避免隐式全局状态。\n' >&2
    exit 1
fi
