#!/usr/bin/env bash
# 教案级注释：该脚本为 ReadyState / PollReady / Context 变更治理守门人
#
# 目标 (Why):
# - 核心目标：在 PR 触及 ReadyState / PollReady / Context 的 Rust 代码时，确保提交者已经准备好契约测试、指标/Runbook 更新以及 cargo semver-checks 报告。
# - 在 CI 架构中的作用：作为 Lints & docs 阶段的早期守门人，阻断缺少治理材料的 PR 合并，避免发布破坏性行为。
# - 设计思路：通过 diff 分析识别敏感符号，再检索 PR 描述中是否勾选对应核对项，从而用最小成本获得合规信号。
#
# 逻辑解析 (How):
# 1. 仅在 pull_request 场景下执行，其他触发源（如 push/main）不需要治理校验。
# 2. 自动补全对比基线：优先使用 PR base SHA；若不可用则回退到 GITHUB_BASE_REF 或本地 origin/main。
# 3. 仅检查 Rust (.rs) 文件的 diff，匹配 ReadyState / PollReady / Context 关键字，避免文档噪声触发误报。
# 4. 一旦命中敏感符号，就解析 PR 描述文本，确认三个核对项均被显式勾选（- [x]）。
# 5. 若有缺失项，打印清晰的故障提示并返回非零退出码，CI 将因此失败。
#
# 契约 (What):
# - 输入参数：依赖以下环境变量
#   * GITHUB_EVENT_NAME：GitHub Actions 默认注入的事件名称。
#   * PR_BODY：GitHub Actions 步骤通过 env 传入的 PR 描述全文。
#   * PR_BASE_SHA：Pull Request 基线提交的 SHA；若缺失则自动回退。
#   * GITHUB_BASE_REF：Pull Request 目标分支名，用于回退抓取。
# - 前置条件：脚本运行目录位于仓库根目录；git 工作区包含 PR head；可访问 origin 远程。
# - 后置条件：
#   * 若命中了敏感符号且核对项齐全，脚本安静退出 (0)。
#   * 若命中但缺少核对项，脚本输出错误信息并以 (1) 退出，阻断合并。
#   * 若未命中敏感符号，脚本直接通过 (0) 退出，不影响普通 PR。
#
# 设计权衡与提醒 (Trade-offs):
# - 采用关键字匹配而非 AST 分析，换取实现简单与性能；权衡是极少数非 ReadyState 语义但包含同名符号的场景可能触发治理，需要人工确认。
# - 仅扫描 Rust 文件，避免纯文档变更误触发；若未来需要覆盖其他语言文件，可扩展 paths 数组。
# - PR 描述检查依赖 `[x]` 勾选，若使用者误删核对项，会提示缺少，指导其补齐。
# - 性能敏感点：git diff 与 grep 操作在 CI 中开销较小，但仍保持无副作用设计，避免修改工作区。
set -euo pipefail

if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" ]]; then
  exit 0
fi

readonly PR_BODY_TEXT="${PR_BODY:-}"
readonly BASE_SHA_ENV="${PR_BASE_SHA:-}"
readonly BASE_REF_ENV="${GITHUB_BASE_REF:-main}"

resolve_base_commit() {
  local candidate_sha="$1"
  local candidate_ref="$2"

  if [[ -n "$candidate_sha" ]]; then
    if ! git rev-parse --verify "$candidate_sha" >/dev/null 2>&1; then
      git fetch origin "$candidate_sha"
    fi
    git rev-parse "$candidate_sha"
    return
  fi

  local remote_ref="refs/remotes/origin/${candidate_ref}"
  if ! git rev-parse --verify "$remote_ref" >/dev/null 2>&1; then
    git fetch origin "${candidate_ref}:${remote_ref}" 2>/dev/null || git fetch origin "$candidate_ref"
  fi

  if git rev-parse --verify "$remote_ref" >/dev/null 2>&1; then
    git rev-parse "$remote_ref"
    return
  fi

  # 回退：若远程引用仍不存在，尝试寻找本地 main 或 merge-base。
  if git rev-parse --verify origin/main >/dev/null 2>&1; then
    git rev-parse origin/main
    return
  fi

  if git rev-parse --verify main >/dev/null 2>&1; then
    git rev-parse main
    return
  fi

  # 最后退路：使用 HEAD^ 作为基线，尽量提供 diff 能力。
  git rev-parse HEAD^
}

readonly BASE_COMMIT="$(resolve_base_commit "$BASE_SHA_ENV" "$BASE_REF_ENV")"

# 在基线和当前 HEAD 之间比对 Rust 文件的 diff，查找敏感关键字。
# 使用变量缓存 diff，以规避 `grep` 提前退出导致的 SIGPIPE（配合 `set -o pipefail` 会被视为失败）。
diff_content="$(git diff "$BASE_COMMIT"...HEAD -- '*.rs')"
if ! grep -Eq 'ReadyState|PollReady|context::Context' <<<"$diff_content"; then
  exit 0
fi

missing_checks=()
if ! grep -Eq '\- \[[xX]\] .*契约测试' <<<"$PR_BODY_TEXT"; then
  missing_checks+=("契约测试核对项未勾选")
fi
if ! grep -Eq '\- \[[xX]\] .*指标.*Runbook' <<<"$PR_BODY_TEXT"; then
  missing_checks+=("指标 / Runbook 核对项未勾选")
fi
if ! grep -Eq '\- \[[xX]\] .*semver-checks' <<<"$PR_BODY_TEXT"; then
  missing_checks+=("cargo semver-checks 报告核对项未勾选")
fi

if ((${#missing_checks[@]} == 0)); then
  exit 0
fi

echo "ReadyState / PollReady / Context 相关变更缺少以下治理材料：" >&2
for item in "${missing_checks[@]}"; do
  echo "- ${item}" >&2
done

echo "请在 PR 描述中勾选所有治理核对项，并附上对应材料后重试。" >&2
exit 1
