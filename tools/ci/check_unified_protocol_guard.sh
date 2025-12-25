#!/usr/bin/env bash
set -euo pipefail

# == 统一协议红线守卫脚本（教案级注释） ==
#
# ## 意图 (Why)
# 1. 将“统一协议”公共 API 的红线固化到 CI 中，避免在缺少 CEP 备案的情况下
#    私自扩展或修改协议面向调用方的接口，导致生态碎片化。
# 2. 统一告知贡献者：凡触碰协议层 API（例如 `Service`、`Channel`、`CallContext` 等）
#    必须走治理流程，提前准备兼容性评估与迁移策略。
#
# ## 所在位置与架构作用 (Where)
# - 脚本位于 `tools/ci/`，由 GitHub Actions 在 `lint-and-docs` Job 的最前置步骤调起。
# - 与 `check_consistency.sh` 等语义护栏协作，前者保证术语一致性，当前脚本
#   负责治理流程（CEP）的硬约束。
#
# ## 核心策略 (How)
# - 基于 Git diff 识别公共 API 变更：
#   1. 拉取基线分支（默认 `main` 或 PR 的 `GITHUB_BASE_REF`），计算与 HEAD 的差异。
#   2. 在 `spark-*/src/**/*.rs` 范围内，查找新增/删除的 `pub` 声明（排除 `pub(crate)`、
#      `pub(super)`、`pub(self)`、`pub(in ...)` 等内部可见性）。
#   3. 若检测到公共 API 变更，进一步检查是否提交了 CEP 文档（`docs/governance/CEP-*.md`）。
# - 当公共 API 变化却无 CEP 记录时立即失败，并给出修复指引。
#
# ## 契约 (What)
# - **输入参数**：可选环境变量
#   - `UNIFIED_PROTOCOL_BASE_REF`：覆盖默认基线分支。
#   - `UNIFIED_PROTOCOL_OVERRIDE=allow`：在紧急回滚等特殊情况下临时放宽（需在 PR 说明）。
# - **前置条件**：
#   1. 当前目录为 Git 仓库根目录（脚本内部会切换到根目录）。
#   2. 可访问 `origin/<base>` 分支；脚本会自动 `git fetch`。
#   3. 环境已安装 `git`、`rg`（CI 镜像已包含）。
# - **返回值**：
#   - 通过：退出码 0，无输出或仅有提示信息。
#   - 失败：输出违规详情，退出码非 0，阻断后续流水线。
# - **后置条件**：若失败，仓库状态不被修改；开发者需补齐 CEP 或撤回 API 变更。
#
# ## 设计考量与权衡 (Trade-offs)
# - 选择基于文本 diff，而非 Rust AST，原因是 diff 更轻量且适用于 CI 快速反馈；
#   但需约束代码风格（例如 `pub` 必须单独成词），已通过 `rustfmt` 保障。
# - 将检测范围限定在 `spark-*` 前缀 crate，避免误拦测试/工具代码；如未来新增公共
#   crate，应保持命名约定或同步更新此脚本。
# - 允许通过环境变量临时豁免，便于紧急 hotfix；但需在 PR 模板中勾选“触碰统一协议”
#   并说明原因，防止滥用。
#
# ## 风险提醒 (Gotchas)
# - 若 PR 仅重构内部实现（不涉及 `pub` 声明），脚本不会拦截，但仍建议同步更新
#   契约文档，避免语义漂移。
# - 若贡献者忘记 `git add` CEP 文件，脚本会误判为缺失 CEP；因此在本地自检时也应
#   运行该脚本或 `make ci-*`。
# - 当分支历史过旧时，`git fetch` 可能需更深历史；脚本使用 `--depth=100` 取得足够
#   合并基线，如仍失败需人工拉取完整历史。

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd "$REPO_ROOT"

if [[ "${UNIFIED_PROTOCOL_OVERRIDE:-}" == "allow" ]]; then
    echo "[unified-protocol] 检测被 UNIFIED_PROTOCOL_OVERRIDE=allow 跳过，确保在 PR 说明中记录治理豁免。"
    exit 0
fi

BASE_BRANCH=${UNIFIED_PROTOCOL_BASE_REF:-${GITHUB_BASE_REF:-main}}
FETCH_TARGET="origin/${BASE_BRANCH}"
MERGE_BASE=""
FALLBACK_DESC=""

if git remote | grep -qx origin; then
    if git fetch --no-tags --depth=100 origin "${BASE_BRANCH}" >/dev/null 2>&1; then
        MERGE_BASE=$(git merge-base HEAD "${FETCH_TARGET}" || true)
        FALLBACK_DESC="origin/${BASE_BRANCH}"
    else
        echo "[unified-protocol] 警告：无法从 origin 获取 ${BASE_BRANCH}，将尝试本地基线。" >&2
    fi
else
    echo "[unified-protocol] 未检测到 origin 远端，使用本地历史作为基线。" >&2
fi

if [[ -z "${MERGE_BASE}" ]]; then
    MERGE_BASE=$(git rev-parse HEAD^ 2>/dev/null || true)
    FALLBACK_DESC="HEAD^"
fi

if [[ -z "${MERGE_BASE}" ]]; then
    echo "统一协议守卫：无法确定基线提交，请确保仓库存在至少一个父提交或配置 origin。" >&2
    exit 1
fi

if [[ "${FALLBACK_DESC}" == "HEAD^" ]]; then
    echo "[unified-protocol] 使用 HEAD^ 作为比对基线；如与主干差异较大，建议先同步 main。" >&2
fi

API_DIFF=$(git diff "${MERGE_BASE}"...HEAD -- 'spark-*/src/**/*.rs' | \
    rg --pcre2 '^[+-]\s*pub(?!\s*\((crate|self|super)\)|\s*in\b)' || true)

if [[ -z "${API_DIFF}" ]]; then
    exit 0
fi

CEP_CHANGES=$(git diff --name-only "${MERGE_BASE}"...HEAD -- 'docs/governance/CEP-*.md' || true)
if [[ -n "${CEP_CHANGES}" ]]; then
    exit 0
fi

echo "统一协议守卫：检测到公共 API 变更，但未发现 CEP 文档更新。" >&2
echo "请执行以下任一操作后重新提交：" >&2
echo "  1. 在 docs/governance/ 下补充或更新 CEP（例如 CEP-xxxx-*.md），描述统一协议变更。" >&2
echo "  2. 若误报，请说明变更不影响公共 API，或使用 UNIFIED_PROTOCOL_OVERRIDE=allow（需在 PR 中备案）。" >&2
echo "-- 触发 diff 片段 --" >&2
echo "${API_DIFF}" >&2
exit 1
