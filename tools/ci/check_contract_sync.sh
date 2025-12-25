#!/usr/bin/env bash
# 教案级注释：ReadyState / ErrorCategory / ObservabilityContract / BufView 契约同步守门脚本
#
# 目标 (Why)：
# - 核心目的：当 PR 在 `crates/spark-core/src` 中改动 ReadyState、ErrorCategory、ObservabilityContract 或 BufView 契约实现时，强制要求作者同步更新
#   `docs/` 目录下的正式文档或仪表盘说明，确保核心契约变化能第一时间传达到运行与治理团队；同时，针对错误分类矩阵（代码 + SOT 文档）
#   提供双向守门，避免业务契约与知识库出现“只改其一”的偏差。
# - 在整体流程中的定位：该脚本作为 CI Lints & docs 阶段的早期守门人，位于编译/测试之前快速阻断缺少文档的契约变更，降低回滚成本。
# - 设计理念：通过 `git diff` 精确定位 `crates/spark-core/src` 目录内涉及关键契约的改动，再校验是否伴随至少一项 `docs/*` 更新，以最小成本获得治理信号。
#
# 逻辑解析 (How)：
# 1. 仅在 Pull Request 事件下执行：push 到主分支的提交已经通过评审，此处无需重复守门。
# 2. 自动解析对比基线：优先使用 PR 基线 SHA；若缺失则回退到目标分支或 `origin/main`，保证 fork 与分支均可运行。
# 3. 限定扫描范围：只分析 `crates/spark-core/src/**/*.rs` 的 diff，过滤测试/文档噪声；随后匹配四个关键契约的标识符。
# 4. 收集触发契约：若 diff 中包含 `ReadyState|ErrorCategory|ObservabilityContract|BufView`，记录涉及的源文件列表。
# 5. 错误分类矩阵 SOT 守门：要求 `crates/spark-core/src/error/generated/category_matrix.rs` 与 `docs/error-category-matrix.md` 必须同步改动，防止代码与文档分离。
# 6. 校验文档更新：检查本次提交是否修改了任意 `docs/` 前缀的文件；若缺失则输出详细指引并失败退出。
#
# 契约说明 (What)：
# - 输入：
#   * 环境变量 `GITHUB_EVENT_NAME`、`PR_BASE_SHA`、`GITHUB_BASE_REF`（GitHub Actions 在 PR 步骤中注入）。
# - 前置条件：
#   * 当前工作目录位于仓库根目录，且本地存在 PR Head 以及可访问的 `origin` 远程。
#   * 基线提交必须可解析；脚本会在必要时自动 `git fetch`。
# - 后置条件：
#   * 若检测到契约改动且缺少文档，脚本以非零退出码终止，CI 因此失败。
#   * 若契约改动配套文档齐全或未命中关键契约，脚本静默退出 (0)。
#
# 设计权衡与注意事项 (Trade-offs & Gotchas)：
# - 关键字匹配 VS AST：选择关键字匹配以降低实现复杂度与运行时间；可能出现极少数同名符号导致的“误报”，此时需要人工确认是否应更新文档。
# - 扫描范围：限定 `crates/spark-core/src` 避免测试或外部 crate 的噪声；若未来将契约迁移至其他 crate，需要同步扩展 `SCOPE_PATHSPEC`。
# - 文档检查宽松：仅要求任意 `docs/` 变更，允许作者在最合适的文件中补充说明；治理团队可在评审中进一步确认内容质量。
# - SOT 守门：针对错误分类矩阵额外执行一对一同步检查，优先保障易遗忘的知识库对齐；该约束是对通用文档检查的增强且仅影响特定文件。
# - 性能：`git diff` 和 `grep` 的成本极低，脚本不修改工作区，也不会生成临时文件，确保在 CI 中运行稳定。
#
# 风险提示：
# - 若作者以“更新文档在其他 PR 中处理”为由拆分提交，本守门人会拒绝合并；请在同一 PR 中完成契约与文档的同步。
# - 若确实无需文档（罕见），请在 PR 中说明并在评审中获得豁免，但此脚本仍会要求最少的 `docs/` 变更，可通过更新摘要或变更记录满足。
set -euo pipefail

if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" ]]; then
  exit 0
fi

readonly BASE_SHA_ENV="${PR_BASE_SHA:-}"
readonly BASE_REF_ENV="${GITHUB_BASE_REF:-main}"

resolve_base_commit() {
  local candidate_sha="$1"
  local candidate_ref="$2"

  # 解析顺序：显式 SHA > 目标分支远程引用 > origin/main > 本地 main > HEAD^
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

  if git rev-parse --verify origin/main >/dev/null 2>&1; then
    git rev-parse origin/main
    return
  fi

  if git rev-parse --verify main >/dev/null 2>&1; then
    git rev-parse main
    return
  fi

  git rev-parse HEAD^
}

readonly BASE_COMMIT="$(resolve_base_commit "$BASE_SHA_ENV" "$BASE_REF_ENV")"

# 限定扫描范围：使用 GIT pathspec 的 glob 语法，仅关注 crates/spark-core/src 下的 Rust 文件。
readonly SCOPE_PATHSPEC=':(glob)crates/spark-core/src/**/*.rs'
readonly CONTRACT_REGEX='\b(ReadyState|ErrorCategory|ObservabilityContract|BufView)\b'
readonly CATEGORY_MATRIX_SOURCE='crates/spark-core/src/error/generated/category_matrix.rs'
readonly CATEGORY_MATRIX_DOC='docs/error-category-matrix.md'
readonly CATEGORY_MATRIX_CONTRACT='contracts/error_matrix.toml'
readonly CONFIG_EVENTS_SOURCE='crates/spark-core/src/governance/configuration/events.rs'
readonly CONFIG_EVENTS_DOC='docs/configuration-events.md'
readonly CONFIG_EVENTS_CONTRACT='contracts/config_events.toml'
readonly CONFIG_EVENTS_SCHEMA_JSON='schemas/configuration-events.schema.json'
readonly CONFIG_EVENTS_ASYNCAPI_JSON='schemas/configuration-events.asyncapi.json'
readonly PYTHON_SDK_PATHSPEC=':(glob)sdk/python/**'
readonly JAVA_SDK_PATHSPEC=':(glob)sdk/java/**'
readonly CATEGORY_SURFACE_PATHS=(
  'crates/spark-core/src/error.rs'
  'crates/spark-core/src/error/generated/category_matrix.rs'
  'crates/spark-core/src/data_plane/pipeline/default_handlers.rs'
)

# 教案级注释：错误分类矩阵 SOT 守门（Why / How / What / Trade-offs）
# - Why：`contracts/error_matrix.toml` 是声明式 SOT，`generated/category_matrix.rs` 与 `docs/error-category-matrix.md` 分别承载代码与知识库视图。
#   任何一侧单独更新都会导致契约偏差，因此 CI 要求三者保持一致。
# - How：
#   1. 分别通过 `git diff --name-only` 检测合约、代码、文档是否在当前 PR 中被修改；
#   2. 只要任一文件发生改动，就强制校验其余两者也被修改；否则立即失败并输出指导信息。
# - What（契约）：
#   * 输入：无需额外参数，直接读取 `BASE_COMMIT` 与工作树 HEAD 的差异；
#   * 前置条件：仓库为 Git 工作区且已存在 `BASE_COMMIT`（脚本前序逻辑保证）；
#   * 后置条件：要么三者同时修改，要么均保持不变；若违反则脚本以非零码退出。
# - Trade-offs：
#   * 采用文件级别守门（而非解析矩阵内容），实现成本低且足以捕获“忘记同步”情形；
#   * 若确实仅调整生成逻辑，可同步提交微小的注释或排序改动以记录生成器变化。
category_matrix_source_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$CATEGORY_MATRIX_SOURCE" | grep -q .; then
  category_matrix_source_changed=true
fi

category_matrix_doc_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$CATEGORY_MATRIX_DOC" | grep -q .; then
  category_matrix_doc_changed=true
fi

category_matrix_contract_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$CATEGORY_MATRIX_CONTRACT" | grep -q .; then
  category_matrix_contract_changed=true
fi

if $category_matrix_source_changed || $category_matrix_doc_changed || $category_matrix_contract_changed; then
  if ! $category_matrix_source_changed || ! $category_matrix_doc_changed || ! $category_matrix_contract_changed; then
    echo "错误分类矩阵为 SOT 资源：合约 (${CATEGORY_MATRIX_CONTRACT})、代码 (${CATEGORY_MATRIX_SOURCE}) 与文档 (${CATEGORY_MATRIX_DOC}) 必须在同一 PR 中同步更新。" >&2
    echo "请补齐缺失的改动后再触发 CI。" >&2
    exit 1
  fi
fi

# 配置事件契约 SOT 守门：要求合约、生成代码与文档同步更新。
config_events_source_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$CONFIG_EVENTS_SOURCE" | grep -q .; then
  config_events_source_changed=true
fi

config_events_doc_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$CONFIG_EVENTS_DOC" | grep -q .; then
  config_events_doc_changed=true
fi

config_events_contract_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$CONFIG_EVENTS_CONTRACT" | grep -q .; then
  config_events_contract_changed=true
fi

config_events_schema_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$CONFIG_EVENTS_SCHEMA_JSON" | grep -q .; then
  config_events_schema_changed=true
fi

config_events_asyncapi_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$CONFIG_EVENTS_ASYNCAPI_JSON" | grep -q .; then
  config_events_asyncapi_changed=true
fi

python_sdk_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$PYTHON_SDK_PATHSPEC" | grep -q .; then
  python_sdk_changed=true
fi

java_sdk_changed=false
if git diff --name-only "$BASE_COMMIT"...HEAD -- "$JAVA_SDK_PATHSPEC" | grep -q .; then
  java_sdk_changed=true
fi

if $config_events_source_changed || $config_events_doc_changed || $config_events_contract_changed || $config_events_schema_changed || $config_events_asyncapi_changed || $python_sdk_changed || $java_sdk_changed; then
  if ! $config_events_source_changed || ! $config_events_doc_changed || ! $config_events_contract_changed || ! $config_events_schema_changed || ! $config_events_asyncapi_changed || ! $python_sdk_changed || ! $java_sdk_changed; then
    echo "配置事件契约为跨语言 SOT：Schema (${CONFIG_EVENTS_SCHEMA_JSON}) 与 AsyncAPI (${CONFIG_EVENTS_ASYNCAPI_JSON}) 是 Python/Java SDK 的事实来源，需与合约 (${CONFIG_EVENTS_CONTRACT})、代码 (${CONFIG_EVENTS_SOURCE})、文档 (${CONFIG_EVENTS_DOC}) 及 SDK 目录 (sdk/python, sdk/java) 在同一 PR 中同步更新。" >&2
    echo "请补齐缺失的改动后再触发 CI。" >&2
    exit 1
  fi
fi

# 教案级注释：错误分类公共接口 → 矩阵文档强制同步守门
# - Why：`ErrorCategory` 枚举与默认自动响应（`ExceptionAutoResponder`）是矩阵文档的“外部接口”；
#   若仅改动这些入口而未更新矩阵说明，文档会与实际行为脱节，直接影响排障和运维决策。
# - How：
#   1. 扫描 `CATEGORY_SURFACE_PATHS` 中的关键文件是否在当前 PR 中发生改动；
#   2. 若命中，则进一步确认 `docs/error-category-matrix.md` 是否伴随更新；
#   3. 缺少文档更新时立即失败并提示具体文件，提示作者补齐知识库。
# - What（契约）：
#   * 输入：`CATEGORY_SURFACE_PATHS` 定义的文件列表；
#   * 前置条件：脚本已定位到 PR 对比基线，Git 工作区干净；
#   * 后置条件：若触发守门，将输出命中文件路径并以非零退出码终止。
# - Trade-offs：
#   * 采用文件级别守门而非语义 diff，可在极低成本下覆盖 90%+ 的漂移风险；
#   * 若确实只对这些文件做无行为影响的注释修订，可同时在文档中补充说明，保持一致性。
category_surface_hits=()
for surface in "${CATEGORY_SURFACE_PATHS[@]}"; do
  if git diff --name-only "$BASE_COMMIT"...HEAD -- "$surface" | grep -q .; then
    category_surface_hits+=("$surface")
  fi
done

if ((${#category_surface_hits[@]} > 0)) && ! $category_matrix_doc_changed; then
  echo "检测到以下 ErrorCategory / 默认自动响应入口发生改动，但 docs/error-category-matrix.md 未同步更新：" >&2
  for path in "${category_surface_hits[@]}"; do
    echo "- ${path}" >&2
  done
  echo "请在同一 PR 中补充更新 ${CATEGORY_MATRIX_DOC}，确保文档描述与代码行为一致。" >&2
  exit 1
fi

mapfile -t candidate_files < <(git diff "$BASE_COMMIT"...HEAD --name-only -- "$SCOPE_PATHSPEC")
if ((${#candidate_files[@]} == 0)); then
  exit 0
fi

contract_hits=()
for file in "${candidate_files[@]}"; do
  diff_chunk="$(git diff "$BASE_COMMIT"...HEAD -- "$file")"
  if grep -Eq "$CONTRACT_REGEX" <<<"$diff_chunk"; then
    contract_hits+=("$file")
  fi
done

if ((${#contract_hits[@]} == 0)); then
  exit 0
fi

mapfile -t docs_changes < <(git diff "$BASE_COMMIT"...HEAD --name-only -- 'docs/**')
if ((${#docs_changes[@]} > 0)); then
  exit 0
fi

echo "检测到以下契约文件改动但缺少 docs/* 同步：" >&2
for file in "${contract_hits[@]}"; do
  echo "- ${file}" >&2
done

echo "请同步更新 ReadyState / ErrorCategory / ObservabilityContract / BufView 相关文档或仪表盘，并在 PR 中说明具体文件。" >&2
exit 1
