#!/usr/bin/env bash
# 教案级注释块: Public API diff 预算守门脚本详解。
# -----------------------------------------------------------------------------
# 教案级注释（满足 R1~R5）
# 目标 / 架构定位（Why, R1.1-R1.3）
#   * 本脚本负责在 CI 以及开发者本地执行“公共 API 破坏性变更预算”检查，
#     通过调用 cargo public-api diff 并解析输出来统计 breaking 变更数量。
#     当 Trait 合并等演进需要提供迁移窗口时，脚本提供“警戒线”能力 ——
#     超过预算即失败，促使提交者在 PR 模板中补充迁移方案。
#   * 该脚本位于 tools/ci，与 check_public_api_core.sh 等守门脚本并列，
#     在整体 DevOps 架构中属于“SemVer/兼容性防线”子模块，补足原有
#     `check_public_api_core.sh`“零第三方符号”检查无法覆盖的“变更节奏管理”。
#   * 关键依赖为 cargo-public-api 工具；通过 git stash + `cargo public-api diff` 的
#     组合，实现对任意基线 commit 的公共 API 快照比较。脚本利用“临时缓存 +
#     输出解析”模式提供可复现的诊断信息。
# 合同 / 输入输出（What, R3.1-R3.3）
#   * 输入参数：
#       - BASE_COMMIT（位置参数1或环境变量 PUBLIC_API_DIFF_BASE），默认 origin/main。
#         用于指定公共 API diff 的基线 commit。
#       - BREAKING_BUDGET（位置参数2或环境变量 PUBLIC_API_BREAKING_BUDGET），默认 0。
#         表示允许的 breaking 变更数量上限。
#       - 包名列表通过环境变量 PUBLIC_API_DIFF_PACKAGES 传入（空白分隔），默认仅
#         spark-core。
#   * 前置条件：
#       - 当前工作目录处于 spark2026 仓库（git rev-parse 成功）。
#       - 已安装 cargo public-api 与 rustup 对应 toolchain（脚本会检查可执行路径）。
#       - 工作树可自动暂存（若有未提交的 tracked 变更，脚本会暂存后恢复）。
#   * 输出：
#       - 正常情况下打印每个 crate 的 diff 摘要与 breaking 数量；当 breaking 数量
#         超过预算时，以非零退出并打印诊断路径，方便开发者定位。
# 执行逻辑（How, R2.1-R2.2）
#   1. 解析输入参数/环境变量并校验：确保 budget 为非负整数、base commit 存在。
#   2. 若工作树存在 tracked 变更，使用 git stash 暂存（不包含未跟踪文件），确保
#      cargo public-api diff 可以 checkout 基线 commit；通过 trap 保证异常时仍会恢复。
#   3. 针对每个待检查 crate：
#        a. 执行 cargo public-api -p <crate> diff <base>..HEAD，输出写入临时文件。
#        b. 解析“Removed items”“Changed items”区块中以 '-' 开头的条目数，统计
#           breaking 变更数量（其中 Changed 区块的 '-' 行对应旧签名，视为 breaking）。
#        c. 打印摘要并判断是否超出预算。
#   4. 汇总所有 crate 的 breaking 变更；只要任一 crate 超出预算即退出 1。
# 设计考量与权衡（Trade-offs, R4.1-R4.3）
#   * 采用 git stash（仅针对 tracked）而非 --force，避免 cargo public-api 擅自覆盖
#     开发者工作区；代价是需要 trap 维护状态，脚本复杂度略升高。
#   * 解析文本输出而非 JSON，是因为 cargo public-api 当前版本提供文本级 diff，
#     通过统计 '-' 行即可可靠识别 breaking 数量；若后续引入 JSON 输出，可在此
#     将解析层替换为 jq 方案。
#   * 当前仅覆盖 spark-core，后续可通过 PUBLIC_API_DIFF_PACKAGES 扩展更多 crate；
#     预算策略也可在 CI 配置中调整。脚本未直接与 PR 模板耦合，保持模块化。
# 可维护性提示（Readability, R5.1-R5.2）
#   * 关键函数采用清晰命名（例如 parse_breaking_count），并使用多行注释解释契约，
#     便于后续维护者快速定位逻辑。
#   * 所有临时文件使用 mktemp 创建，并在 trap 中清理，防止污染仓库。
# -----------------------------------------------------------------------------

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

# 教案级注释：日志辅助函数的意图与契约说明。
# -----------------------------------------------------------------------------
# Why (R1):
#   * log_info/log_error 统一输出格式，便于 CI 收集；避免直接 echo 导致可读性差。
# How (R2):
#   * 使用 printf 确保字符串中包含 % 时不会被解释；统一输出到 stderr，既避免与命令
#     替换冲突，又能在 CI 日志中集中展示信息。
# What (R3):
#   * 入参为消息文本，无返回值；调用前提是脚本已设定 pipefail，确保日志不吞错。
# Readability (R5):
#   * 统一使用前缀 [public-api-diff]，方便 grep。
# -----------------------------------------------------------------------------
log_info() {
  printf '[public-api-diff][INFO] %s\n' "$1" >&2
}

log_error() {
  printf '[public-api-diff][ERROR] %s\n' "$1" >&2
}

# 教案级注释：输入解析模块，定义契约与安全校验。
# -----------------------------------------------------------------------------
# Why (R1):
#   * parse_inputs 将环境变量与命令行参数统一解析，确保脚本在 CI/本地均可配置。
# How (R2):
#   * 采用参数优先级：命令行 > 环境变量 > 默认值；对 budget 采用正则校验。
# What (R3):
#   * 输出：设置全局只读变量 BASE_COMMIT、BREAKING_BUDGET、PACKAGES。
#   * 前置条件：git 仓库存在，且 BASE_COMMIT 可通过 git rev-parse 解析。
# Trade-offs (R4):
#   * 使用 bash 正则 [[ ]] 完成整数校验，避免引入外部依赖；若未来需要浮点预算，
#     可调整此处逻辑。
# -----------------------------------------------------------------------------
parse_inputs() {
  local base_from_env budget_from_env packages_from_env
  base_from_env="${PUBLIC_API_DIFF_BASE:-}"
  budget_from_env="${PUBLIC_API_BREAKING_BUDGET:-}"
  packages_from_env="${PUBLIC_API_DIFF_PACKAGES:-}"

  local base_arg="${1:-}" budget_arg="${2:-}"

  local default_base="origin/main"
  local fallback_base="main"
  local base_commit="${base_arg:-${base_from_env:-${default_base}}}"
  local breaking_budget="${budget_arg:-${budget_from_env:-0}}"
  local packages_raw="${packages_from_env:-spark-core}"

  if ! git -C "${REPO_ROOT}" rev-parse --quiet --verify "${base_commit}^{commit}" >/dev/null; then
    if [[ "${base_commit}" == "${default_base}" ]] \
      && git -C "${REPO_ROOT}" rev-parse --quiet --verify "${fallback_base}^{commit}" >/dev/null; then
      log_info "未检测到 ${default_base}，自动回退使用 '${fallback_base}' 作为基线。"
      base_commit="${fallback_base}"
    elif git -C "${REPO_ROOT}" rev-parse --quiet --verify 'HEAD^\{commit\}' >/dev/null; then
      log_info "无法解析 '${base_commit}'，改用 HEAD^ 作为临时基线。"
      base_commit="HEAD^"
    else
      log_info "无法解析 '${base_commit}'，改用 HEAD 作为临时基线（仅比较当前提交）。"
      base_commit="HEAD"
    fi
  fi

  if [[ ! "${breaking_budget}" =~ ^[0-9]+$ ]]; then
    log_error "Breaking 预算必须为非负整数，当前值: '${breaking_budget}'。"
    exit 2
  fi

  readonly BASE_COMMIT="${base_commit}"
  readonly BREAKING_BUDGET="${breaking_budget}"
  # shellcheck disable=SC2206  # 需要按照空白分割为数组
  readonly PACKAGES=( ${packages_raw} )
}

# 教案级注释：工作区暂存与恢复逻辑的设计动机与契约。
# -----------------------------------------------------------------------------
# Why (R1):
#   * cargo public-api diff 在 diff commit 场景会尝试 git checkout 基线；若当前工作区
#     存在 tracked 修改将失败。auto_stash_if_needed 确保脚本可在开发者未提交时运行。
# How (R2):
#   * 检测 git status --porcelain 输出中除 '??' 之外的行；若存在则执行 git stash push，
#     并记录 stash 标识供后续恢复；通过 trap 自动调用 restore_git_state。
# What (R3):
#   * 前置条件：当前目录属于 git 仓库，且 stash 操作不会失败（若失败则直接退出）。
#   * 后置条件：
#       - 若创建 stash，restore_git_state 会在脚本结束时 git stash pop。
#       - 若未创建，restore_git_state 为 no-op。
# Trade-offs (R4):
#   * 仅暂存 tracked 文件，避免未跟踪文件（例如生成的诊断报告）被移除；缺点是若
#     未跟踪文件与基线路径冲突仍可能阻塞，需要开发者自行处理。
# -----------------------------------------------------------------------------
readonly STASH_MESSAGE_PREFIX="public-api-diff-budget"
STASH_REF=""

auto_stash_if_needed() {
  local status_output
  status_output="$(git -C "${REPO_ROOT}" status --porcelain)"
  if [[ -n "${status_output}" ]]; then
    local needs_stash="false"
    while IFS= read -r line; do
      if [[ -n "${line}" && "${line}" != ??* ]]; then
        needs_stash="true"
        break
      fi
    done <<< "${status_output}"

    if [[ "${needs_stash}" == "true" ]]; then
      local timestamp
      timestamp="$(date +%s)"
      local message
      message="${STASH_MESSAGE_PREFIX}-${timestamp}"
      if git -C "${REPO_ROOT}" stash push --message "${message}" >/dev/null; then
        STASH_REF="stash^{/${message}}"
        log_info "检测到未提交的 tracked 变更，已暂存至 ${STASH_REF}。脚本结束后会自动恢复。"
      else
        log_error "git stash push 失败，无法安全执行 diff。请手动清理工作区。"
        exit 2
      fi
    fi
  fi
}

restore_git_state() {
  if [[ -n "${STASH_REF}" ]]; then
    log_info "恢复先前暂存的工作区变更 (${STASH_REF})。"
    if ! git -C "${REPO_ROOT}" stash pop >/dev/null; then
      log_error "git stash pop 失败，请手动执行 'git stash pop'."
      exit 2
    fi
  fi
}

trap restore_git_state EXIT

# 教案级注释：解析 cargo public-api diff 输出的核心函数。
# -----------------------------------------------------------------------------
# Why (R1):
#   * parse_breaking_count 将文本 diff 映射为 breaking 变更数量，是预算判断的关键。
# How (R2):
#   * 通过 awk 状态机遍历输出：
#       1. 识别“Removed items”“Changed items”区块。
#       2. 在对应区块统计以 '-' 开头的行数。
#       3. 忽略 "(none)"、空行与 '+' 行，确保 changed 中只计旧签名。
#   * 伪代码：
#       state <- none; count <- 0
#       for line in file:
#         if heading -> 更新 state
#         else if state in {removed, changed} and line startswith '-' -> count++
#   * 返回值通过 echo 输出，供调用方捕获。
# What (R3):
#   * 入参：输出文件路径（必须可读）。无副作用。
#   * 前置条件：文件内容来自 cargo public-api diff --color=never。
#   * 后置条件：函数返回非负整数，表示 breaking 数量。
# Trade-offs (R4):
#   * 使用 awk 而非 sed+wc 组合，以便未来扩展更多区块；缺点是稍微增加学习成本。
#   * 假定 cargo public-api 的输出结构稳定；若上游格式调整，需要同步更新模式匹配。
# -----------------------------------------------------------------------------
parse_breaking_count() {
  local diff_file="$1"
  if [[ ! -r "${diff_file}" ]]; then
    log_error "无法读取 diff 输出文件: ${diff_file}"
    exit 2
  fi

  awk '
    BEGIN { state="none"; count=0; }
    /^Removed items from the public API$/ { state="removed"; next }
    /^Changed items in the public API$/ { state="changed"; next }
    /^Added items to the public API$/ { state="added"; next }
    /^$/ { next }
    state == "removed" || state == "changed" {
      if ($0 ~ /^-/) { count++ }
      next
    }
    END { printf "%d", count }
  ' "${diff_file}"
}

# 教案级注释：单 crate diff 执行函数。
# -----------------------------------------------------------------------------
# Why (R1):
#   * run_diff_for_package 封装重复操作，保持主流程简洁；同时打印诊断信息，帮助
#     Reviewers 在 CI 日志中快速定位问题。
# How (R2):
#   * 步骤：
#       1. 构造临时输出文件。
#       2. 运行 cargo public-api diff BASE_COMMIT..HEAD，强制 --color=never 以简化解析。
#       3. 若命令失败，打印日志并退出。
#       4. 调用 parse_breaking_count 统计 breaking 数量，并打印概要。
#   * 伪代码见注释区。
# What (R3):
#   * 入参：crate 名称。
#   * 返回：echo breaking 数量，供调用方合并统计。
#   * 前置条件：当前目录在仓库根目录；cargo public-api 可用。
#   * 后置条件：临时文件会在函数末尾删除。
# Trade-offs (R4):
#   * 在命令失败时直接退出，避免继续处理其它 crate 造成级联错误；
#     若未来需要“最佳努力模式”，可在此处调整为记录错误后继续。
# -----------------------------------------------------------------------------
run_diff_for_package() {
  local package_name="$1"
  local diff_output
  diff_output="$(mktemp)"

  log_info "正在比较 crate '${package_name}' 的公共 API：${BASE_COMMIT}..HEAD"
  if ! cargo public-api \
    --color never \
    --manifest-path "${REPO_ROOT}/Cargo.toml" \
    -p "${package_name}" \
    diff "${BASE_COMMIT}..HEAD" \
    >"${diff_output}"; then
    log_error "cargo public-api diff 执行失败，请检查输出日志（保存在 ${diff_output}）。"
    log_error "若为构建失败，可在本地手动执行命令定位原因。"
    exit 1
  fi

  local breaking_count
  breaking_count="$(parse_breaking_count "${diff_output}")"
  log_info "crate '${package_name}' 检测到 ${breaking_count} 个 breaking 变更 (预算 ${BREAKING_BUDGET})。"

  rm -f -- "${diff_output}"
  printf '%s' "${breaking_count}"
}

main() {
  cd -- "${REPO_ROOT}"

  if ! command -v cargo-public-api >/dev/null; then
    log_error "未检测到 cargo-public-api 可执行文件，请先运行 'cargo install cargo-public-api'。"
    exit 127
  fi

  parse_inputs "$@"
  auto_stash_if_needed

  local total_breaking=0
  local breaking_count
  for pkg in "${PACKAGES[@]}"; do
    breaking_count="$(run_diff_for_package "${pkg}")"
    total_breaking=$(( total_breaking + breaking_count ))
    if (( breaking_count > BREAKING_BUDGET )); then
      log_error "crate '${pkg}' 的 breaking 变更数 (${breaking_count}) 超出预算 (${BREAKING_BUDGET})。"
      exit 1
    fi
  done

  log_info "所有 crate 的 breaking 变更合计 ${total_breaking}（预算 ${BREAKING_BUDGET}）。"
  log_info "公共 API diff 预算检查通过。"
}

main "$@"
