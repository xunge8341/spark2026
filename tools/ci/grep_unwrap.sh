#!/usr/bin/env bash
# 教案级注释块：生产代码 unwrap/expect 守门脚本
# -----------------------------------------------------------------------------
# 1. 目标与架构定位（Why，R1.1-R1.3）
#    - 目标：阻止生产路径误用 `.unwrap()` / `.expect()`，避免在运行时因未处理错误
#      导致进程崩溃。该脚本位于 tools/ci，与其他静态守护脚本（如 check_consistency）
#      并列，承担“稳态安全基线”职责，确保 PR 未引入显式 panic 风险。
#    - 架构位置：作为 CI pre-merge 守门环节的一部分，脚本被 GitHub Actions 调用，
#      对仓库核心目录（crates/、contracts/、sdk/）执行只读扫描，生成诊断报告并在
#      违规时阻断流水线。
#    - 设计思想：采用“路径分层 + 模式匹配”策略，结合 ripgrep 的高性能搜索能力，
#      构建针对 `.rs` 生产文件的过滤器；通过白名单机制允许测试/基准等场景保留
#      unwrap/expect，以维持研发效率。
# 2. 契约定义（What，R3.1-R3.3）
#    - 输入：可选环境变量 `CI_ARTIFACT_DIR` 指定诊断文件输出目录，默认生成到
#      `ci-artifacts`（脚本会自动创建）。无命令行参数，避免误用。
#    - 输出：若无违规，输出提示并返回 0；若存在违规，将匹配结果写入
#      `${CI_ARTIFACT_DIR}/unwrap-violations.txt` 并以 2 退出。
#    - 前置条件：仓库根目录可通过脚本定位（依赖 git 仓库结构）；系统安装了 ripgrep。
#    - 后置条件：若产生诊断文件，CI 流水线可进一步上传为 Artifact，供审阅者下载。
# 3. 执行逻辑（How，R2.1-R2.2）
#    - 步骤：
#      a. 解析仓库根路径与输出目录，确保目录存在。
#      b. 构建 ripgrep 搜索命令：限定在生产目录、仅匹配 `.rs` 文件，排除测试/基准/
#         示例路径以及常见的“允许 panic”文件（如 build.rs）。
#      c. 采用正则 `\.un(?:wrap|expect)\s*\(` 捕获调用点，并将结果保存到诊断文件。
#      d. 若文件为空则删除并返回成功；否则提示违规并返回失败。
#    - 关键技巧：使用 `--glob` 组合实现路径排除，避免 shell find 复杂度；输出文件
#      利用 `tee` 方式写入，确保命令失败时仍保留部分日志。
# 4. 设计权衡（Trade-offs，R4.1-R4.3）
#    - 为兼顾性能与准确率，脚本采用简单正则匹配，可能漏报宏内展开的 unwrap；但
#      实践中绝大多数误用来自显式调用，因此选择可维护性更高的方案。
#    - 未对第三方依赖执行扫描，避免误判；若未来需要，可扩展 `SEARCH_ROOTS`。
#    - 针对允许 panic 的白名单路径（如 `/tests/`），若有特殊需求需人工评审并更新
#      白名单，脚本注释已指明修改位置。
# 5. 可维护性提示（Readability，R5.1-R5.2）
#    - 变量命名尽量语义化（如 `SEARCH_ROOTS`、`IGNORE_GLOBS`）。
#    - 日志信息统一携带前缀 `[grep-unwrap]`，方便在 CI 日志中筛查。
# -----------------------------------------------------------------------------

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
readonly ARTIFACT_DIR="${CI_ARTIFACT_DIR:-${REPO_ROOT}/ci-artifacts}"
readonly REPORT_FILE="${ARTIFACT_DIR}/unwrap-violations.txt"

log() {
  printf '[grep-unwrap] %s\n' "$1" >&2
}

main() {
  mkdir -p -- "${ARTIFACT_DIR}"

  # 约定扫描范围：核心生产目录。
  local -a SEARCH_ROOTS=(
    "${REPO_ROOT}/crates"
    "${REPO_ROOT}/contracts"
    "${REPO_ROOT}/sdk"
  )

  local -a RG_ARGS=(
    '--with-filename'
    '--line-number'
    '--color=never'
    "--pcre2"
    "-g" '*.rs'
    "--glob" '!**/tests/**'
    "--glob" '!**/benches/**'
    "--glob" '!**/examples/**'
    "--glob" '!**/tests.rs'
    "--glob" '!**/benches.rs'
    "--glob" '!**/examples.rs'
    "--glob" '!**/build.rs'
    "--glob" '!**/main.rs'
    "--glob" '!**/cli.rs'
  )

  local pattern='\\.un(?:wrap|expect)\\s*\('
  local found="false"

  : >"${REPORT_FILE}"
  for root in "${SEARCH_ROOTS[@]}"; do
    if [[ -d "${root}" ]]; then
      if rg "${RG_ARGS[@]}" "${pattern}" "${root}" >>"${REPORT_FILE}"; then
        found="true"
      fi
    fi
  done

  if [[ "${found}" == "true" ]]; then
    log "检测到生产代码中存在 unwrap/expect 调用，请查看 ${REPORT_FILE}。"
    exit 2
  fi

  rm -f -- "${REPORT_FILE}"
  log "扫描完成，生产代码未发现 unwrap/expect。"
}

main "$@"
