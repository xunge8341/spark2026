#!/usr/bin/env bash
# 教案级注释块：工作区编译特性矩阵生成脚本
# -----------------------------------------------------------------------------
# 1. 目标与架构定位（Why，R1.1-R1.3）
#    - 目标：为 CI 动态生成 Cargo 编译特性矩阵，确保在默认、全特性以及 no_std(alloc)
#      三种关键配置下均执行构建验证，从而覆盖最常见的兼容性风险面。
#    - 架构位置：作为 CI guardrail 工作流的“准备阶段”，脚本输出 JSON 供 GitHub Actions
#      via `fromJSON` 使用，实现配置与 CI 逻辑解耦，避免在 YAML 中硬编码特性集合。
#    - 设计思想：采用“静态白名单 + 只读校验”模式——脚本内置矩阵定义，且会检测
#      根 `Cargo.toml` 是否被修改；若成员或特性矩阵发生调整，开发者需同时更新脚本，
#      形成显式审查流程。
# 2. 契约定义（What，R3.1-R3.3）
#    - 输入：无命令行参数；可选环境变量 `ALLOW_CARGO_TOML_CHANGES`，当值为 `1` 时跳过
#      只读校验（主要用于需要同步更新 Cargo.toml 的基线刷新场景）。
#    - 输出：向标准输出打印 JSON 数组，每个元素包含 `name`、`cargo_flags`、`summary`。
#    - 前置条件：脚本需在仓库根目录（或任一子目录）执行；依赖 git 命令检测变更。
#    - 后置条件：若检测到根 Cargo.toml 发生改动且未显式允许，将退出 3 并提示开发者
#      更新矩阵；否则返回 0。
# 3. 执行逻辑（How，R2.1-R2.2）
#    - 步骤：
#      a. 解析脚本/仓库根路径，构造默认矩阵数据结构。
#      b. 调用 `git status --short Cargo.toml` 检测是否存在工作区改动；如存在且未放行，
#         输出详细提示后退出。
#      c. 使用 here-document 构造 JSON，并保持紧凑格式以简化 GitHub Actions 消费。
#    - 实现要点：
#      * 采用 `cat <<'JSON'` 避免 shell 展开；
#      * JSON 中包含 doc 字段 summary，方便未来在 CI 日志中输出。
# 4. 设计权衡与注意事项（R4.1-R4.3）
#    - 将矩阵硬编码在脚本中，提升了可审查性，但需要在新增特性组合时同步维护；
#      相比解析 Cargo.toml 的动态方案，复杂度更低，也避免引入额外依赖。
#    - 只读校验可能在合法场景下阻塞（例如确实需要更新 workspace members），因此提供
#      环境变量逃生口；CI 默认不开启该变量以保持严格性。
#    - 若未来特性组合超过 10 个，可考虑改为读取外部 JSON 文件以提升可维护性。
# 5. 可维护性与可读性（R5.1-R5.2）
#    - 所有提示信息前缀 `[feature-matrix]`，便于在日志中检索。
#    - 注释中列出了矩阵使用方式，帮助后续维护者理解其与 GitHub Actions 的衔接关系。
# -----------------------------------------------------------------------------

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

log() {
  printf '[feature-matrix] %s\n' "$1" >&2
}

check_cargo_toml_readonly() {
  if [[ "${ALLOW_CARGO_TOML_CHANGES:-0}" == "1" ]]; then
    log "检测到环境变量 ALLOW_CARGO_TOML_CHANGES=1，跳过 Cargo.toml 只读校验。"
    return
  fi

  if git -C "${REPO_ROOT}" status --short --untracked-files=no Cargo.toml | grep -q '.'; then
    log "根 Cargo.toml 存在未提交改动。若此次 PR 合理修改了 workspace 配置，请同步更新" \
      "tools/ci/feature_combos.sh 并设置 ALLOW_CARGO_TOML_CHANGES=1。"
    exit 3
  fi
}

emit_matrix_json() {
  cat <<'JSON'
[
  {
    "name": "default",
    "cargo_flags": "",
    "summary": "默认特性集（std）。"
  },
  {
    "name": "all-features",
    "cargo_flags": "--all-features",
    "summary": "开启 workspace 中声明的全部特性，验证特性交叉影响。"
  },
  {
    "name": "no-default-alloc",
    "cargo_flags": "--no-default-features --features alloc",
    "summary": "no_std + alloc 基线，确保核心组件在无 std 场景可编译。"
  },
  {
    "name": "core-no-std-min",
    "cargo_flags": "--no-default-features",
    "summary": "核心最小化：禁用全部 Feature，验证 spark-core 在仅有 alloc 支持时可通过检查。"
  }
]
JSON
}

main() {
  check_cargo_toml_readonly
  emit_matrix_json
}

main "$@"

