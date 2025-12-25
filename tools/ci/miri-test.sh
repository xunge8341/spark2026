#!/usr/bin/env bash
set -euo pipefail

# == 并发原语 Miri 扫描器（教案级注释） ==
#
# ## 意图 (Why)
# 1. 聚焦 `Cancellation`/`Budget`/`Channel` 三类原语的并发单元测试，
#    使用 Miri 抽样执行 `concurrency_primitives` 测试二进制，验证核心原子序语义。
# 2. 封装 toolchain、参数与准备步骤，避免在多个工作流/本地脚本中复制 `cargo miri setup`
#    等样板代码，降低维护成本并确保 CI 必过。
#
# ## 所在位置与架构作用 (Where)
# - 位于 `tools/ci/` 目录，供 GitHub Actions 与本地开发者复用；
# - 在整体 CI 拓扑中，它属于“内存与数据竞争审计”链路的第一环，
#   专注取消/预算/通道三类原语的 UB 排查。
#
# ## 核心策略 (How)
# - 默认使用滚动更新的 `nightly` toolchain（可通过环境变量覆盖），满足 crate 的 `rust-version = 1.89` 约束；
# - 若目标 toolchain 未安装 `miri` 组件，会自动通过 `rustup component add` 安装；
# - 随后运行 `cargo +<toolchain> miri setup` 以下载并配置 Miri runtime；
# - 调用 `cargo +<toolchain> miri test -p spark-core --test concurrency_primitives` 聚焦关键测试；
# - 支持通过环境变量覆写测试特性：
#   - `MIRI_FEATURES`：传入 `--features <value>`；
#   - `MIRI_NO_DEFAULT_FEATURES=1`：追加 `--no-default-features`；
#   - `MIRI_EXTRA_ARGS`：拼接到命令末尾，便于未来扩展（例如指定包或测试过滤器）。
#
# ## 契约 (What)
# - **输入参数**：脚本不接受位置参数，仅读取环境变量：
#   - `MIRI_TOOLCHAIN`（可选）：Rust toolchain 名称；
#   - `MIRI_FEATURES`（可选）：Cargo features 列表；
#   - `MIRI_NO_DEFAULT_FEATURES`（可选，设置为 `1` 生效）：禁用默认特性；
#   - `MIRI_EXTRA_ARGS`（可选）：附加到 `cargo miri test` 的参数字符串。
# - **输出**：标准输出/错误直接透传 cargo 日志；成功退出码 0，失败则返回 cargo 的非零状态。
# - **前置条件**：
#   1. 仓库已通过 `git` 管理，且脚本在仓库根目录或其子目录中执行；
#   2. 目标 toolchain 已安装 Miri 组件；
#   3. 运行环境具备 Bash、Cargo 与必要的 LLVM 依赖。
# - **后置条件**：
#   - 成功时，取消/预算/通道三类原语的 Miri 抽样测试全部通过；
#   - 失败时，CI 应立即终止后续与 UB 相关的作业，以提醒贡献者修复。
#
# ## 设计考量 (Trade-offs)
# - 采用脚本封装而非直接在工作流内写命令，便于未来在本地或其他 CI 平台复用；
# - Toolchain 默认值选取具体日期而非 `nightly` 流水号，保证可重复性；
# - 暂未并行拆分 crate，优先追求确定性与日志可读性，待测试规模扩大后再引入拆分策略。
#
# ## 风险提醒 (Gotchas)
# - 若某些测试需要特定环境变量（如网络、文件路径），请在调用脚本前设置好；
# - Miri 对 FFI/裸金属代码限制较多，如遇“不支持的操作”报错，需要在代码中添加守卫或条件编译；
# - 当 toolchain 版本升级时需同步更新默认值与缓存键，否则会触发全量重编译。

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd "$REPO_ROOT"

MIRI_TOOLCHAIN=${MIRI_TOOLCHAIN:-nightly}
MIRI_FEATURES=${MIRI_FEATURES:-}
MIRI_NO_DEFAULT_FEATURES=${MIRI_NO_DEFAULT_FEATURES:-0}
MIRI_EXTRA_ARGS=${MIRI_EXTRA_ARGS:-}
#
# == Miri 场景收敛器 ==
#
# ## Why
# - 将执行范围收敛到取消/预算/通道三大核心不变式，既满足“CI 必过”的稳定性，
#   又控制模拟执行的时间上限。
#
# ## How
# - `MIRI_SCENARIOS` 默认为三个测试函数名，脚本会逐个以 cargo filter 方式运行；
# - 若调用者需要自定义场景，可通过覆盖该环境变量实现；
# - `MIRI_EXTRA_ARGS` 支持追加 cargo/miri 参数，若包含 `--` 会自动分流到测试运行器端。
#
# ## What
# - 当列表为空时，退化为执行整个 `concurrency_primitives` 测试文件；
# - 每个场景失败立即中断，保证“失败即阻断”。
MIRI_SCENARIOS=${MIRI_SCENARIOS:-"cancellation_cross_thread_visibility budget_concurrent_consume_refund_invariant channel_close_sequences_eventually_closed"}

rustup component add --toolchain "${MIRI_TOOLCHAIN}" miri >/dev/null
cargo +"${MIRI_TOOLCHAIN}" miri setup

cmd_prefix=(cargo +"${MIRI_TOOLCHAIN}" miri test --package spark-core --test concurrency_primitives)
cmd_suffix=()

if [[ -n "${MIRI_FEATURES}" ]]; then
  cmd_prefix+=(--features "${MIRI_FEATURES}")
fi

if [[ "${MIRI_NO_DEFAULT_FEATURES}" == "1" ]]; then
  cmd_prefix+=(--no-default-features)
fi

if [[ -n "${MIRI_EXTRA_ARGS}" ]]; then
  # shellcheck disable=SC2206
  extra=( ${MIRI_EXTRA_ARGS} )
  pass_to_suffix=0
  for arg in "${extra[@]}"; do
    if [[ $pass_to_suffix -eq 0 && "$arg" == "--" ]]; then
      pass_to_suffix=1
      cmd_suffix+=("--")
      continue
    fi

    if [[ $pass_to_suffix -eq 1 ]]; then
      cmd_suffix+=("${arg}")
    else
      cmd_prefix+=("${arg}")
    fi
  done
fi

read -r -a miri_scenarios <<<"${MIRI_SCENARIOS}"

if [[ ${#miri_scenarios[@]} -eq 0 || ( ${#miri_scenarios[@]} -eq 1 && -z "${miri_scenarios[0]}" ) ]]; then
  full_cmd=("${cmd_prefix[@]}")
  if [[ ${#cmd_suffix[@]} -gt 0 ]]; then
    full_cmd+=("${cmd_suffix[@]}")
  fi
  printf '::group::Running %s\n' "${full_cmd[*]}"
  "${full_cmd[@]}"
  printf '::endgroup::\n'
else
  for scenario in "${miri_scenarios[@]}"; do
    scenario_cmd=("${cmd_prefix[@]}" "${scenario}")
    if [[ ${#cmd_suffix[@]} -gt 0 ]]; then
      scenario_cmd+=("${cmd_suffix[@]}")
    fi
    printf '::group::Running %s\n' "${scenario_cmd[*]}"
    "${scenario_cmd[@]}"
    printf '::endgroup::\n'
  done
fi
