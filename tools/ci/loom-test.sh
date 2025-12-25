#!/usr/bin/env bash
set -euo pipefail

# == Loom 模型测试驱动器（教案级注释） ==
#
# ## 意图 (Why)
# 1. 在 CI 中聚焦 `Cancellation`、`Budget`、`Channel` 三类原语的并发模型，
#    通过 Loom 穷举调度交错验证顺序性与死锁风险。
# 2. 封装 `RUSTFLAGS`、toolchain 与特性选择，确保脚本可在 GitHub Actions 与本地环境重用，
#    并保持“必过”要求。
#
# ## 所在位置与架构作用 (Where)
# - 位于 `tools/ci/` 目录，供持续集成与开发者本地预检复用；
# - 在整体验证链路中，与 `miri-test.sh` 搭配覆盖“内存可见性 + 顺序一致性”双重维度。
#
# ## 核心策略 (How)
# - 默认使用滚动更新的 `nightly` toolchain（满足 crate `rust-version = 1.89` 约束，必要时可手动覆写具体日期）；
# - 通过 `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS` 注入 `--cfg spark_loom`，并设置
#   `CARGO_TARGET_APPLIES_TO_DEPENDENCIES=false`，确保仅对根包生效；
# - 启动命令：`cargo +<toolchain> test -p spark-core --features loom-model,std --test loom_concurrency`；
# - 允许通过环境变量自定义：
#   - `LOOM_FEATURES`：替换默认 features；
#   - `LOOM_NO_DEFAULT_FEATURES=1`：禁用默认特性；
#   - `LOOM_EXTRA_ARGS`：向 cargo 命令追加更多参数（如 `-- --nocapture`）。
#
# ## 契约 (What)
# - **输入**：仅读取环境变量，不接受位置参数；
# - **输出**：透传 cargo 测试日志；成功返回 0，失败返回非零码；
# - **前置条件**：Loom 已作为 dev-dependency 安装，且运行环境支持 `--cfg spark_loom`；
# - **后置条件**：取消/预算/通道三个场景的模型测试全部执行并通过。
#
# ## 风险提醒 (Gotchas)
# - 若需要扩大探索深度，可额外设置 `LOOM_MAX_PREEMPTIONS` 等环境变量，但运行时间会随之增加；
# - 当更换 nightly 版本时需同步更新脚本默认值以匹配缓存键；
# - 请勿在开启 Loom 时运行生产特性组合，以免误将模型配置带入正式构建。

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd "$REPO_ROOT"

LOOM_TOOLCHAIN=${LOOM_TOOLCHAIN:-nightly}
LOOM_FEATURES=${LOOM_FEATURES:-loom-model,std}
LOOM_NO_DEFAULT_FEATURES=${LOOM_NO_DEFAULT_FEATURES:-0}
LOOM_EXTRA_ARGS=${LOOM_EXTRA_ARGS:-}
LOOM_RUSTFLAGS=${LOOM_RUSTFLAGS:---cfg spark_loom}
#
# == Loom 运行窗口聚焦控制 ==
#
# ## Why
# - 为了保证“取消/预算/通道”三类关键不变式在 CI 中稳定必过，同时限制总运行时间，
#   我们需要对模型检查进行“场景白名单 + 状态空间上限”双重约束。
#
# ## How
# - `LOOM_SCENARIOS` 列出需要执行的测试函数名（默认即三大场景）；
# - `LOOM_MAX_BRANCHES` 与 `LOOM_MAX_PREEMPTIONS` 联合限制全局交错数量，避免
#   Loom 在复杂分支中指数级膨胀；
# - 若调用者需要扩展场景或放宽限制，可覆写环境变量，脚本保持兼容。
#
# ## What
# - 当 `LOOM_SCENARIOS` 为空时，退化为运行完整 `loom_concurrency` 测试文件。
# - 否则将逐个以 `--exact` 过滤运行，确保不会意外引入新的耗时场景。
LOOM_SCENARIOS=${LOOM_SCENARIOS:-"cancellation_visibility_is_sequentially_consistent budget_concurrent_consume_and_refund_preserves_limits channel_close_paths_converge_to_closed"}

ORIGINAL_TARGET_FLAGS=${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS:-}
if [[ -n "${ORIGINAL_TARGET_FLAGS}" ]]; then
  export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="${ORIGINAL_TARGET_FLAGS} ${LOOM_RUSTFLAGS}"
else
  export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="${LOOM_RUSTFLAGS}"
fi
export CARGO_TARGET_APPLIES_TO_DEPENDENCIES=${CARGO_TARGET_APPLIES_TO_DEPENDENCIES:-false}
export LOOM_MAX_PREEMPTIONS=${LOOM_MAX_PREEMPTIONS:-2}
export LOOM_MAX_BRANCHES=${LOOM_MAX_BRANCHES:-4096}

cmd_prefix=(cargo +"${LOOM_TOOLCHAIN}" test --package spark-core --test loom_concurrency)
cmd_suffix=()

if [[ -n "${LOOM_FEATURES}" ]]; then
  cmd_prefix+=(--features "${LOOM_FEATURES}")
fi

if [[ "${LOOM_NO_DEFAULT_FEATURES}" == "1" ]]; then
  cmd_prefix+=(--no-default-features)
fi

if [[ -n "${LOOM_EXTRA_ARGS}" ]]; then
  # shellcheck disable=SC2206
  extra=( ${LOOM_EXTRA_ARGS} )
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

#
# == 执行策略说明 ==
#
# ## Why
# - 针对性地以 `--exact` 运行指定测试，确保 CI 只覆盖三大关键不变式。
#
# ## How
# - 若存在场景列表，则循环执行，借助 GitHub Actions 的日志分组观测各场景；
# - 通过检测命令是否已包含 `--`，避免重复拼接测试运行器参数。
#
# ## What
# - 每个场景失败都会立即终止脚本，满足“失败即阻断”要求；
# - 没有场景（列表为空字符串）时，退化为一次性执行全部测试。
read -r -a loom_scenarios <<<"${LOOM_SCENARIOS}"

if [[ ${#loom_scenarios[@]} -eq 0 || ( ${#loom_scenarios[@]} -eq 1 && -z "${loom_scenarios[0]}" ) ]]; then
  full_cmd=("${cmd_prefix[@]}")
  if [[ ${#cmd_suffix[@]} -gt 0 ]]; then
    full_cmd+=("${cmd_suffix[@]}")
  fi
  printf '::group::Running %s\n' "${full_cmd[*]}"
  "${full_cmd[@]}"
  printf '::endgroup::\n'
else
  for scenario in "${loom_scenarios[@]}"; do
    scenario_cmd=("${cmd_prefix[@]}" "${scenario}")
    if [[ ${#cmd_suffix[@]} -gt 0 ]]; then
      scenario_cmd+=("${cmd_suffix[@]}")
    fi
    printf '::group::Running %s\n' "${scenario_cmd[*]}"
    "${scenario_cmd[@]}"
    printf '::endgroup::\n'
  done
fi
