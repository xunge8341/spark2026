#!/usr/bin/env bash
set -euo pipefail

# == 工作区 Sanitizer 执行器（教案级注释） ==
#
# ## 意图 (Why)
# 1. 使用 LLVM Sanitizer (ASan/TSan) 对工作区内的 Rust 测试进行动态检测，
#    发现编译阶段无法覆盖的内存错误与数据竞争问题；
# 2. 将复杂的环境配置（RUSTFLAGS、toolchain、内存限制）封装在脚本中，
#    让 CI 与本地执行共享一套安全可靠的命令行入口。
#
# ## 所在位置与架构作用 (Where)
# - 与 `miri-test.sh` 同属 `tools/ci/` 工具集，是“内存与数据竞争审计”流程的第二环；
# - 脚本由 GitHub Actions 的矩阵任务调用，针对不同 sanitizer 模式重复使用。
#
# ## 核心策略 (How)
# - 读取 `SANITIZER` 环境变量（必填）决定运行模式：`address` 或 `thread`；
# - 默认使用 `nightly-2024-12-31` toolchain（可覆盖），确保 `-Z sanitizer` 支持稳定；
# - 设置 `RUSTFLAGS="-Zsanitizer=<mode>"` 与 `RUSTDOCFLAGS`，避免构建依赖不一致；
# - 通过 `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="sudo -E"` 保障 sanitizer 所需的
#   `LD_PRELOAD` 环境透传；该值可被调用方覆写以适配非容器化环境；
# - 执行 `cargo +<toolchain> test --workspace`，并允许外界通过：
#   - `SANITIZER_FEATURES` 指定额外 `--features`；
#   - `SANITIZER_NO_DEFAULT_FEATURES=1` 禁用默认特性；
#   - `SANITIZER_EXTRA_ARGS` 注入任意附加参数（如 `-p crate-name`）。
#
# ## 契约 (What)
# - **输入环境变量**：
#   - `SANITIZER`（必填）：`address` 或 `thread`；
#   - `SANITIZER_TOOLCHAIN`（可选）：Rust toolchain 名称；
#   - `SANITIZER_FEATURES`、`SANITIZER_NO_DEFAULT_FEATURES`、`SANITIZER_EXTRA_ARGS`（可选）；
# - **输出**：标准输出/错误直传 Cargo 日志；
# - **前置条件**：
#   1. 容器/主机安装了支持 sanitizer 的 libc 以及 `llvm-symbolizer`；
#   2. 若以非 root 身份运行，需要具备 `ulimit -s unlimited` 等权限以避免栈溢出；
#   3. `cargo` 与目标 toolchain 已安装。
# - **后置条件**：
#   - 成功返回 0；失败时保留 sanitizer 报告，供 CI 断言和开发者定位问题。
#
# ## 设计考量与权衡 (Trade-offs)
# - 使用脚本集中管理 RUSTFLAGS，可防止工作流 YAML 中因字符串转义造成的错误；
# - 未对 `address` 与 `thread` 做并行执行，优先保证日志顺序性；
# - 默认不启用 `leak` sanitizer，避免测试中大量的“预期泄漏”导致噪音；后续若需要可扩展。
#
# ## 风险提醒 (Gotchas)
# - Sanitizer 构建会显著增加编译与执行时间，请在 CI 中使用缓存；
# - 某些第三方依赖在 `no_std` 或特定 CPU 架构下可能不支持 sanitizer，若出现链接错误需
#   通过条件编译规避；
# - `RUSTFLAGS` 会覆盖调用者设置，如需合并请在外部先拼接变量后再调用脚本。

if [[ -z "${SANITIZER:-}" ]]; then
  echo "[sanitizers] Missing SANITIZER environment variable (expected 'address' or 'thread')." >&2
  exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd "$REPO_ROOT"

SANITIZER_TOOLCHAIN=${SANITIZER_TOOLCHAIN:-nightly-2024-12-31}
SANITIZER_FEATURES=${SANITIZER_FEATURES:-}
SANITIZER_NO_DEFAULT_FEATURES=${SANITIZER_NO_DEFAULT_FEATURES:-0}
SANITIZER_EXTRA_ARGS=${SANITIZER_EXTRA_ARGS:-}

export RUSTFLAGS="-Zsanitizer=${SANITIZER}"
export RUSTDOCFLAGS="-Zsanitizer=${SANITIZER}"

if [[ -z "${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER:-}" ]]; then
  export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="sudo -E"
else
  export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER
fi

cmd=(cargo +"${SANITIZER_TOOLCHAIN}" test --workspace)

if [[ -n "${SANITIZER_FEATURES}" ]]; then
  cmd+=(--features "${SANITIZER_FEATURES}")
fi

if [[ "${SANITIZER_NO_DEFAULT_FEATURES}" == "1" ]]; then
  cmd+=(--no-default-features)
fi

if [[ -n "${SANITIZER_EXTRA_ARGS}" ]]; then
  # shellcheck disable=SC2206
  extra=( ${SANITIZER_EXTRA_ARGS} )
  cmd+=("${extra[@]}")
fi

printf '::group::Running %s with RUSTFLAGS=%s\n' "${cmd[*]}" "${RUSTFLAGS}"
"${cmd[@]}"
printf '::endgroup::\n'
