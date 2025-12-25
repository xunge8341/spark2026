#!/usr/bin/env bash
set -euo pipefail

# == MSRV 双重护栏脚本（教案级注释） ==
#
# ## 意图 (Why)
# - 保障仓库所有 `Cargo.toml` 的 `rust-version` 字段与项目约定的 MSRV (`1.89`) 完全一致，
#   避免因为成员 crate 忘记同步导致贡献者在更低 Rust 版本上构建失败。
# - 确保 CI 与本地默认工具链 (`rust-toolchain.toml`、GitHub Actions workflow) 均锁定在同一补丁版本 (`1.89.0`)，
#   防止“本地默认 1.89.0，但 CI 切到更新 nightly”这类割裂情况破坏重现性。
#
# ## 所在位置与架构作用 (Where)
# - 文件位于 `tools/ci/`，在 CI `lint-and-docs` Job 的最前面执行，与 `check_consistency.sh` 并列。
# - 作为 MSRV 的“文字版守门员”：
#   1. `check_consistency.sh` 约束语义一致性；
#   2. 本脚本约束编译器版本；
#   3. 后续 `make ci-*` 命令在这个前置检查全部通过后才运行。
#
# ## 核心策略 (How)
# 1. 解析 `rust-toolchain.toml` 的 `channel` 字段，要求严格等于 `1.89.0`；
# 2. 遍历仓库中所有被 Git 追踪的 `Cargo.toml`，读取 `rust-version`，要求全部存在且为 `1.89`；
# 3. 校验 CI Workflow（当前为 `.github/workflows/ci.yml`）中显式写死了 `toolchain: 1.89.0`；
# 4. 输出 Markdown 风格的错误提示，指导贡献者如何修复（更新字段或补充缺失值）。
#
# ## 契约 (What)
# - **输入**：无外部参数，默认在仓库根目录执行；
# - **输出**：
#   - 全部满足约束 → 静默退出（退出码 0）；
#   - 任一约束失败 → 打印带项目符号的错误描述，退出码 1。
# - **前置条件**：环境中存在 `git`、`rg`、`sed`（CI 镜像和开发环境默认具备）。
# - **后置条件**：失败时 CI 会立即终止后续步骤，提示贡献者修正 MSRV 设置。
#
# ## 设计考量与权衡 (Trade-offs)
# - 解析 TOML 采用 `grep + sed`，避免引入额外依赖（如 `tomlq`）。
#   虽不是完整解析器，但对于单行 `key = "value"` 足够可靠；若未来字段分多行，可切换到 Rust 小工具。
# - 工作流文件仅校验 `toolchain:` 行的字面值，未强制出现次数（容许新增 Job）；
#   若引入更多 Workflow，可在 `WORKFLOWS` 中追加路径。
#
# ## 风险提示 (Gotchas)
# - 若某个成员 crate 忘记声明 `rust-version`，脚本会报错并指明文件，需要维护者补充字段；
# - 如果未来需要升级 MSRV，必须同步更新本脚本中 `EXPECTED_*` 常量，否则 CI 会持续失败。
#
# ## 跟踪任务 / 已知限制
# - 当前脚本未检查 README 或文档中手写的 Rust 版本号；若需覆盖该场景，请参见《docs/tracking/T107.md#msrv-doc-sync》
#   中的任务，新增巡检逻辑需评估误报概率并补充对常见文档格式的兼容测试。
#
# ## 维护建议
# - 升级 MSRV 时的步骤清单：
#   1. 更新 `EXPECTED_MSRV` 与 `EXPECTED_TOOLCHAIN_CHANNEL`；
#   2. 修改 `rust-toolchain.toml`、各 `Cargo.toml`、CI Workflow 中的版本号；
#   3. 更新 `docs/ci/redline.md` 等宣告文档。
#
EXPECTED_MSRV="1.89"
EXPECTED_TOOLCHAIN_CHANNEL="1.89.0"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd "$REPO_ROOT"

failures=()

# -- 检查 rust-toolchain.toml --
TOOLCHAIN_FILE="${REPO_ROOT}/rust-toolchain.toml"
if [[ -f "$TOOLCHAIN_FILE" ]]; then
    actual_channel=$(sed -nE 's/^\s*channel\s*=\s*"([^"]+)"\s*$/\1/p' "$TOOLCHAIN_FILE" | head -n1)
    if [[ -z "$actual_channel" ]]; then
        failures+=("- \`rust-toolchain.toml\` 缺少 \`channel\` 定义，请写成 \`channel = \"${EXPECTED_TOOLCHAIN_CHANNEL}\"\`。")
    elif [[ "$actual_channel" != "$EXPECTED_TOOLCHAIN_CHANNEL" ]]; then
        failures+=("- \`rust-toolchain.toml\` 当前声明为 \`${actual_channel}\`，应更新为 \`${EXPECTED_TOOLCHAIN_CHANNEL}\` 以匹配 MSRV。")
    fi
else
    failures+=("- 未找到 \`rust-toolchain.toml\`，无法确保本地默认工具链锁定在 ${EXPECTED_TOOLCHAIN_CHANNEL}。")
fi

# -- 检查每个 Cargo.toml 的 rust-version --
while IFS= read -r manifest; do
    msrv_line=$(sed -nE 's/^\s*rust-version\s*=\s*"([^"]+)"\s*$/\1/p' "$manifest" | head -n1)
    if [[ -z "$msrv_line" ]]; then
        failures+=("- \`${manifest}\` 缺少 \`rust-version\` 字段，请添加 \`rust-version = \"${EXPECTED_MSRV}\"\`。")
    elif [[ "$msrv_line" != "$EXPECTED_MSRV" ]]; then
        failures+=("- \`${manifest}\` 中的 \`rust-version\` 为 \`${msrv_line}\`，应改为 \`${EXPECTED_MSRV}\` 与 MSRV 对齐。")
    fi
done < <(git ls-files '*Cargo.toml')

# -- 校验 CI Workflow 固定工具链版本 --
WORKFLOWS=(".github/workflows/ci.yml")
for workflow in "${WORKFLOWS[@]}"; do
    if [[ -f "$workflow" ]]; then
        if ! rg -q "toolchain:\s*${EXPECTED_TOOLCHAIN_CHANNEL}" "$workflow"; then
            failures+=("- \`${workflow}\` 未显式锁定 \`toolchain: ${EXPECTED_TOOLCHAIN_CHANNEL}\`，请同步更新 CI。")
        fi
    else
        failures+=("- 预期存在的 Workflow 文件 \`${workflow}\` 缺失，请检查 CI 配置。")
    fi
done

if ((${#failures[@]} > 0)); then
    printf 'MSRV 检查失败：\n' >&2
    for failure in "${failures[@]}"; do
        printf '  %s\n' "$failure" >&2
    done
    exit 1
fi

printf 'MSRV guard: 所有 rust-version 与工具链声明均已对齐 %s/%s。\n' "$EXPECTED_MSRV" "$EXPECTED_TOOLCHAIN_CHANNEL"
