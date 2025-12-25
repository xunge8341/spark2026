#!/usr/bin/env bash
set -euo pipefail

# 教案级注释：脚本整体目标与背景
# 1. 设计意图 (Why)
#    - Spark Core 的架构原则要求“契约层不携带具体运行时假设”，避免核心 crate 与 tokio/async-std/smol/glommio 等执行框架产生硬编码耦合。
#    - 本脚本作为 CI 守门人，自动化扫描 `cargo tree` 输出，确保任意特性组合下都没有直接引入上述运行时依赖。
# 2. 使用场景 (What)
#    - 在本地或 CI 环境调用脚本，若检测到禁止的依赖，将立即退出并给出清晰错误提示；
#    - 如无违例，脚本静默完成，用于守护依赖树的纯净性。
# 3. 方法概览 (How)
#    - 进入仓库根目录后，对“std 默认特性启用”和“--no-default-features”两种模式分别执行 `cargo tree -e normal`；
#    - 通过 `rg` 正则匹配运行时 crate 名称，一旦命中即认定为违规并退出。
# 4. 边界&风险 (Trade-offs)
#    - `cargo tree` 仅分析编译期依赖，无法捕获运行时动态加载；
#    - 若未来新增其它运行时，需要同步更新 `FORBIDDEN_RUNTIME_PATTERNS`。

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

# 禁止的运行时依赖关键字，保持集中管理便于后续扩展。
FORBIDDEN_RUNTIME_PATTERNS='tokio|async-std|smol|glommio'

# teach-level 注释：函数契约与实现细节
#
# check_mode() 的职责：
# 1. 为什么 (Why)
#    - 针对给定的特性组合执行一次依赖扫描，将“运行时零依赖”的架构约束模块化封装，避免脚本主体重复逻辑。
# 2. 契约 (What)
#    - 参数：
#      * $1 —— 仅用于日志的人类可读模式名称。
#      * 其余参数 —— 透传给 `cargo tree` 的命令行选项（例如 `--no-default-features`）。
#    - 前置条件：
#      * 调用前工作目录必须位于仓库根目录，使得 `cargo tree` 可正确解析 `Cargo.toml`。
#    - 后置条件：
#      * 若 `spark-core` 在指定模式下存在禁止依赖，函数将打印错误并以非零状态退出整个脚本；
#      * 若未检测到违规，函数正常返回且不产生副作用。
# 3. 实现思路 (How)
#    - 使用 `cargo tree -p spark-core -e normal` 生成常规依赖图，确保覆盖编译时依赖；
#    - 通过管道交给 `rg` 进行正则匹配，实现高性能筛查；
#    - 将 `rg` 的结果缓存到变量中，避免直接依赖其退出码造成逻辑混淆。
# 4. 注意事项 (Gotchas)
#    - 当 `cargo tree` 输出为空时，`rg` 返回非零，此时表示无匹配，应视为成功；
#    - 一旦检测到命中，为了提供可诊断性，会把完整匹配输出打印到 stderr。
check_mode() {
    local mode_label="$1"
    shift || true

    echo "[spark-core] 校验运行时依赖：${mode_label}"

    # 执行依赖树分析并捕获匹配结果。
    local matches
    if ! matches=$(cargo tree -p spark-core -e normal "$@" | rg --color=never "${FORBIDDEN_RUNTIME_PATTERNS}" || true); then
        matches=""
    fi

    if [[ -n "${matches}" ]]; then
        cat <<EOF >&2
错误：在模式 "${mode_label}" 下检测到禁止的运行时依赖：
${matches}
请移除 spark-core 对 tokio/async-std/smol/glommio 的直接引用，或将运行时能力下沉至宿主层。
EOF
        exit 1
    fi
}

# 执行两种核心特性组合：
# 1. 默认模式（std 特性开启），代表标准构建配置；
# 2. `--no-default-features` 模式，确保裸合同配置同样保持无运行时依赖。
check_mode "默认特性 (std)"
check_mode "--no-default-features" --no-default-features

echo "[spark-core] 运行时依赖检查通过。"
