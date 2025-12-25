#!/usr/bin/env bash
set -euo pipefail

# == 一致性护栏脚本（教案级注释） ==
#
# ## 意图 (Why)
# 1. 防止历史上曾出现过的“第二套 PollReady/Backpressure Reason”定义重新混入代码库，
#    破坏跨 crate 的状态表达一致性，进而导致调用方在面对背压信号时出现歧义或重复实现。
# 2. 将守护逻辑固化在 CI 中，确保每一次提交都会自动巡检，而不是依赖人工 code review
#    记忆规则，降低漏检风险。
#
# ## 所在位置与架构作用 (Where)
# - 脚本位于 `tools/ci/`，作为 CI Job 的前置门禁，在其他构建、测试任务之前快速失败，
#   以节省资源并为贡献者提供即时反馈。
# - 该脚本与 `make ci-*` 等命令互补：`make` 主要覆盖编译/测试，当前脚本负责语义一致性。
#
# ## 核心策略 (How)
# - 使用 `ripgrep`/`git ls-files` 精准定位潜在违规：
#   1. 禁止出现新的「PollReady 枚举」定义，避免重复的就绪状态枚举。
#   2. 若检测到旧的 `Backpressure` `Reason` 关键词拼接，立即失败，保证仓库内不存在第二套背压原因实现。
#   3. 禁止新增 `backpressure*.rs` 顶层文件名（保留内部模块目录结构），防止模块命名分叉。
# - 每一项检查均返回详细的错误提示，协助开发者理解违规原因和修复建议。
#
# ## 契约 (What)
# - **输入**：无显式参数，脚本基于当前 Git 仓库状态运行，默认在 CI/本地仓库根目录执行。
# - **输出**：
#   - 成功：退出码 0，且无输出；
#   - 失败：标准错误输出违规明细，退出码非 0。
# - **前置条件**：
#   - 仓库必须初始化 Git，且已安装 `git`、`rg`；
#   - 需要在 Unix shell 环境下运行（使用 Bash）。
# - **后置条件**：若发现违规，脚本立即通过非零退出码阻断后续 CI 步骤。
#
# ## 设计考量与权衡 (Trade-offs)
# - 直接使用正则匹配可以快速覆盖大部分情况，但如果未来出现宏生成代码，需要额外 guard。
# - 为彻底移除旧实现，我们直接禁止任何形式的 `Backpressure` + `Reason` 字面量出现，
#   一旦迁移完毕无需再维护可见性 allowlist，降低脚本复杂度。
# - 文件名检查采用 allowlist（允许位于目录 `*/backpressure/` 内的内部模块），
#   可以与现有结构兼容，但如需重构文件布局需调整 allowlist。
#
# ## 风险提醒 (Gotchas)
# - 若开发者在未安装 `ripgrep` 的环境运行脚本，将收到缺少命令的错误；CI 镜像已预装。
# - 由于使用 `git ls-files`，未跟踪文件不会被检测到；提交前请确保已 `git add`。

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd "$REPO_ROOT"

violation_count=0

## 变更集收集：用于跨检查共享文件触达信息
# - **意图 (Why)**：多条护栏需要知道“本次改动是否触及状态机/泛型层”等上下文。
#   若每条检查重复计算 `git diff`，不仅浪费时间，也可能因为 diff 基准不一致而产生误报。
# - **所在位置 (Where)**：全局初始化阶段，紧邻 `violation_count`，供后续函数引用。
# - **执行策略 (How)**：
#   1. 通过 `determine_diff_base` 推断“对比基准”，优先使用 CI 传入的 PR 基线；
#   2. 结合 `git diff <base>..HEAD`（覆盖 PR 内提交）与 `git diff HEAD`（覆盖当前工作区未提交改动）；
#   3. 使用集合去重，得到一次执行中稳定的“已变更文件列表”。
# - **契约 (What)**：
#   - 输出：全局数组 `changed_files`，其中元素为仓库根目录下的相对路径；
#   - 前置条件：仓库处于 Git 环境；
#   - 后置条件：即便无法推断基准（例如孤立分支），函数也会尽力回退到本地工作区 diff，避免硬失败。
# - **设计权衡 (Trade-offs)**：不引入额外依赖（如 `jq`），确保脚本可在最简 CI 镜像中运行；若未来需要更精细的变更元信息，可在此处集中扩展。
# - **风险提示 (Gotchas)**：若 PR 大量重命名文件，`git diff` 会返回 `old -> new` 形式，此处取终点路径以匹配后续检查的判断逻辑。
changed_files=()
diff_base=""
diff_base_mode="absent"
declare -A __changed_lookup=()

add_changed_file() {
    ## 辅助函数：向全局 `changed_files` 中安全写入
    # - **意图 (Why)**：封装去重逻辑，避免在多处手动维护 `declare -A` 判断；
    # - **契约 (What)**：参数 1 为相对路径；若路径非空且未出现过，则加入数组。
    local path="$1"
    [[ -z "$path" ]] && return
    if [[ -z "${__changed_lookup[$path]:-}" ]]; then
        changed_files+=("$path")
        __changed_lookup[$path]=1
    fi
}

normalize_porcelain_path() {
    ## 辅助函数：解析 `git status --porcelain -z` 输出
    # - **意图 (Why)**：处理 `old -> new` 与含空格路径，保证后续检查获得准确文件名。
    local raw="$1"
    if [[ "$raw" == *" -> "* ]]; then
        raw="${raw##* -> }"
    fi
    printf '%s' "$raw"
}

determine_diff_base() {
    ## 推断当前执行的 diff 基准
    # - **意图 (Why)**：在 PR 中与目标分支比较，在本地则退化为 `HEAD^` 或直接使用 `HEAD`，确保护栏以一致视角审视变更。
    # - **契约 (What)**：若能找到合适基准则输出其 commit id，否则输出空字符串。
    local candidate=""

    if [[ -n "${CONSISTENCY_BASE_SHA:-}" ]]; then
        if git cat-file -e "${CONSISTENCY_BASE_SHA}^{commit}" >/dev/null 2>&1; then
            candidate=$(git rev-parse "${CONSISTENCY_BASE_SHA}")
        fi
    fi

    if [[ -z "$candidate" && -n "${CONSISTENCY_BASE_REF:-}" ]]; then
        if git rev-parse --verify "${CONSISTENCY_BASE_REF}" >/dev/null 2>&1; then
            candidate=$(git rev-parse "${CONSISTENCY_BASE_REF}")
        fi
    fi

    if [[ -z "$candidate" ]]; then
        if git rev-parse --verify origin/main >/dev/null 2>&1; then
            candidate=$(git merge-base HEAD origin/main)
        fi
    fi

    if [[ -z "$candidate" ]]; then
        if git rev-parse --verify HEAD^ >/dev/null 2>&1; then
            candidate=$(git rev-parse HEAD^)
        elif git rev-parse --verify HEAD >/dev/null 2>&1; then
            candidate=$(git rev-parse HEAD)
        fi
    fi

    printf '%s' "$candidate"
}

populate_changed_files() {
    ## 统一收集“相对于基准”与“当前工作区”两类变更
    # - **意图 (Why)**：既兼容 CI（干净工作区，只存在 commit diff），也兼容开发者本地（包含未提交改动）。
    # - **执行策略 (How)**：
    #   1. 若存在 diff 基准，调用 `git diff --name-only <base>..HEAD`；
    #   2. 额外调用 `git diff --name-only HEAD` 捕获工作区未提交改动；
    #   3. 使用 `git status --porcelain -z` 兜底，确保新增文件也被纳入（尤其是未 `git add` 的场景）。
    local base
    base=$(determine_diff_base)
    diff_base="$base"
    diff_base_mode="absent"

    # - **延伸契约补充**：额外记录基线来源，供后续守卫判断“是否需要对比提交历史”
    #   - 若 `diff_base` 来自上游分支（CI 正常场景），则需要扫描整个提交差异；
    #   - 若仅能回退到 `HEAD` 或 `HEAD^`（本地孤立分支场景），则仅针对工作区变更触发守卫，避免对历史提交重复执法。
    if [[ -n "$base" ]]; then
        local head_rev
        head_rev=$(git rev-parse HEAD)
        if [[ "$base" == "$head_rev" ]]; then
            diff_base_mode="self"
        elif git rev-parse --verify HEAD^ >/dev/null 2>&1 && [[ "$base" == "$(git rev-parse HEAD^ 2>/dev/null)" ]]; then
            diff_base_mode="parent"
        else
            diff_base_mode="upstream"
        fi
    fi

    if [[ -n "$base" ]]; then
        while IFS= read -r path; do
            add_changed_file "$path"
        done < <(git diff --name-only "$base"..HEAD || true)
    fi

    while IFS= read -r path; do
        add_changed_file "$path"
    done < <(git diff --name-only HEAD || true)

    while IFS= read -r -d '' entry; do
        local payload
        payload=$(normalize_porcelain_path "${entry:3}")
        add_changed_file "$payload"
    done < <(git status --porcelain -z || true)
}

populate_changed_files

is_file_changed() {
    ## 查询某文件是否出现在当前执行的变更集中
    # - **契约 (What)**：参数 1 为相对路径，若存在返回 0，否则返回 1。
    local target="$1"
    [[ -z "$target" ]] && return 1
    [[ -n "${__changed_lookup[$target]:-}" ]]
}

## 检查一：禁止新的 PollReady 枚举
# - **意图**：保持唯一的就绪状态枚举定义。
# - **实现逻辑**：
#   1. 使用 `rg` 在所有 Rust 源文件中查找 “`enum` + `PollReady`” 组合。
#   2. 若命中，记录文件位置并输出修复指引。
# - **后置条件**：无违规时保持静默。
check_forbidden_poll_ready() {
    local matches
    mapfile -t matches < <(rg --color=never --pcre2 --glob '*.rs' -n '\benum\s+PollReady\b' || true)
    if ((${#matches[@]} > 0)); then
        printf '错误：检测到被禁止的 `enum` `PollReady` 组合定义，禁止引入第二套就绪状态枚举。\n' >&2
        printf '位置：\n' >&2
        printf '  %s\n' "${matches[@]}" >&2
        printf '建议：请统一使用 `status::ready` 模块中的现有定义。\n' >&2
        violation_count=1
    fi
}

## 检查二：禁止遗留的 Backpressure Reason 关键字
# - **意图**：迁移完成后彻底清除旧的背压枚举，防止贡献者从历史提交中复制粘贴旧实现。
# - **实现逻辑**：若仓库仍出现 “Backpressure” 与 “Reason” 无缝拼接的字面量，立即报告违规。
# - **契约**：允许在 Markdown 文档中保留历史记录，但要求在 Rust 源码中完全消除。
check_forbidden_backpressure_reason() {
    local matches
    local legacy_token="Backpressure""Reason"
    mapfile -t matches < <(rg --color=never --pcre2 --glob '*.rs' -n "\\b${legacy_token}\\b" || true)
    if ((${#matches[@]} > 0)); then
        printf '错误：检测到遗留的 `%s%s` 关键词，请彻底移除旧的背压枚举实现。\n' 'Backpressure' 'Reason' >&2
        printf '位置：\n' >&2
        printf '  %s\n' "${matches[@]}" >&2
        printf '建议：统一使用 `status::ready` 模块中的 `ReadyState/BusyReason`。\n' >&2
        violation_count=1
    fi
}

## 检查三：禁止新增 `backpressure*.rs` 顶层文件
# - **意图**：防止再次出现平行的背压模块命名，保持模块拓扑唯一。
# - **实现逻辑**：
#   1. 利用 `git ls-files` 枚举所有匹配 `backpressure*.rs` 的已跟踪文件。
#   2. 允许位于目录 `*/backpressure/` 内的内部模块（通常为私有实现细分目录）。
#   3. 其余命名视为违规，提示改用已有模块或内部 mod 结构。
# - **风险提示**：若未来调整目录结构，需要同步更新 allowlist。
check_backpressure_filenames() {
    local files
    mapfile -t files < <(git ls-files '*backpressure*.rs')
    for file in "${files[@]}"; do
        [[ -z "$file" ]] && continue
        if [[ "$file" == *'/tests/'* || "$file" == crates/spark-contract-tests/* ]]; then
            continue
        fi
        if [[ "$file" == */backpressure/* ]]; then
            continue
        fi
        printf '错误：检测到受限文件命名 `%s`，请勿新增顶层 `backpressure*.rs`。\n' "$file" >&2
        printf '建议：如需扩展，请在已有模块目录下创建子模块或复用现有实现。\n' >&2
        violation_count=1
    done
}

## 检查四：禁止公共 API 暴露第二套就绪/背压命名
# - **意图 (Why)**：
#   1. 确保 `ReadyState/ReadyCheck/BusyReason` 仍然是唯一且权威的语义出口，
#      避免贡献者以 `ReadyCheck2`、`BusyCode` 等命名包装同一语义，破坏跨 crate 一致性。
#   2. 一旦公共接口暴露了第二套命名，下游调用方在升级依赖后将面临“哪套类型才是准绳”
#      的二义性，本检查通过 CI 直接阻断这类分叉。
# - **所在位置与角色 (Where)**：位于一致性脚本中，紧跟前序结构化检查，确保在编译/测试之前
#   就能捕获命名违规，降低回滚成本。
# - **执行逻辑 (How)**：
#   1. 使用正则匹配 `pub struct/enum/type/trait/mod` 及 `pub use` 语句，收集所有对外可见的标识符；
#   2. 将名称与禁止列表进行对比：
#      - 若以 `ReadyCheck/ReadyState/BusyReason` 为前缀但带有额外后缀（表示试图派生“第二版”）；
#      - 或命名包含 `BusyCode`、`Backpressure*` 等历史遗留标签；
#   3. 命中后打印文件及建议，提示回归统一的 `status::ready` 抽象。
# - **契约与边界 (What)**：
#   - **输入**：仓库内所有受 Git 管理的 `.rs` 文件；
#   - **输出**：若发现违规，输出详细位置与整改建议；
#   - **前置条件**：`rg` 可用；
#   - **后置条件**：一旦检测到命名分叉即设置 `violation_count`，阻断后续流程。
# - **设计取舍 (Trade-offs)**：
#   - 通过“正则 + allowlist”方案快速覆盖 80% 以上风险，避免引入 AST 解析成本；
#   - 仅关注公开可见项（`pub` 开头），既保证精准，又允许内部实现自由演进。
check_public_ready_naming() {
    local definition_pattern
    local use_pattern
    local definition_matches
    local use_matches

    # 正则说明：
    # - `pub(?:\s+|\([^)]*\)|/\*.*?\*/)*` 允许 `pub`, `pub(crate)` 及行内注释；
    # - `struct|enum|trait|type|mod` 捕获对外可见的类型/模块定义；
    # - `ReadyCheck[A-Za-z0-9_]+` 等限定至少带一个后缀，排除合法基准名。
    definition_pattern='^\s*pub(?:\s+|\([^)]*\)\s+|/\*.*?\*/\s*)*(struct|enum|trait|type|mod)\s+(?:ReadyCheck[A-Za-z0-9_]+|ReadyState[A-Za-z0-9_]+|BusyReason[A-Za-z0-9_]+|BusyCode\b|Backpressure[A-Za-z0-9_]+)'
    use_pattern='^\s*pub\s+use\b[^;]*\b(?:ReadyCheck[A-Za-z0-9_]+|ReadyState[A-Za-z0-9_]+|BusyReason[A-Za-z0-9_]+|BusyCode\b|Backpressure[A-Za-z0-9_]+)\b'

    mapfile -t definition_matches < <(rg --color=never --pcre2 --no-heading -n --glob '*.rs' "$definition_pattern" || true)
    mapfile -t use_matches < <(rg --color=never --pcre2 --no-heading -n --glob '*.rs' "$use_pattern" || true)

    local -a allow_keywords=(
        'ReadyStateEvent'
    )

    if ((${#definition_matches[@]} > 0)); then
        local -a filtered=()
        for entry in "${definition_matches[@]}"; do
            local skip=0
            for keyword in "${allow_keywords[@]}"; do
                if [[ "$entry" == *"$keyword"* ]]; then
                    skip=1
                    break
                fi
            done
            ((skip == 1)) && continue
            filtered+=("$entry")
        done
        definition_matches=("${filtered[@]}")
    fi

    if ((${#use_matches[@]} > 0)); then
        local -a filtered=()
        for entry in "${use_matches[@]}"; do
            local skip=0
            for keyword in "${allow_keywords[@]}"; do
                if [[ "$entry" == *"$keyword"* ]]; then
                    skip=1
                    break
                fi
            done
            ((skip == 1)) && continue
            filtered+=("$entry")
        done
        use_matches=("${filtered[@]}")
    fi

    if ((${#definition_matches[@]} > 0 || ${#use_matches[@]} > 0)); then
        printf '错误：检测到对外暴露的第二套 Ready/Busy/Backpressure 命名，请回归 `status::ready` 提供的统一抽象。\n' >&2
        printf '位置：\n' >&2
        if ((${#definition_matches[@]} > 0)); then
            printf '  %s\n' "${definition_matches[@]}" >&2
        fi
        if ((${#use_matches[@]} > 0)); then
            printf '  %s\n' "${use_matches[@]}" >&2
        fi
        printf '建议：直接复用 `ReadyState/ReadyCheck/BusyReason`，或在私有模块内进行转换，避免面向调用方暴露新命名。\n' >&2
        violation_count=1
    fi
}

## 检查五：禁止在泛型层直接调用 `tokio::spawn`
# - **意图 (Why)**：
#   1. 泛型层承担“零虚分派 + 不携带运行时假设”的职责，若在此层直接 `tokio::spawn`，
#      将强迫所有实现默认依赖 Tokio 调度器，破坏“可在不同执行器上复用”的设计前提。
#   2. 历史上出现过“泛型层偷用 Tokio 特性”导致 `no_std`/自定义 runtime 场景无法编译的问题，
#      本检查将问题前置到 CI，避免再次回归。
# - **所在位置 (Where)**：位于一致性脚本中，介于命名守卫与语义守卫之间，确保在编译前尽早失败。
# - **执行策略 (How)**：
#   1. 利用 `git ls-files` 精确枚举所有 `*/traits/generic.rs` 泛型层文件；
#   2. 使用 `rg` 搜索 `tokio::spawn(` 调用（允许存在空格），一旦命中即提示；
#   3. 若未来泛型层新增拆分模块，可在 allowlist 中扩展匹配规则。
# - **契约 (What)**：
#   - **输入**：仓库中所有受 Git 管理的 `generic` 层 Rust 文件；
#   - **输出**：打印违规位置及修复建议；
#   - **前置条件**：环境需具备 `rg`；
#   - **后置条件**：命中即设置 `violation_count`，阻断流水线。
# - **设计权衡 (Trade-offs)**：
#   - 直接匹配字符串而非解析 AST，换取实现简洁；若未来允许特定宏包装需另行豁免。
# - **风险提示 (Gotchas)**：
#   - 若某实现通过 re-export 将 `tokio::spawn` 重命名后调用，此检查无法捕获，需要 code review 配合。
check_generic_layer_tokio_spawn() {
    local files
    local matches=()

    mapfile -t files < <(git ls-files 'crates/spark-core/src/**/traits/generic.rs')

    for file in "${files[@]}"; do
        [[ -z "$file" ]] && continue
        while IFS= read -r line; do
            matches+=("$line")
        done < <(rg --color=never --pcre2 -n 'tokio::spawn\s*\(' "$file" || true)
    done

    if ((${#matches[@]} > 0)); then
        printf '错误：检测到泛型层直接调用 `tokio::spawn`，违背“无运行时假设”原则。\n' >&2
        printf '位置：\n' >&2
        printf '  %s\n' "${matches[@]}" >&2
        printf '建议：请在对象层或 runtime 适配层发起任务，泛型层仅暴露抽象接口。\n' >&2
        violation_count=1
    fi
}

## 检查六：禁止新增“第二表达”的就绪/背压枚举
# - **意图 (Why)**：
#   1. 框架已将就绪/背压语义收敛为 `ReadyState/ReadyCheck/BusyReason`，
#      新增平行的枚举将让上下游再次面临“多语义并存”的混乱局面。
#   2. 通过守卫禁止除 `status::ready` 模块以外的同类枚举，确保所有演进集中在单一文件维护。
# - **所在位置 (Where)**：继泛型层约束之后，继续巩固语义一致性。
# - **执行策略 (How)**：
#   1. 遍历所有位于 `*/src/` 的 Rust 文件；
#   2. 使用正则捕获名称中包含 `Ready`/`Busy`/`Backpressure` 的 `enum` 定义；
#   3. 将合法枚举（`ReadyState`、`ReadyCheck`、`BusyReason`）限定在 `crates/spark-core/src/kernel/status/ready.rs`，其他命中视为违规。
# - **契约 (What)**：
#   - **输入**：仓库内所有源代码枚举定义；
#   - **输出**：违规枚举的文件、行号与名称；
#   - **前置条件**：`python3` 可用；
#   - **后置条件**：命中即计入违规。
# - **设计权衡 (Trade-offs)**：使用简单正则即可覆盖“新增并行枚举”的典型情形，避免引入重量级解析库。
# - **风险提示 (Gotchas)**：测试代码或兼容层若确需自定义枚举，应置于 `tests/` 或显式放行；未来若需扩展 allowlist，请在此函数中更新。
check_duplicate_ready_backpressure_expression() {
    local python_output

    python_output=$(python3 - <<'PY'
import subprocess
import sys
from pathlib import Path
import re

ALLOWED = {
    (Path('crates/spark-core/src/kernel/status/ready.rs'), 'ReadyState'),
    (Path('crates/spark-core/src/kernel/status/ready.rs'), 'ReadyCheck'),
    (Path('crates/spark-core/src/kernel/status/ready.rs'), 'BusyReason'),
}

pattern = re.compile(r'^\s*(?:pub\s+)?enum\s+([A-Za-z0-9_]*(?:Ready|Busy|Backpressure)[A-Za-z0-9_]*)')

try:
    files = subprocess.check_output(['git', 'ls-files', '*.rs'], text=True).splitlines()
except subprocess.CalledProcessError as exc:
    sys.stderr.write(f'无法枚举 Rust 文件：{exc}\n')
    sys.exit(1)

violations = []

for rel in files:
    if '/tests/' in rel or rel.startswith('tools/'):
        continue

    path = Path(rel)
    try:
        lines = path.read_text(encoding='utf-8').splitlines()
    except OSError as exc:
        sys.stderr.write(f'读取文件失败 {rel}: {exc}\n')
        sys.exit(1)

    for idx, line in enumerate(lines, start=1):
        match = pattern.match(line)
        if not match:
            continue

        name = match.group(1)
        key = (path, name)
        if key in ALLOWED:
            continue

        violations.append(f'{rel}:{idx}:{name}')

if violations:
    for item in violations:
        print(item)
PY
) || true

    if [[ -n "$python_output" ]]; then
        printf '错误：检测到新增的就绪/背压枚举定义，违背统一语义出口原则。\n' >&2
        printf '位置（文件:行:枚举名）：\n' >&2
        printf '  %s\n' "$python_output" >&2
        printf '建议：请直接扩展 `status::ready` 中的既有枚举，而非在其他模块重建语义。\n' >&2
        violation_count=1
    fi
}

## 检查七：禁止 `Busy(...)` 携带预算上下文
# - **意图 (Why)**：
#   1. 预算耗尽应直接映射为 `ReadyState::BudgetExhausted`，若放入 `Busy(...)`，
#      调用方会误以为“可重试”，从而在软退避后重复触发硬性拒绝。
#   2. 该检查与前一条“BudgetExhausted → Busy”互补：前者关注状态映射，当前检查聚焦构造参数。
# - **所在位置 (Where)**：位于语义守卫末端，专门处理跨模块调用的细粒度约束。
# - **执行策略 (How)**：
#   1. 逐行扫描所有源文件；
#   2. 发现 `Busy(` 后跟踪括号深度，仅在该调用作用域内搜寻 `Budget`/`BudgetExhausted`；
#   3. 忽略注释及字符串字面量中的 `Budget`，尽量降低误报。
# - **契约 (What)**：
#   - **输入**：仓库中的 `.rs` 文件；
#   - **输出**：列出 `Busy` 触发行与捕获的预算相关片段；
#   - **前置条件**：`python3` 可用；
#   - **后置条件**：捕获任意一处违规即终止流程。
# - **设计权衡 (Trade-offs)**：
#   - 为平衡准确率与实现复杂度，采用固定窗口扫描；若未来出现跨函数宏展开需扩展逻辑。
# - **风险提示 (Gotchas)**：
#   - 若业务使用 `let budget = ...; ReadyState::Busy(budget.into())` 且 `into` 内部才引用 `Budget`，
#      本检查难以捕获，需要配合 code review；
#   - 对测试/工具代码同样生效，以确保示例不会误导贡献者。
check_busy_wrapped_budget() {
    local python_output

    python_output=$(python3 - <<'PY'
import subprocess
import sys
from pathlib import Path

try:
    files = subprocess.check_output(['git', 'ls-files', '*.rs'], text=True).splitlines()
except subprocess.CalledProcessError as exc:
    sys.stderr.write(f'无法枚举 Rust 文件：{exc}\n')
    sys.exit(1)

violations = []

def iter_busy_positions(text: str):
    start = text.find('Busy(')
    while start != -1:
        yield start
        start = text.find('Busy(', start + 5)

for rel in files:
    path = Path(rel)
    try:
        lines = path.read_text(encoding='utf-8').splitlines()
    except OSError as exc:
        sys.stderr.write(f'读取文件失败 {rel}: {exc}\n')
        sys.exit(1)

    for idx, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith('//'):
            continue

        for pos in iter_busy_positions(line):
            depth = 1
            budget_hit = False

            segment = line[pos + 5 :]
            head = line[pos:].strip()
            if 'Budget' in segment and '"' not in segment:
                budget_hit = True

            for ch in segment:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0:
                        break

            look = idx + 1
            budget_line = None
            budget_text = None

            while not budget_hit and depth > 0 and look < len(lines):
                neighbor = lines[look]
                if neighbor.lstrip().startswith('//'):
                    look += 1
                    continue

                if 'Budget' in neighbor and '"' not in neighbor:
                    budget_hit = True
                    budget_line = look + 1
                    budget_text = neighbor.strip()
                    break

                for ch in neighbor:
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                        if depth == 0:
                            break
                if depth == 0:
                    break
                look += 1

            if budget_hit:
                if budget_line is not None:
                    violations.append(
                        f'{rel}:{idx + 1}->{budget_line}:{budget_text}'
                    )
                else:
                    violations.append(f'{rel}:{idx + 1}:{head}')

if violations:
    for item in violations:
        print(item)
PY
) || true

    if [[ -n "$python_output" ]]; then
        printf '错误：检测到 `Busy(...)` 携带预算上下文，违反“预算耗尽需单独表达”的规范。\n' >&2
        printf '位置（Busy 行 → Budget 行）：\n' >&2
        printf '  %s\n' "$python_output" >&2
        printf '建议：改为返回 `ReadyState::BudgetExhausted`，或在繁忙原因中使用非预算信息。\n' >&2
        violation_count=1
    fi
}

## 检查八：禁止将 `ReadyState::BudgetExhausted` 映射为 `Busy(...)`
# - **意图 (Why)**：
#   1. `BudgetExhausted` 表示硬性额度耗尽，属于拒绝请求的终止态；若被映射为 `Busy`，
#      会误导调用方继续重试，造成级联退化。
#   2. 历史上曾出现“BudgetExhausted → Busy” 的折衷实现，本检查在 CI 层面彻底封堵该做法。
# - **所在位置 (Where)**：延续一致性脚本的防线，针对业务语义的关键分支提供额外保护。
# - **执行策略 (How)**：
#   1. 通过 Python 脚本遍历所有受 Git 管理的 `.rs` 文件，逐行分析；
#   2. 仅当命中的行属于代码（跳过注释）时，向后检查最多 5 行，寻找 `ReadyState::Busy` 或 `Busy(` 构造；
#   3. 一旦发现“预算耗尽”分支后立即构造繁忙状态，记录违规位置，提示返回 `ReadyState::BudgetExhausted`。
# - **契约 (What)**：
#   - **输入**：仓库内的 Rust 源文件；
#   - **输出**：若存在违规映射，列出 `起始行 → 目标行` 的对应关系；
#   - **前置条件**：环境需提供 `python3`；
#   - **后置条件**：发现任一违规即终止后续流程。
# - **权衡 (Trade-offs)**：
#   - 采用“窗口检测”而非 AST 解析，兼顾实现复杂度与准确率；
#   - 5 行窗口可覆盖常见 `match`/`if let` 写法，同时避免对无关 `Busy` 分支的误报。
# - **边界提醒 (Gotchas)**：若未来出现生成代码或更复杂的宏展开发生映射，需要扩展检测逻辑。
check_budget_exhausted_to_busy() {
    local python_output

    python_output=$(python3 - <<'PY'
import subprocess
import sys
from pathlib import Path

try:
    files = subprocess.check_output(["git", "ls-files", "*.rs"], text=True).splitlines()
except subprocess.CalledProcessError as exc:  # pragma: no cover - Git 必须可用
    sys.stderr.write(f"无法枚举 Rust 文件：{exc}\n")
    sys.exit(1)

violations = []

for rel_path in files:
    if not rel_path:
        continue

    path = Path(rel_path)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:  # pragma: no cover - 文件读取异常直接失败
        sys.stderr.write(f"读取文件失败 {rel_path}: {exc}\n")
        sys.exit(1)

    for idx, line in enumerate(lines):
        if "ReadyState::BudgetExhausted" not in line:
            continue

        if line.lstrip().startswith("//"):
            continue

        # 检查同一行是否立即将 BudgetExhausted 映射为 Busy。
        if "ReadyState::Busy" in line or "Busy(" in line:
            snippet = line.strip()
            violations.append(f"{rel_path}:{idx + 1}:{snippet}")
            continue

        window_end = min(len(lines), idx + 6)
        for look_ahead in range(idx + 1, window_end):
            neighbor = lines[look_ahead]
            if neighbor.lstrip().startswith("//"):
                continue

            if "ReadyState::Busy" in neighbor or "Busy(" in neighbor:
                snippet = neighbor.strip()
                violations.append(
                    f"{rel_path}:{idx + 1}->{look_ahead + 1}:{snippet}"
                )
                break

if violations:
    for item in violations:
        print(item)
PY
) || true

    if [[ -n "$python_output" ]]; then
        printf '错误：检测到将 `ReadyState::BudgetExhausted` 映射为 `Busy(...)` 的实现，违背预算耗尽语义。\n' >&2
        printf '位置（BudgetExhausted 行 → Busy 行）：\n' >&2
        printf '  %s\n' "$python_output" >&2
        printf '建议：在预算耗尽时直接返回 `ReadyState::BudgetExhausted`，并让上层决定是否降级或拒绝请求。\n' >&2
        violation_count=1
    fi
}

## 检查九：关键状态语义改动需同步跨职能文档
# - **意图 (Why)**：
#   1. 当 PR 改动 `ReadyState` / `RetryAfter` / `ErrorCategory` / `Pipeline` 等核心治理语义时，若缺少配套的状态机手册、重试策略文档、观测仪表以及 Runbook，
#      运维与值班人员将无法快速理解新行为，导致恢复流程断层。
#   2. 历史上出现过“代码先行，Runbook 滞后”的案例，本检查通过自动化方式要求一次提交内补齐各侧文档，形成端到端可追溯链路。
# - **所在位置与作用 (Where)**：位于语义守卫尾部，紧随运行时语义一致性检查之后，专门处理“跨团队知识同步”这一治理维度。
# - **执行策略 (How)**：
#   1. 使用 `git diff --unified=0` 获取相对基线与当前工作区的增量，结合 Python 解析 hunk 行号，定位新增/删除的关键字行；
#   2. 过滤掉 `docs/` 目录，使得纯文档修改不会误触；
#   3. 一旦侦测到目标关键字变更，即要求同时存在以下改动：
#      - `docs/state_machines.md`：记录状态机拓扑与分支语义；
#      - `docs/retry-policy.md`：对外声明重试/退避契约；
#      - `docs/observability/` 目录：更新指标、仪表盘或告警规则；
#      - `docs/runbook/` 下至少一个文件：确保排障脚本与操作指引同步；
#      若缺失任何一项，将输出违规清单并提示补充。
# - **契约 (What)**：
#   - **输入**：`changed_files`（已由前序步骤填充）以及可选的 `diff_base`；
#   - **输出**：当文档缺失时打印触发的关键字行与缺失项，退出码设为非 0；
#   - **前置条件**：仓库可执行 `git diff` 与 `python3`；
#   - **后置条件**：成功时静默通过，失败时阻断 CI；
# - **设计权衡 (Trade-offs)**：
#   - 采用轻量的 diff 解析而非构建 AST，可在不依赖额外第三方库的前提下运行于最小 CI 镜像；
#   - 通过“关键字 + 文档清单”策略覆盖 ReadyState/RP/ErrorCategory/Pipeline 四类典型改动，若未来需要扩展可在关键字数组中追加；
# - **风险提示 (Gotchas)**：
#   - 若贡献者仅在工具脚本中引入关键字字符串（例如生成器参数），也会触发守卫，请在 PR 中说明原因并补齐文档；
#   - 若文档更新通过自动生成脚本完成，需确保最终文件已被 `git add`，否则仍会被视为缺失；
#   - 当前实现仅校验“是否更新”，不验证内容质量，仍需 code review 辅助把关。
check_keyword_semantic_docs() {
    local python_output

    python_output=$(DIFF_BASE="$diff_base" DIFF_BASE_MODE="$diff_base_mode" python3 - <<'PY'
import os
import re
import subprocess
import sys

KEYWORDS = re.compile(r"\b(ReadyState|RetryAfter|ErrorCategory|Pipeline)\b")
HUNK_RE = re.compile(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@")

def run_diff(args, scope, sink):
    try:
        diff = subprocess.check_output(args, text=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"运行 {' '.join(args)} 失败：{exc}") from exc

    current_file = None
    old_line = None
    new_line = None

    for raw in diff.splitlines():
        if raw.startswith('diff --git'):
            continue
        if raw.startswith('index '):
            continue
        if raw.startswith('--- '):
            continue
        if raw.startswith('+++ '):
            candidate = raw[4:]
            if candidate == '/dev/null':
                current_file = None
            else:
                if candidate.startswith('b/'):
                    candidate = candidate[2:]
                current_file = candidate
            continue
        if current_file is None:
            continue
        if current_file.startswith(('docs/', '.github/', 'tools/')):
            continue

        if raw.startswith('@@ '):
            match = HUNK_RE.match(raw)
            if not match:
                old_line = None
                new_line = None
                continue
            old_line = int(match.group(1))
            new_line = int(match.group(2))
            continue

        if raw.startswith('+') and not raw.startswith('+++'):
            if new_line is None:
                continue
            content = raw[1:]
            if KEYWORDS.search(content):
                sink.add((scope, current_file, new_line, 'add', content.strip()))
            new_line += 1
            continue

        if raw.startswith('-') and not raw.startswith('---'):
            if old_line is None:
                continue
            content = raw[1:]
            if KEYWORDS.search(content):
                sink.add((scope, current_file, old_line, 'del', content.strip()))
            old_line += 1
            continue

        if not raw.startswith('\\'):
            if old_line is not None:
                old_line += 1
            if new_line is not None:
                new_line += 1


class OrderedSet:
    def __init__(self):
        self._items = []
        self._seen = set()

    def add(self, item):
        if item in self._seen:
            return
        self._seen.add(item)
        self._items.append(item)

    def __iter__(self):
        return iter(self._items)


records = OrderedSet()

base = os.environ.get('DIFF_BASE', '').strip()
base_mode = os.environ.get('DIFF_BASE_MODE', '').strip()
if base and base_mode == 'upstream':
    run_diff(['git', 'diff', '--no-color', '--unified=0', f'{base}..HEAD', '--', '.', ':(exclude)docs/**'], 'committed', records)

run_diff(['git', 'diff', '--no-color', '--unified=0', 'HEAD', '--', '.', ':(exclude)docs/**'], 'worktree', records)

for scope, path, line, kind, content in records:
    print(f"{scope}|{path}|{line}|{kind}|{content}")
PY
) || true

    if [[ -z "$python_output" ]]; then
        return
    fi

    local state_doc=0
    local retry_doc=0
    local observability_doc=0
    local runbook_doc=0

    for file in "${changed_files[@]}"; do
        case "$file" in
            'docs/state_machines.md')
                state_doc=1
                ;;
            'docs/retry-policy.md')
                retry_doc=1
                ;;
            docs/observability/*)
                observability_doc=1
                ;;
            docs/runbook/*)
                runbook_doc=1
                ;;
        esac
    done

    if ((state_doc == 1 && retry_doc == 1 && observability_doc == 1 && runbook_doc == 1)); then
        return
    fi

    printf '错误：检测到涉及 ReadyState/RetryAfter/ErrorCategory/Pipeline 的代码变更，但缺少文档/仪表/Runbook 配套更新。\n' >&2
    printf '触发的关键字行：\n' >&2
    printf '  %s\n' "$python_output" >&2

    if ((state_doc == 0)); then
        printf '缺失：请更新 `docs/state_machines.md`，保持状态机语义与实现一致。\n' >&2
    fi
    if ((retry_doc == 0)); then
        printf '缺失：请更新 `docs/retry-policy.md`，同步重试/退避契约。\n' >&2
    fi
    if ((observability_doc == 0)); then
        printf '缺失：请更新 `docs/observability/` 下的指标或仪表盘，确保告警与采集覆盖新语义。\n' >&2
    fi
    if ((runbook_doc == 0)); then
        printf '缺失：请更新 `docs/runbook/` 目录，指导一线排障操作。\n' >&2
    fi

    printf '建议：在 PR 描述中附带关键测试/演练链接，证明新语义已通过验证。\n' >&2
    violation_count=1
}

## 检查六：generic 层禁止直接调用 `tokio::spawn`
# - **意图 (Why)**：`TaskExecutor::spawn` 已强制绑定 `CallContext`，若在泛型 Handler 层直接调用
#   `tokio::spawn` 会绕过上下文传播，导致取消/截止信号丢失。
# - **执行策略 (How)**：使用 `rg` 定位 `crates/spark-core/src/data_plane/service/traits/generic.rs` 中的 `tokio::spawn(`；
#   若命中则立即报错并提醒改用运行时注入的执行器。
# - **契约 (What)**：仅检查泛型层源文件，避免误报测试或文档。
check_generic_no_tokio_spawn() {
    local matches
    mapfile -t matches < <(rg --color=never -n 'tokio::spawn\(' crates/spark-core/src/data_plane/service/traits/generic.rs || true)
    if ((${#matches[@]} > 0)); then
        printf '错误：generic 层禁止直接调用 `tokio::spawn`，请改用 `TaskExecutor::spawn` 以确保上下文传播。\n' >&2
        printf '位置：\n' >&2
        printf '  %s\n' "${matches[@]}" >&2
        printf '建议：通过依赖注入获取运行时执行器，并传入父 `CallContext`。\n' >&2
        violation_count=1
    fi
}

## 检查十：状态机语义变更需同步文档与 Runbook
# - **意图 (Why)**：
#   1. ReadyState/BusyReason 的契约一旦调整，运维侧的 Runbook 与研发侧的状态机说明若未及时更新，将导致告警处置与设计解读脱节。
#   2. 将“文档同步”前置到 CI，可避免在合入后补写文档的惯性拖延，实现语义与文档双闭环。
# - **所在位置 (Where)**：位于所有语义守卫之后，作为最终的治理关卡，聚焦“变更是否配套文档”。
# - **执行策略 (How)**：
#   1. 枚举状态机关键哨兵文件（`crates/spark-core/src/kernel/status/ready.rs`、`crates/spark-core/src/kernel/status/mod.rs`、`crates/spark-core/src/data_plane/service/simple.rs` 等）；
#   2. 若本次改动触及任一哨兵，则要求同时修改：
#      - `docs/state_machines.md`（研发态视角的状态机文档）；
#      - `docs/runbook/` 目录下至少一个文件（运维态 Runbook）。
#   3. 将缺失项列出，提供明确整改指引。
# - **契约 (What)**：
#   - 输入：全局 `changed_files`；
#   - 输出：若存在缺失文档，打印错误信息并标记违规；
#   - 前置条件：`populate_changed_files` 已运行；
#   - 后置条件：文档缺失时阻断流程，否则静默通过。
# - **设计权衡 (Trade-offs)**：
#   - 哨兵列表采用显式列举，优先覆盖 ReadyState 主干文件；若未来状态机拆分，可在此扩展列表；
#   - Runbook 校验以目录前缀匹配，允许一次性更新多个 Runbook，避免硬编码文件名。
# - **风险提示 (Gotchas)**：
#   - 若改动仅涉及示例或测试（`tests/` 目录），不会触发此守卫；
#   - 若文档更新通过脚本生成，请确保在执行前已 `git add`，否则仍会被判定缺失。
check_state_machine_docs_synced() {
    local -a sentinels=(
        'crates/spark-core/src/kernel/status/ready.rs'
        'crates/spark-core/src/kernel/status/mod.rs'
        'crates/spark-core/src/data_plane/service/simple.rs'
        'crates/spark-core/src/data_plane/service/traits/generic.rs'
    )

    local -a touched_sentinels=()
    local doc_touched=0
    local runbook_touched=0

    for sentinel in "${sentinels[@]}"; do
        if is_file_changed "$sentinel"; then
            touched_sentinels+=("$sentinel")
        fi
    done

    if ((${#touched_sentinels[@]} == 0)); then
        return
    fi

    for file in "${changed_files[@]}"; do
        if [[ "$file" == 'docs/state_machines.md' ]]; then
            doc_touched=1
        fi
        if [[ "$file" == docs/runbook/* ]]; then
            runbook_touched=1
        fi
    done

    if ((doc_touched == 1 && runbook_touched == 1)); then
        return
    fi

    printf '错误：状态机相关代码变更缺少文档或 Runbook 更新。\n' >&2
    printf '触发的状态机文件：\n' >&2
    printf '  %s\n' "${touched_sentinels[@]}" >&2

    if ((doc_touched == 0)); then
        printf '缺失：`docs/state_machines.md` 未更新，请补充状态机演进记录。\n' >&2
    fi
    if ((runbook_touched == 0)); then
        printf '缺失：`docs/runbook/` 目录下未见改动，请同步运维处置指引。\n' >&2
    fi

    printf '建议：与架构文档、Runbook 共同更新，确保研发与运维对 ReadyState 语义保持一致。\n' >&2
    violation_count=1
}

check_forbidden_poll_ready
check_forbidden_backpressure_reason
check_backpressure_filenames
check_public_ready_naming
check_generic_layer_tokio_spawn
check_duplicate_ready_backpressure_expression
check_busy_wrapped_budget
check_budget_exhausted_to_busy
check_generic_no_tokio_spawn
check_keyword_semantic_docs
check_state_machine_docs_synced

if ((violation_count > 0)); then
    exit 1
fi
