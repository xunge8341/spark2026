#!/usr/bin/env bash
# 教案级注释：公共 Trait 预算守门脚本
# -----------------------------------------------------------------------------
# 设计目标（Why, R1.1-R1.3）
# - Trait 合并路线需要明确的“非破坏式门控”：当我们通过 Facade 等手段合并接口时，
#   仍可能暂时保留兼容层（如 #[deprecated] 适配器）。本脚本提供精确的公共 Trait 计数，
#   防止一次提交引入过量新 Trait，确保迁移节奏受控。
# - 在 DevOps 架构中，该脚本位于 tools/ci，与 `check_public_api_core.sh`、
#   `public_api_diff_budget.sh` 同级，共同构成 SemVer 防线。Trait 预算守门专注于“类型
#   数量”这一维度，补足 Diff 预算无法直接反映的增长趋势。
# - 关键依赖为 cargo-public-api（输出公共符号），以及 Python（快速解析 JSON 与文本）。
#   通过“执行 -> 解析 -> 守门”三段式流程，脚本能在 CI 与本地保持一致行为。
#
# 合同定义（What, R3.1-R3.3）
# - 输入：可选环境变量
#     * `PUBLIC_TRAIT_BUDGET`：允许新增的公共 Trait 数量，默认 35。
#     * `PUBLIC_API_DIFF_BASE`：透传至 `public_api_diff_budget.sh` 的基线，这里仅用于日志
#       提示（Trait 守门始终比较当前输出与仓库基线）。
# - 前置条件：
#     * 当前目录处于仓库根；
#     * 已安装 `cargo-public-api`、`python3`；
#     * 基线文件 `tools/baselines/spark-core.public-api.json` 存在。
# - 输出：
#     * 当新增 Trait 数 ≤ 预算时，打印通过信息及当前总数；
#     * 若超出预算，列出新增 Trait 并退出状态码 1。
# - 后置条件：脚本不会修改工作区，仅生成临时文件并自动清理。
#
# 执行流程（How, R2.1-R2.2）
# 1. 解析环境变量并校验预算值为非负整数；
# 2. 调用 `cargo public-api --simplified` 输出当前公共 API 至临时文件；
# 3. 使用 Python 比较基线 JSON 与当前输出：
#      - 统计当前 Trait 总数；
#      - 计算新增 Trait 集合（差集）；
#      - 将新增数量与预算比较，决定是否失败；
# 4. 打印诊断信息并释放临时文件。
#
# 设计权衡（Trade-offs, R4.1-R4.3）
# - 重新运行 `cargo public-api` 会带来数秒开销，但换来独立的 Trait 计数逻辑；
#   若未来希望共用输出，可在流水线中先运行本脚本再将产物传给其它守门；
# - 预算默认为 35，体现“合并阶段允许少量兼容 Trait 存活”的策略；如需更严格，可在
#   CI 环境覆盖该值；
# - 若出现新的 crate 需要守门，可扩展脚本接受 `-p` 参数，本版聚焦 `spark-core`。
#
# 可维护性提示（Readability, R5.1-R5.2）
# - 函数与变量均使用自描述名称（如 `trait_budget`, `baseline_traits_file`），并在关键
#   步骤前附注释；
# - 使用 `trap` 保证异常时清理临时文件，避免污染工作区；
# - 所有日志统一加 `[trait-budget]` 前缀，便于在 CI 日志中检索。
# -----------------------------------------------------------------------------

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
readonly TARGET_CRATE="spark-core"
readonly BASELINE_FILE="${REPO_ROOT}/tools/baselines/${TARGET_CRATE}.public-api.json"
readonly TRAIT_BUDGET="${PUBLIC_TRAIT_BUDGET:-35}"

log_info() {
  printf '[trait-budget][INFO] %s\n' "$1"
}

log_error() {
  printf '[trait-budget][ERROR] %s\n' "$1" >&2
}

ensure_tools() {
  command -v cargo-public-api >/dev/null || {
    log_error "未找到 cargo-public-api，请运行 'cargo install cargo-public-api'。"
    exit 127
  }
  command -v python3 >/dev/null || {
    log_error "未找到 python3，无法解析公共 API 输出。"
    exit 127
  }
}

validate_inputs() {
  if [[ ! -f "${BASELINE_FILE}" ]]; then
    log_error "缺少基线文件 ${BASELINE_FILE}，请执行 tools/ci/check_public_api_core.sh 生成。"
    exit 2
  fi

  if [[ ! "${TRAIT_BUDGET}" =~ ^[0-9]+$ ]]; then
    log_error "PUBLIC_TRAIT_BUDGET 必须为非负整数，当前值: ${TRAIT_BUDGET}。"
    exit 2
  fi
}

run_public_api() {
  local output_file=""
  output_file="$(mktemp)"
  trap 'if [[ -n "${output_file-}" ]]; then rm -f -- "${output_file}"; fi' EXIT

  log_info "正在统计 ${TARGET_CRATE} 公共 Trait，总预算 ${TRAIT_BUDGET}。"
  if ! cargo public-api \
    -p "${TARGET_CRATE}" \
    --simplified \
    --color never \
    >"${output_file}"; then
    log_error "cargo public-api 执行失败，请检查上方编译日志。"
    exit 1
  fi

  python3 - <<'PY' "${BASELINE_FILE}" "${output_file}" "${TRAIT_BUDGET}"
import json
import sys
from json import JSONDecodeError
from pathlib import Path

baseline_path, current_path, budget_str = sys.argv[1:4]
budget = int(budget_str)
baseline_text = Path(baseline_path).read_text(encoding="utf-8")
current = Path(current_path).read_text(encoding="utf-8").splitlines()

try:
    baseline = json.loads(baseline_text)
except JSONDecodeError:
    # 教案级补充说明：部分历史基线缺少 JSON 数组包装，为兼容迁移期
    # 的老格式，这里回退到逐行解析，并通过 json.loads 处理转义字符。
    parsed_lines = []
    for raw_line in baseline_text.splitlines():
        stripped = raw_line.strip().rstrip(',')
        if not stripped:
            continue
        parsed_lines.append(json.loads(stripped))
    baseline = parsed_lines

baseline_traits = {item for item in baseline if item.startswith("pub trait ")}
current_traits = {line for line in current if line.startswith("pub trait ")}

new_traits = sorted(current_traits - baseline_traits)

print(f"[trait-budget][INFO] 当前公共 Trait 总数：{len(current_traits)}（基线 {len(baseline_traits)}）。")

if new_traits:
    print(f"[trait-budget][INFO] 本次新增 Trait {len(new_traits)} 个：")
    for item in new_traits:
        print(f"  {item}")
else:
    print("[trait-budget][INFO] 未检测到新增 Trait，维持基线状态。")

if len(new_traits) > budget:
    print(
        f"[trait-budget][ERROR] 新增 Trait 数 {len(new_traits)} 超出预算 {budget}，请压缩兼容层或合并接口。",
        file=sys.stderr,
    )
    raise SystemExit(1)
PY

  trap - EXIT
  rm -f -- "${output_file}"
}

main() {
  cd -- "${REPO_ROOT}"
  ensure_tools
  validate_inputs
  run_public_api
  log_info "Trait 预算检查通过。"
}

main "$@"
