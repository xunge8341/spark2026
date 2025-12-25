#!/usr/bin/env bash

# 教案级说明：混沌场景执行入口脚本
# ==================================
# 意图 (Why)
# ---------
# * 该脚本作为 SRE 与 CI 的统一入口，屏蔽 `chaos_runner.py` 的调用细节，避免直接依赖 Python 模块路径。
# * 通过解析 JSON 场景文件获取场景 ID，可直接以文件为单位进行版本控制与 Code Review，降低误操作风险。
# * 允许将额外参数透传给 Runner（如 `--dry-run`、`--note`），保持与现有混沌执行流程的兼容性。
#
# 架构位置与模式
# ---------------
# * 本脚本位于 `tools/chaos/`，与场景定义、Runner 同级，是“命令适配器”层的一部分。
# * 采用“薄包装器 (Thin Wrapper)”模式：自身不实现业务逻辑，仅完成参数校验与协调，核心执行仍由 Python Runner 负责。
#
# 契约 (What)
# -----------
# * 输入：
#   1. 第一个位置参数为 JSON 场景文件路径（相对或绝对路径均可）。
#   2. 可选地接受多个附加参数，原样透传给 `chaos_runner.py run` 子命令。
# * 前置条件：
#   - JSON 文件必须存在且包含 `id` 字段；脚本会在调用 Runner 前进行校验。
#   - 运行环境需具备 Python 3 解释器，并能执行 `tools/chaos/chaos_runner.py`。
# * 后置条件：
#   - 当 Runner 正常返回时，标准输出会打印记录文件路径；脚本退出码为 0。
#   - 若校验失败或 Runner 出错，脚本会将错误信息打印至标准错误并返回非零退出码。
#
# 逻辑解析 (How)
# ---------------
# 1. 启用 `set -euo pipefail` 以在 shell 层面捕获未定义变量及子命令失败，避免错误被静默吞掉。
# 2. 校验参数个数，若缺失必填场景文件则给出友好提示并退出。
# 3. 使用内嵌 Python 解析 JSON，获取 `id` 字段：
#    ```python
#    data = json.load(handle)
#    scenario_id = data["id"]
#    ```
#    - 之所以选用 Python 而非 `jq`，是为了避免额外依赖，满足在最小化环境中执行的需求。
# 4. 将解析出的 ID 传递给 Runner：
#    ```bash
#    python3 chaos_runner.py run "$scenario_id" ...
#    ```
#    同时透传剩余参数，保持可扩展性。
#
# 权衡与注意事项 (Trade-offs & Gotchas)
# -------------------------------------
# * 将 JSON 校验逻辑放在 Bash + Python 组合中，牺牲了一定的统一性，但换来零外部依赖和更易理解的实现。
# * 若 JSON 缺失 `id` 字段，脚本会明确报错；这是一种 fail-fast 策略，防止误执行其他场景。
# * 读取 JSON 时未设置巨大文件的内存保护，因场景文件通常很小（<10KB），性能与安全风险可接受。
# * 若未来 Runner 位置发生调整，需要同步更新脚本中的路径推导逻辑（通过 `BASH_SOURCE` 自动适配绝大多数情况）。
#
# 跟踪任务提醒
# ------------
# * 若需支持一次运行多个场景，请参见《docs/tracking/T107.md#chaos-runner-batch-support》记录的任务；当前实现仅处理单个场景，
#   以接口简洁性优先，后续扩展需评估批量运行的并发控制策略与错误收敛机制。

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "用法: $0 <scenario_json_path> [runner_args...]" >&2
  exit 1
fi

SCENARIO_PATH="$1"
shift || true

if [[ ! -f "$SCENARIO_PATH" ]]; then
  echo "错误: 场景文件 '$SCENARIO_PATH' 不存在" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SCENARIO_ID="$(python3 - "$SCENARIO_PATH" <<'PY'
import json
import sys
from pathlib import Path

# 教案级注释：解析 JSON 获取场景 ID
# 意图
# ----
# * 该嵌入式 Python 代码负责读取场景文件并提取 `id` 字段，确保后续 Runner 执行的场景与文件内容一致。
# * 使用 Python 标准库避免额外依赖，保证脚本在 CI 与 Bastion 环境中可直接运行。
#
# 契约
# ----
# * 输入：命令行第一个参数为场景文件路径。
# * 前置条件：文件必须可读且包含 `id` 字段。
# * 后置条件：标准输出打印场景 ID；若缺失字段或 JSON 解析失败则抛出异常，中止脚本执行。
#
# 逻辑
# ----
# 1. 解析路径，确保错误信息中展示绝对路径，便于排查。
# 2. 读取文件并解析 JSON，捕获 KeyError 以提供更清晰的错误提示。
#
# 风险提示
# --------
# * 若 JSON 超大或带有 BOM，`json.load` 可能失败；此处假设混沌场景文件较小且遵循 UTF-8 无 BOM。

path = Path(sys.argv[1]).expanduser().resolve()
with path.open("r", encoding="utf-8") as handle:
    data = json.load(handle)
try:
    scenario_id = data["id"]
except KeyError as exc:  # pragma: no cover - 运行期显式抛错
    raise SystemExit(f"场景文件缺少必填字段 'id': {path}") from exc
print(scenario_id)
PY
)"

if [[ -z "$SCENARIO_ID" ]]; then
  echo "错误: 无法从 '$SCENARIO_PATH' 解析到场景 ID" >&2
  exit 1
fi

python3 "$SCRIPT_DIR/chaos_runner.py" run "$SCENARIO_ID" "$@"
