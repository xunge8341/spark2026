#!/usr/bin/env bash
# shellcheck disable=SC2086

set -euo pipefail

# ============================= 教案级注释 =============================
# 目的 (Why):
#   - 本脚本提供 CI 阶段的门禁检查，确保「评分卡」产物存在且符合客观门控要求。
#   - 它属于合入前的自动化保障链路，与测试、监控数据导出等步骤并行验证，
#     防止缺失关键文档导致评分模型无法执行。
# 在整体架构中的作用 (Where):
#   - 该脚本位于 `tools/ci/`，由仓库的顶层 CI 任务调用，
#     与其他治理脚本（如一致性守卫、协议守卫）同级，属于评分治理的入口。
# 设计说明 (How & Pattern):
#   - 采用「显式清单检查」模式：维护一组必须存在的产物路径和文档关键段落标识。
#   - 每个检查都以短路策略执行，一旦缺失即终止，以确保 CI 失败并提示原因。
# 输入 / 输出 契约 (Contract):
#   - 输入：无显式参数，隐式依赖仓库根目录和 Markdown 文档内容。
#   - 前置条件：调用环境必须已经检出完整仓库，且评分卡文档应按约定完成编写。
#   - 后置条件：
#       * 若所有产物存在且关键标识均匹配，则退出码为 0；
#       * 一旦缺失产物或关键标识，脚本会输出错误信息并以非零码退出。
# 边界情况与权衡 (Trade-offs):
#   - 为兼容 POSIX shell 与 Bash，我们刻意避免使用 Bash 4+ 的关联数组，
#     改用纯数组迭代以提升可移植性。
#   - 关键字检查采用 `grep -F`，避免正则的歧义，同时 `LC_ALL=C` 保证编码一致性。
#   - 脚本不解析具体分数计算逻辑，仅确保基础产物到位；
#     更复杂的评分计算交由后续流水线任务处理，以保持职责单一。
# ======================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SCORECARD_PATH="${REPO_ROOT}/docs/scorecard.md"

if [[ ! -f "${SCORECARD_PATH}" ]]; then
  echo "[scorecard] 缺失评分卡文档: ${SCORECARD_PATH}" >&2
  exit 1
fi

# 教案级说明：我们定义必须出现的关键标识词，覆盖四大维度和硬门阈值。
# 逻辑：逐项 grep，任一缺失立即失败，保障文档结构稳定。
REQUIRED_MARKERS=(
  "DX 体验"
  "Robustness"
  "Operability"
  "Performance"
  "TCK=必过"
  "BoxService P99"
  "Cancel 信号"
  "宏分配"
  "RetryAfter"
  "epoch/config_epoch"
  "9.8 / 10"
)

export LC_ALL=C

for marker in "${REQUIRED_MARKERS[@]}"; do
  if ! grep -Fq "${marker}" "${SCORECARD_PATH}"; then
    echo "[scorecard] 文档缺少关键段落: ${marker}" >&2
    exit 1
  fi
done

echo "scorecard 校验通过"
