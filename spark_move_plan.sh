#!/usr/bin/env bash
# ================================= 教案级注释 =================================
# 背景 (Why):
# 1. 早期目录结构将部分 `spark-*` crate 置于 `crates/<域>/` 的二级目录中, 使得工
#    具难以通过前缀枚举所有实现层模块。
# 2. 本脚本用于一次性迁移, 将旧的二级目录结构扁平化为 `crates/spark-*` 顶层, 让
#    后续的构建脚本与治理工具能够通过统一规则定位 crate。
# 3. 迁移策略与新版架构文档保持一致, 即运行时、传输、编解码等实现层全部以
#    `crates/spark-*` 形式存在, 三方依赖继续驻留在 `crates/3p/`。
#
# 契约 (What):
# - 输入: 在仓库根目录执行, 需要 git 工作区干净 (或开发者已知并接受文件的
#   `git mv` 覆盖行为)。
# - 前置条件: 当前目录必须包含 `Cargo.toml`、`spark-codec-rtp/` 等 crate 的旧路径。
# - 后置条件: 目标 crate 将位于 `crates/<domain>/...`, 原位置不再保留, git 历史
#   通过 `git mv` 保持迁移关系。
#
# 设计权衡与注意事项 (Trade-offs & Gotchas):
# - 使用 `git mv` 而不是 `mv`, 以保留历史并自动记录删除/新增操作。
# - 迁移阶段需确保源目录存在, 否则 `git mv` 会失败并终止脚本。
# - 目标路径统一位于 `crates/` 顶层, 无需额外创建二级目录; 若已有同名目录, 脚
#   本会覆盖并保留 git 历史。
# - 本脚本幂等性有限: 再次执行将因源目录缺失而失败, 因此只应用于一次迁移。
#
# 执行步骤 (How):
# 1. 验证当前目录存在 `Cargo.toml`, 以确认在仓库根目录运行。
# 2. 针对 `MOVE_PLAN` 中的每个键值对, 创建目标目录、调用 `git mv -f` 迁移。
# 3. 遇到缺少源目录时立即失败, 防止部分迁移导致仓库不一致。
# ==============================================================================
set -euo pipefail

# 第 1 步: 校验执行位置, 避免在错误目录运行导致大规模误删。
if [[ ! -f Cargo.toml ]]; then
  echo "[错误] 请在仓库根目录执行本脚本 (缺少 Cargo.toml)." >&2
  exit 1
fi

# 第 2 步: 定义迁移映射表, 键为旧路径, 值为新路径。
# 说明:
# - 运行时相关 crate 目前仅有 `spark-core`, 暂定位于 `crates/spark-core` 层。
# - 传输层与编解码器统一迁移到 `crates/spark-*` 顶层, 摆脱历史上的二级目录。
declare -A MOVE_PLAN=(
  ["crates/codecs/spark-codec-line"]="crates/spark-codec-line"
  ["crates/codecs/spark-codec-rtcp"]="crates/spark-codec-rtcp"
  ["crates/codecs/spark-codec-rtp"]="crates/spark-codec-rtp"
  ["crates/codecs/spark-codec-sdp"]="crates/spark-codec-sdp"
  ["crates/codecs/spark-codec-sip"]="crates/spark-codec-sip"
  ["spark-core"]="crates/spark-core"
  ["crates/transport/spark-transport-tcp"]="crates/spark-transport-tcp"
  ["crates/transport/spark-transport-udp"]="crates/spark-transport-udp"
  ["crates/transport/spark-transport-tls"]="crates/spark-transport-tls"
  ["crates/transport/spark-transport-quic"]="crates/spark-transport-quic"
)

# 第 3 步: 执行迁移。
for SRC in "${!MOVE_PLAN[@]}"; do
  DEST="${MOVE_PLAN[${SRC}]}"
  if [[ ! -d "${SRC}" ]]; then
    echo "[错误] 源目录不存在: ${SRC}." >&2
    exit 1
  fi
  mkdir -p "$(dirname "${DEST}")"
  echo "[INFO] git mv -f ${SRC} ${DEST}"
  git mv -f "${SRC}" "${DEST}"
done

echo "[INFO] 迁移完成, 请手动更新 Cargo.toml 等配置中的路径。"
