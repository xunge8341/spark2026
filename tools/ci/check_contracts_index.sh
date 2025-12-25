#!/usr/bin/env bash
# 教案级注释：P0-04 契约索引覆盖率守门脚本
# -----------------------------------------------------------------------------
# 目标（Why）
# - 确保 `docs/contracts-index.md` 中的“文档→Rustdoc→代码→TCK”索引与公开 API 一致。
# - 当 `spark-core` 暴露新的 trait/类型但未在索引登记时立即阻断 CI，防止契约失真。
# - 为治理团队提供覆盖率指标（≥95%），衡量索引与真实 API 的贴合程度。
#
# 契约（What）
# - 输入：无显式参数；可通过环境变量覆盖行为：
#   * `CONTRACTS_INDEX_MIN_COVERAGE`：最低覆盖率阈值（默认 0.95）。
# - 前置条件：
#   * 已安装 `cargo-public-api`（需要 nightly toolchain）。
#   * 当前工作目录为仓库根；`docs/contracts-index.md` 含 `<!-- contracts-index:start -->` 标记。
# - 后置条件：
#   * 输出全局覆盖率及逐项缺失列表；
#   * 若覆盖率不足或存在未索引项，脚本以非零码退出。
#
# 执行流程（How）
# 1. 校验依赖：`cargo-public-api` 与 `python3` 均可用。
# 2. 分别运行 `cargo +nightly public-api -p spark-core --simplified` 与
#    `cargo +nightly public-api -p spark-router --simplified`，将公开 API 序列写入临时文件。
# 3. 由 Python 完成核心分析：
#    - 解析 `contracts-index.md` 的 `toml` 数据块，收集每个词条的 `covers`、`rustdoc`、`examples`、`tck`。
#    - 解析 public-api 输出，筛选 `struct`/`enum`/`trait`，并按词条映射（Buffer/Frame/…）。
#    - 计算覆盖率，找出缺失条目与冗余条目。
# 4. 打印诊断信息，若覆盖率低于阈值或存在缺失条目则阻断 CI。
#
# 权衡与注意事项（Trade-offs）
# - 使用 `cargo-public-api` 需要 nightly toolchain，CI 已预装；本地若缺失需手动安装。
# - 仅统计 `struct`/`enum`/`trait`，忽略关联类型/函数，聚焦契约面。
# - 冗余条目只给出警告，不阻断，便于逐步清理历史别名。
# -----------------------------------------------------------------------------

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
readonly INDEX_FILE="${REPO_ROOT}/docs/contracts-index.md"
readonly COVERAGE_THRESHOLD="${CONTRACTS_INDEX_MIN_COVERAGE:-0.95}"

log() {
  printf '[contracts-index][%s] %s\n' "$1" "$2"
}

ensure_tools() {
  command -v cargo-public-api >/dev/null || {
    log ERROR "未找到 cargo-public-api，请运行 'cargo install cargo-public-api'。"
    exit 127
  }
  command -v python3 >/dev/null || {
    log ERROR "未找到 python3，无法解析索引数据。"
    exit 127
  }
}

run_public_api() {
  local output_core output_router
  output_core="$(mktemp)"
  output_router="$(mktemp)"
  # Why: 生成多个 crate 的 API 快照需要统一清理策略，防止 CI 遗留临时文件。
  # How: 使用 trap 捕获函数返回点，统一删除 `mktemp` 创建的文件。
  # What: 在函数退出时确保 `spark-core` 与 `spark-router` 的 public-api 输出均被清理。
  trap 'rm -f -- "$output_core" "$output_router"' RETURN

  log INFO "生成 spark-core 公共 API 列表（使用 nightly toolchain）。"
  if ! cargo +nightly public-api -p spark-core --simplified --color never >"$output_core"; then
    log ERROR "cargo public-api 执行失败，请检查编译日志。"
    exit 1
  fi

  log INFO "生成 spark-router 公共 API 列表（使用 nightly toolchain）。"
  if ! cargo +nightly public-api -p spark-router --simplified --color never >"$output_router"; then
    log ERROR "cargo public-api (spark-router) 执行失败，请检查编译日志。"
    exit 1
  fi

  python3 - "$INDEX_FILE" "$COVERAGE_THRESHOLD" \
    spark-core "$output_core" \
    spark-router "$output_router" <<'PY'
import re
import sys
from pathlib import Path

index_path, threshold_str, *rest = sys.argv[1:]
threshold = float(threshold_str)

if len(rest) % 2 != 0:
    raise SystemExit("public-api 输出参数必须成对出现：<crate> <path>")

# Why: Router 契约已经拆分至 `spark-router` crate，脚本需要一次性聚合多 crate 的公开 API。
# What: `sources` 存储 (crate, 路径) 元组，供下游统一遍历。
# How: 解析命令行参数时按两个一组组合，保证顺序与 `cargo public-api` 调用一致。
sources = []
for crate, path in zip(rest[::2], rest[1::2]):
    sources.append((crate, Path(path)))

text = Path(index_path).read_text(encoding="utf-8")
start_marker = "<!-- contracts-index:start -->"
end_marker = "<!-- contracts-index:end -->"
if start_marker not in text or end_marker not in text:
    raise SystemExit("contracts-index.md 缺少 contracts-index 标记")
start = text.index(start_marker)
end = text.index(end_marker, start)
block = text[start:end]
if "```toml" not in block:
    raise SystemExit("contracts-index.toml 数据块缺失")
toml_body = block.split("```toml", 1)[1].split("```", 1)[0]

import tomllib
try:
    parsed = tomllib.loads(toml_body)
except tomllib.TOMLDecodeError as exc:
    raise SystemExit(f"无法解析 contracts-index toml: {exc}")

contracts = parsed.get("contract")
if not isinstance(contracts, list) or not contracts:
    raise SystemExit("contracts-index.toml 缺少 [[contract]] 定义")

covers_map: dict[str, set[str]] = {}
missing_fields: list[str] = []
for entry in contracts:
    term = entry.get("term")
    if not isinstance(term, str):
        missing_fields.append("缺少 term")
        continue
    covers = entry.get("covers")
    rustdoc = entry.get("rustdoc", [])
    examples = entry.get("examples", [])
    tck = entry.get("tck", [])
    if not covers:
        missing_fields.append(f"{term}: covers 为空")
        covers_set: set[str] = set()
    else:
        covers_set = {str(item) for item in covers}
    for label, payload in (("rustdoc", rustdoc), ("examples", examples), ("tck", tck)):
        if not payload:
            missing_fields.append(f"{term}: {label} 为空")
    covers_map[term] = covers_set

if missing_fields:
    raise SystemExit("索引元数据缺失: " + "; ".join(missing_fields))

pattern = re.compile(r"^pub\s+(trait|struct|enum)\s+([^\s]+)")
actual_map: dict[str, set[str]] = {key: set() for key in covers_map}
TARGET_PREFIXES = {
    "Buffer": ["spark_core::buffer::"],
    "Frame": ["spark_core::protocol::Frame"],
    "Codec": ["spark_core::codec::"],
    "Transport": ["spark_core::transport::"],
    "Pipeline": ["spark_core::pipeline::"],
    "PipelineInitializer": [
        "spark_core::pipeline::PipelineInitializer",
        "spark_core::pipeline::initializer::",
    ],
    "Service": ["spark_core::service::"],
    "Router": ["spark_router::", "spark_core::router::"],
    "Context": [
        "spark_core::context::",
        "spark_core::contract::CallContext",
        "spark_core::contract::CallContextBuilder",
        "spark_core::contract::Cancellation",
        "spark_core::contract::Deadline",
        "spark_core::contract::SecurityContextSnapshot",
    ],
    "Error": ["spark_core::error::"],
    "State": ["spark_core::model::", "spark_core::status::"],
}

for _crate, api_path in sources:
    # Why: 需要同时分析 spark-core 与 spark-router 的 API 列表，确保索引覆盖率统计包含新 crate。
    # How: 逐文件读取 `cargo public-api` 输出，匹配公开的 struct/enum/trait 声明并做前缀分类。
    # What: 将命中前缀的符号写入 `actual_map`，后续与 contracts-index 进行比对。
    with api_path.open(encoding="utf-8") as handle:
        for raw in handle:
            raw = raw.strip()
            match = pattern.match(raw)
            if not match:
                continue
            _, path = match.groups()
            canonical = path.split("<", 1)[0]
            if canonical.endswith(":"):
                canonical = canonical[:-1]
            if canonical.count("::") < 2:
                continue
            for term, prefixes in TARGET_PREFIXES.items():
                if any(canonical.startswith(prefix) for prefix in prefixes):
                    actual_map.setdefault(term, set()).add(canonical)
                    break

overall_total = 0
overall_matched = 0
missing_by_term: dict[str, list[str]] = {}
extra_by_term: dict[str, list[str]] = {}

for term, actual in actual_map.items():
    if not actual:
        continue
    covered = covers_map.get(term, set())
    overall_total += len(actual)
    overall_matched += len(actual & covered)
    missing = sorted(actual - covered)
    if missing:
        missing_by_term[term] = missing
    extra = sorted(covered - actual)
    if extra:
        extra_by_term[term] = extra

coverage = (overall_matched / overall_total) if overall_total else 1.0
print(f"[contracts-index][INFO] 总覆盖率: {coverage:.4f} ({overall_matched}/{overall_total})")
for term in sorted(actual_map):
    actual = actual_map[term]
    if not actual:
        continue
    covered = covers_map.get(term, set())
    ratio = (len(actual & covered) / len(actual)) if actual else 1.0
    print(f"[contracts-index][INFO] {term}: {ratio:.4f} ({len(actual & covered)}/{len(actual)})")

if extra_by_term:
    print("[contracts-index][WARN] 索引存在可能的冗余条目：")
    for term, extras in extra_by_term.items():
        preview = ", ".join(extras[:3])
        suffix = "..." if len(extras) > 3 else ""
        print(f"  - {term}: {preview}{suffix}")

if missing_by_term:
    print("[contracts-index][ERROR] 检测到未索引的公开 API：")
    for term, missing in missing_by_term.items():
        preview = ", ".join(missing[:5])
        suffix = "..." if len(missing) > 5 else ""
        print(f"  - {term}: {preview}{suffix}")

if coverage + 1e-9 < threshold:
    print(f"[contracts-index][ERROR] 覆盖率 {coverage:.4f} 低于阈值 {threshold:.2f}。")
    raise SystemExit(1)

if missing_by_term:
    raise SystemExit(2)

print("[contracts-index][INFO] 契约索引覆盖率守门通过。")
PY

  trap - RETURN
}

cd -- "$REPO_ROOT"
ensure_tools
run_public_api
log INFO "检查完成。"
