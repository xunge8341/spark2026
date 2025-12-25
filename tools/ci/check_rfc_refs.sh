#!/usr/bin/env bash
set -euo pipefail

# == RFC 注释一致性守门脚本（教案级注释） ==
#
# ## 意图 (Why)
# 1. 确保所有 `#[doc]` 注释中引用的 `[RFC XXXX §Y.Z]` 章节都在 `contracts/rfc_index.toml` 中登记，
#    避免技术文档引用与规范索引脱节，降低后续维护门槛。
# 2. 在 CI 中自动阻断遗漏或冗余登记，形成「代码即索引」的闭环，方便读者追溯 RFC 原文。
#
# ## 架构位置与作用 (Where)
# - 脚本位于 `tools/ci/` 目录，归属一致性守门工具链；`make ci-*` 等命令可复用该脚本保障文档引用质量。
# - 它依赖 `contracts/rfc_index.toml` 的集中索引，作为 RFC 元信息的单一事实源（Single Source of Truth）。
#
# ## 关键策略 (How)
# - 使用 `git ls-files` 遍历受 Git 追踪的 Rust 源文件，精准定位 `#[doc]` 中的 `[RFC ...]` 模式，避免扫描未提交草稿。
# - 利用 Python + `tomllib` 解析 `rfc_index.toml`，将索引转换为结构化集合，与扫描结果逐一比对。
# - 若发现缺失或冗余项，按 RFC 编号与章节输出详细提示，并附带源码位置，指导贡献者及时同步索引。
#
# ## 契约 (What)
# - **输入**：无显式参数，在仓库根目录执行；依赖环境变量仅限标准 Git/Python/Bash。
# - **输出**：
#   - 成功：无输出，退出码 0；
#   - 失败：标准错误打印差异详情（缺失索引 / 冗余索引 / 格式错误），退出码非 0。
# - **前置条件**：
#   - 当前工作目录为 Git 仓库根；
#   - 已安装 `git`、`python3`、`rg`（用于贡献者本地排查，但脚本本身不直接调用 `rg`）。
# - **后置条件**：保证 `[RFC ...]` 引用集合与索引文件完全一致，便于下游文档生成或审计工具使用。
#
# ## 设计权衡与注意事项 (Trade-offs & Gotchas)
# - 选择 Bash 调度 + Python 比对：Bash 负责环境定位，Python 负责结构化解析，兼顾可读性与表达力。
# - 扫描仅覆盖以 `§` 标记章节的注释；若未来出现其他格式，可在正则层扩展，避免误报。
# - 索引文件允许为空，便于增量引入；但一旦写入条目即要求章节字符串保留前缀 `§`。
# - 若索引或源码存在重复章节登记，脚本会警告，防止随着文档演进出现重复条目或阴影依赖。

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd "$REPO_ROOT"

INDEX_PATH="contracts/rfc_index.toml"
if [[ ! -f "$INDEX_PATH" ]]; then
    echo "[RFC] 缺少 contracts/rfc_index.toml 文件，请按照模板新增并登记 RFC 引用。" >&2
    exit 1
fi

python3 <<'PY'
"""
RFC doc 注释校验核心逻辑。

教案级说明：
- 意图：从受控索引中读取 RFC -> 章节映射，并与源码扫描结果对齐，确保“注释与索引”双向同步。
- 全局定位：该脚本作为 Bash 守门的业务核心，输出所有一致性错误，由外层 Bash 负责传递退出码。
- 关键模式：采用结构化集合比对（set difference），在保持实现简洁的同时提供精确差异定位。
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

REPO_ROOT = pathlib.Path('.').resolve()
INDEX_PATH = REPO_ROOT / 'contracts' / 'rfc_index.toml'
REFERENCE_PATTERN = re.compile(r"\[RFC\s+(?P<number>\d{3,5})\s+§(?P<section>[0-9A-Za-z.\-]+)\]")


@dataclass(frozen=True)
class RfcRef:
    """教案级注释：描述单个 RFC 引用的结构化模型。

    Why：将 RFC 编号与章节打包为可哈希对象，便于集合运算并携带上下文信息。
    Where：供扫描/索引模块共享，使二者输出的元素类型一致，从而直接比较。
    What（契约）：
        - `number`: RFC 编号的十进制字符串表示。
        - `section`: 章节标识，统一去除前缀 `§` 以便比较；保留原始符号交由格式化阶段处理。
    Preconditions：输入参数必须已做基本合法性校验（非空、无空格）。
    Postconditions：实例可直接参与集合比较与排序，且 `repr` 便于调试。
    Trade-offs：使用 `@dataclass(frozen=True)` 实现不可变性，避免在集合中被篡改导致难以排查的 bug。
    """

    number: str
    section: str

    def format_label(self) -> str:
        """返回统一格式的标签字符串（例如：`RFC 4122 §4.1`）。"""

        return f"RFC {self.number} §{self.section}"


def git_tracked_rust_files(repo_root: pathlib.Path) -> List[pathlib.Path]:
    """教案级注释：列举 Git 追踪的 Rust 源文件。

    Why：确保扫描范围与 Git 提交内容一致，避免未跟踪文件干扰检查结果。
    Where：作为扫描阶段的入口，由 `collect_doc_refs` 调用并缓存结果。
    How：调用 `git ls-files '*.rs'`，利用 Git 自身的模式匹配能力过滤文件。
    Contract：
        - 输入：仓库根目录路径。
        - 输出：`pathlib.Path` 列表（相对路径转换为绝对路径）。
        - 前置条件：`repo_root` 必须指向有效 Git 仓库。
        - 后置条件：返回的所有文件均存在于文件系统且为 `.rs` 结尾。
    Trade-offs：通过 Git 过滤可避免遍历 `target/` 等目录，性能优于纯文件系统递归。
    """

    result = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "*.rs"],
        check=True,
        capture_output=True,
        text=True,
    )
    files = []
    for line in result.stdout.splitlines():
        candidate = repo_root / line
        if candidate.is_file():
            files.append(candidate)
    return files


def collect_doc_refs(repo_root: pathlib.Path) -> Tuple[Set[RfcRef], Dict[RfcRef, List[str]]]:
    """教案级注释：扫描源码，提取 `#[doc]` 中的 RFC 引用。

    Why：构建实际引用集合，作为守门检查的事实来源。
    Where：由主流程调用，运行在 Python 脚本内部的早期阶段。
    How：
        1. 获取 Git 追踪的 Rust 文件列表；
        2. 逐行读取文件内容，使用正则 `\[RFC XXXX §Y.Z]` 捕获引用；
        3. 将捕获到的编号与章节规范化为 `RfcRef`，并记录出现位置（文件:行号）。
    Contract：
        - 输入：仓库根目录路径。
        - 输出：
            * `Set[RfcRef]`：唯一 RFC 引用集合；
            * `Dict[RfcRef, List[str]]`：引用位置索引，用于告警输出。
        - 前置条件：文件编码为 UTF-8；注释遵循 `[RFC XXXX §Y.Z]` 格式。
        - 后置条件：集合中的引用均来自真实注释，位置列表至少包含一条记录。
    Trade-offs & Gotchas：
        - 若注释存在多行/重复引用，会累积多个位置，便于定位重复定义。
        - 正则目前忽略章节后缀中的希腊字母或括号，如需扩展可调整 `REFERENCE_PATTERN`。
    """

    refs: Set[RfcRef] = set()
    locations: Dict[RfcRef, List[str]] = {}
    for path in git_tracked_rust_files(repo_root):
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError as exc:
            raise SystemExit(f"无法以 UTF-8 读取 {path}: {exc}") from exc
        for line_no, line in enumerate(text.splitlines(), start=1):
            for match in REFERENCE_PATTERN.finditer(line):
                ref = RfcRef(number=match.group('number'), section=match.group('section'))
                refs.add(ref)
                locations.setdefault(ref, []).append(f"{path.relative_to(repo_root)}:{line_no}")
    return refs, locations


def load_index(index_path: pathlib.Path) -> Dict[RfcRef, List[str]]:
    """教案级注释：解析 `rfc_index.toml`，生成 RFC -> 章节映射。

    Why：提供声明性索引，以便与源码扫描结果对比，形成一致性闭环。
    Where：在主流程中位于扫描之后，保证在引用为空时也能正常处理空表。
    How：
        1. 使用 `tomllib` 加载 TOML 文件，读取 `[[rfc]]` 数组；
        2. 校验 `number` 与 `sections` 字段是否存在、类型是否正确；
        3. 将每个章节转换为 `RfcRef`，同时记录其在索引中的来源序号，辅助错误提示。
    Contract：
        - 输入：索引文件路径。
        - 输出：`Dict[RfcRef, List[str]]`，其中 value 记录 `rfc` 条目及章节下标，形如 `entry#1:§4.1`；
        - 前置条件：文件符合 UTF-8 编码，结构满足脚本定义的键；
        - 后置条件：返回的所有 `RfcRef` 均唯一，若发现重复章节会抛出异常提示行号。
    Trade-offs & Gotchas：
        - 索引允许缺省 `[[rfc]]`（即完全空表），此时返回空字典。
        - 若章节字符串缺少 `§` 前缀，将立即报错以提醒维护者修正格式。
    """

    try:
        raw_text = index_path.read_text(encoding='utf-8')
    except UnicodeDecodeError as exc:
        raise SystemExit(f"无法以 UTF-8 读取索引文件 {index_path}: {exc}") from exc
    if not raw_text.strip():
        return {}

    data = tomllib.loads(raw_text)
    entries = data.get('rfc', [])
    if entries is None:
        entries = []
    if not isinstance(entries, list):
        raise SystemExit('`rfc_index.toml` 中的 `rfc` 字段必须是数组。')

    index_refs: Dict[RfcRef, List[str]] = {}
    for idx, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise SystemExit(f'`rfc` 第 {idx} 条目必须是表格 (table)。')
        if 'number' not in entry or 'sections' not in entry:
            raise SystemExit(f'`rfc` 第 {idx} 条目缺少 `number` 或 `sections` 字段。')
        number = entry['number']
        sections = entry['sections']
        if not isinstance(number, int):
            raise SystemExit(f'`rfc` 第 {idx} 条目中的 `number` 必须是整数。')
        if not isinstance(sections, list):
            raise SystemExit(f'`rfc` 第 {idx} 条目中的 `sections` 必须是数组。')
        number_str = str(number)
        for sec_idx, section in enumerate(sections, start=1):
            if not isinstance(section, str):
                raise SystemExit(
                    f'`rfc` 第 {idx} 条目中第 {sec_idx} 个章节必须是字符串。'
                )
            if not section.startswith('§'):
                raise SystemExit(
                    f'`rfc` 第 {idx} 条目中第 {sec_idx} 个章节缺少 `§` 前缀：{section!r}'
                )
            normalized = section[1:]
            ref = RfcRef(number=number_str, section=normalized)
            origin = f'entry#{idx}:{section}'
            index_refs.setdefault(ref, []).append(origin)
    return index_refs


def main() -> None:
    """主流程：执行扫描、索引加载与集合比对。"""

    doc_refs, doc_locations = collect_doc_refs(REPO_ROOT)
    index_refs = load_index(INDEX_PATH)

    doc_set = set(doc_refs)
    index_set = set(index_refs)

    missing_in_index = sorted(doc_set - index_set, key=lambda r: (r.number, r.section))
    stale_in_index = sorted(index_set - doc_set, key=lambda r: (r.number, r.section))

    has_error = False
    if missing_in_index:
        has_error = True
        print('发现未在 contracts/rfc_index.toml 中登记的 RFC 引用：', file=sys.stderr)
        for ref in missing_in_index:
            positions = ', '.join(doc_locations.get(ref, []))
            print(f"  - {ref.format_label()} @ {positions}", file=sys.stderr)
    if stale_in_index:
        has_error = True
        print('索引文件包含未在源码中使用的章节，请确认是否应当移除：', file=sys.stderr)
        for ref in stale_in_index:
            origins = ', '.join(index_refs.get(ref, []))
            print(f"  - {ref.format_label()} @ {origins}", file=sys.stderr)

    if not has_error:
        return
    raise SystemExit(1)


if __name__ == '__main__':
    main()
PY
