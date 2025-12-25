#!/usr/bin/env python3
"""检查仓库内各 Rust crate 的文档密度，并校验 TODO/FIXME/HACK 汇总。

本脚本主要承担两项职责：
1. 统计并校验工作区内每个 crate 的文档注释密度是否达到设定阈值；
2. 生成或校验 docs/TODO.md 文件，确保所有 TODO/FIXME/HACK 均被列出。

脚本默认在“检查”模式下运行，不会写入任何文件。当检测到文档密度不足或
TODO 汇总未同步时会以非零状态码退出，用于 CI 门禁。通过 `--update-todo`
参数可以在本地重新生成 TODO 汇总后提交。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

# ---------------------------- 常量定义区域 -----------------------------

# 文档注释的阈值，默认要求文档行数至少占代码行数的 6%。
DEFAULT_DENSITY_THRESHOLD: float = 0.06

# 视为文档注释的前缀集合，支持行内(`///`)和模块级(`//!`)写法。
DOC_PREFIXES: Tuple[str, ...] = ("///", "//!")

# 需要忽略的目录名称集合，避免统计到构建产物或第三方依赖内容。
IGNORED_DIRECTORIES: Tuple[str, ...] = (
    ".git",
    "target",
    "node_modules",
    "dist",
    "__pycache__",
    "generated",
)

# 视为二进制文件或无需扫描待办标记的后缀集合，减少误报与性能开销。
BINARY_SUFFIXES: Tuple[str, ...] = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".ico",
    ".pdf",
    ".zip",
    ".gz",
    ".xz",
    ".bz2",
    ".tar",
    ".wasm",
    ".ttf",
    ".otf",
)

# 需要捕获的关键标记集合，未来如需扩展可以在此追加。
TODO_MARKERS: Tuple[str, ...] = ("TODO", "FIXME", "HACK")

# 汇总文档的固定提示语，用于校验文件内容是否过期。
TODO_HEADER: str = "# TODO/FIXME/HACK 汇总\n\n> 本文件由 `tools/check_doc_density.py` 自动生成。请勿手动编辑。\n"


@dataclass
class CrateDensity:
    """记录单个 crate 的文档密度统计信息。"""

    name: str
    doc_lines: int
    code_lines: int

    @property
    def density(self) -> float:
        """计算文档密度。

        返回值为文档行数与代码行数的比值。当 `code_lines` 为 0 时，为避免
        除零错误，同时也为了在空 crate 中鼓励撰写文档，本方法约定返回 1.0，
        视为空 crate 已达到文档要求。
        """

        if self.code_lines == 0:
            return 1.0
        return self.doc_lines / self.code_lines


@dataclass
class TodoEntry:
    """记录单条 TODO/FIXME/HACK 的元数据。"""

    path: Path
    line_number: int
    marker: str
    line_text: str

    def to_markdown_row(self, workspace_root: Path) -> str:
        """将条目渲染为 Markdown 表格行。"""

        relative_path = self.path.relative_to(workspace_root)
        snippet = self.line_text.strip().replace("|", "\\|")
        return f"| `{relative_path}` | {self.line_number} | {self.marker} | {snippet} |"


# ---------------------------- 工具函数区域 -----------------------------


def parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    """教案级说明：

    - **意图（Why）**：集中描述 CLI 层级的开关，确保在不同 CI/本地场景下使用者
      都能调整阈值或触发 TODO 文档更新。
    - **逻辑（How）**：构造 `ArgumentParser`，依次注册三个选项，并返回解析结果。
    - **契约（What）**：
      - 输入：`argv` 为命令行参数序列；
      - 前置条件：`argv` 可迭代，内部元素为字符串；
      - 后置条件：返回值是解析后的 `Namespace`，至少包含 `threshold`、
        `update_todo` 与 `workspace` 字段。
    - **权衡（Trade-offs）**：解析逻辑保持最小依赖，不引入子命令体系，以降低脚本
      在 CI 中的使用门槛。
    """

    parser = argparse.ArgumentParser(description="检查文档密度并同步 TODO 汇总")
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_DENSITY_THRESHOLD,
        help="文档密度阈值，默认为 0.06",
    )
    parser.add_argument(
        "--update-todo",
        action="store_true",
        help="同步生成 docs/TODO.md 文件",
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="工作区根目录，通常无需指定",
    )
    return parser.parse_args(argv)


def run_cargo_metadata(workspace_root: Path) -> dict:
    """教案级说明：

    - **意图**：获取工作区所有 crate 的元信息，以便确定统计对象与根目录。
    - **逻辑**：通过 `subprocess.run` 调用 `cargo metadata`，禁用依赖项解析以提升
      性能，随后解析 JSON 输出。
    - **契约**：
      - 输入：`workspace_root` 为仓库根路径；
      - 前置条件：调用环境需安装 Rust toolchain 并能执行 `cargo`；
      - 后置条件：返回与 cargo 版本兼容的 JSON 字典，至少包含 `packages` 列表。
    - **权衡**：选择使用官方命令而非手写解析 `Cargo.toml`，避免处理 workspace 配置
      的边界情况。
    """

    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=workspace_root,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return json.loads(result.stdout)


def iter_rust_files(crate_root: Path) -> Iterable[Path]:
    """教案级说明：

    - **意图**：生成用于统计的 Rust 源文件集合。
    - **逻辑**：利用 `Path.rglob` 遍历 `*.rs` 文件，并跳过构建产物目录。
    - **契约**：
      - 输入：`crate_root` 为单个 crate 的根目录；
      - 前置条件：目录存在；
      - 后置条件：迭代器产出所有有效 Rust 文件路径，忽略被列入 `IGNORED_DIRECTORIES`
        的子树。
    - **权衡**：保持懒加载迭代器，避免一次性加载所有路径导致内存峰值。
    """

    for path in crate_root.rglob("*.rs"):
        if any(part in IGNORED_DIRECTORIES for part in path.parts):
            continue
        yield path


def count_doc_density(files: Iterable[Path]) -> Tuple[int, int]:
    """教案级说明：

    - **意图**：对单个 crate 的所有源文件执行逐行统计，得到文档与代码的行数。
    - **逻辑**：逐文件读取文本，按行剔除空白与普通注释，仅保留 `///`/`//!` 记为
      文档行，其余有效内容归类为代码行。
    - **契约**：
      - 输入：`files` 为 Rust 源文件路径的可迭代对象；
      - 前置条件：文件必须可读；
      - 后置条件：返回 `(doc_lines, code_lines)` 二元组，元素为非负整数。
    - **权衡**：为避免将大段块注释误算为代码行，直接忽略 `/* ... */` 前缀，宁可
      少计也不夸大文档密度。
    """

    doc_lines = 0
    code_lines = 0

    for file_path in files:
        try:
            content = file_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            content = file_path.read_text(encoding="utf-8", errors="ignore")

        for line in content.splitlines():
            stripped = line.lstrip()
            if not stripped:
                continue
            if stripped.startswith(DOC_PREFIXES):
                doc_lines += 1
                continue
            if stripped.startswith("//"):
                continue
            if stripped.startswith("/*"):
                # 对块注释直接忽略，避免将大段注释计入有效代码行。
                continue
            code_lines += 1

    return doc_lines, code_lines


def collect_crate_densities(metadata: dict, workspace_root: Path) -> List[CrateDensity]:
    """教案级说明：

    - **意图**：生成全工作区的文档密度列表，为最终报告提供数据来源。
    - **逻辑**：遍历 `metadata['packages']`，对每个包调用 `iter_rust_files` 与
      `count_doc_density`，并以 crate 名称排序。
    - **契约**：
      - 输入：`metadata` 为 cargo 输出的 JSON；`workspace_root` 为工作区根目录；
      - 前置条件：`metadata` 必须包含 `packages` 字段；
      - 后置条件：返回 `CrateDensity` 列表，包含所有 crate 的统计结果。
    - **权衡**：排序采用大小写不敏感方式，确保报告在不同平台输出保持稳定。
    """

    densities: List[CrateDensity] = []
    for package in metadata.get("packages", []):
        manifest_path = Path(package["manifest_path"]).resolve()
        crate_root = manifest_path.parent
        files = list(iter_rust_files(crate_root))
        doc_lines, code_lines = count_doc_density(files)
        densities.append(
            CrateDensity(
                name=package["name"],
                doc_lines=doc_lines,
                code_lines=code_lines,
            )
        )
    densities.sort(key=lambda item: item.name.lower())
    return densities


def print_density_report(densities: Sequence[CrateDensity], threshold: float) -> bool:
    """教案级说明：

    - **意图**：以人类可读的表格展示统计结果，同时计算是否全部通过阈值。
    - **逻辑**：打印标题与分隔线后，逐条输出密度数据，并记录是否存在未达标项。
    - **契约**：
      - 输入：`densities` 为统计结果序列；`threshold` 为浮点阈值；
      - 前置条件：`threshold` 介于 0 和 1 之间；
      - 后置条件：返回布尔值指示是否所有 crate 均达标。
    - **权衡**：为兼顾 CI 日志可读性，使用固定宽度格式化，而非额外依赖第三方库。
    """

    header = f"{'crate':30} | {'doc_lines':10} | {'code_lines':11} | density | status"
    separator = "-" * len(header)
    print("文档密度阈值: {:.2%}".format(threshold))
    print(header)
    print(separator)

    all_passed = True
    for item in densities:
        status = "PASS" if item.density >= threshold else "FAIL"
        if status == "FAIL":
            all_passed = False
        print(
            f"{item.name:30} | {item.doc_lines:10d} | {item.code_lines:11d} | "
            f"{item.density:7.3%} | {status}"
        )

    return all_passed


def should_skip_path(path: Path) -> bool:
    """教案级说明：

    - **意图**：统一决定哪些路径不参与 TODO 扫描，避免二进制文件或生成文件污染结果。
    - **逻辑**：根据目录名称、文件后缀以及特例 `docs/TODO.md` 进行过滤。
    - **契约**：
      - 输入：`path` 为待判断的文件或目录；
      - 前置条件：路径可以不存在；
      - 后置条件：返回布尔值，`True` 表示应跳过。
    - **权衡**：宁可少扫一些潜在文件，也不希望在二进制文件上浪费时间或触发解码错误。
    """

    if path.is_dir():
        return path.name in IGNORED_DIRECTORIES
    if path.suffix.lower() in BINARY_SUFFIXES:
        return True
    if path.name == "TODO.md" and path.parent.name == "docs":
        return True
    return False


def scan_todo_entries(workspace_root: Path) -> List[TodoEntry]:
    """教案级说明：

    - **意图**：生成需要在 TODO 文档中记录的待办条目清单。
    - **逻辑**：使用 `os.walk` 深度遍历仓库目录，借助 `should_skip_path` 控制剪枝，
      并逐行匹配可执行的标记。
    - **契约**：
      - 输入：`workspace_root` 为仓库根路径；
      - 前置条件：路径存在且可读；
      - 后置条件：返回经排序的 `TodoEntry` 列表，包含路径与行号。
    - **权衡**：选择在 Python 端解析而非依赖外部工具，保证不同平台输出一致且便于单测。
    """

    entries: List[TodoEntry] = []
    for root, dirnames, filenames in os.walk(workspace_root):
        root_path = Path(root)

        # 通过原地修改 dirnames 来阻止 os.walk 进入被忽略的目录。
        dirnames[:] = [d for d in dirnames if not should_skip_path(root_path / d)]

        for filename in filenames:
            file_path = root_path / filename
            if should_skip_path(file_path):
                continue
            try:
                lines = file_path.read_text(encoding="utf-8").splitlines()
            except UnicodeDecodeError:
                lines = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            for idx, line in enumerate(lines, start=1):
                for marker in TODO_MARKERS:
                    if marker in line and is_actionable_marker(line, file_path):
                        entries.append(
                            TodoEntry(
                                path=file_path,
                                line_number=idx,
                                marker=marker,
                                line_text=line,
                            )
                        )
                        break
    entries.sort(key=lambda item: (item.path.as_posix(), item.line_number))
    return entries


def is_actionable_marker(line: str, path: Path) -> bool:
    """教案级说明：

    - **意图**：过滤掉描述性文本中的“TODO”字样，只保留真正的待办事项。
    - **逻辑**：检查行首是否为常见注释前缀，或文件是否为 Markdown/RST 等文档类型。
    - **契约**：
      - 输入：原始文本行与其文件路径；
      - 前置条件：`line` 为完整文本；
      - 后置条件：返回布尔值表示是否需要纳入 TODO 汇总。
    - **权衡**：通过静态启发式实现“低成本过滤”，尚未处理复杂的跨行注释情形，必要
      时可在未来扩展。
    """

    stripped = line.lstrip()
    comment_prefixes = ("//", "/*", "*", "#", "<!--", "//!", "///")
    if stripped.startswith(comment_prefixes):
        return True

    markdown_like = {".md", ".markdown", ".mdx", ".rst"}
    if path.suffix.lower() in markdown_like:
        return True

    return False


def render_todo_markdown(entries: Sequence[TodoEntry], workspace_root: Path) -> str:
    """教案级说明：

    - **意图**：把待办条目转换成固定格式的 Markdown，便于审阅和比对。
    - **逻辑**：若无条目则输出提示；否则写入表头并逐条调用 `to_markdown_row`。
    - **契约**：
      - 输入：`entries` 为条目序列；`workspace_root` 用于生成相对路径；
      - 前置条件：条目元素完整；
      - 后置条件：返回稳定排序的 Markdown 字符串。
    - **权衡**：不引入模板引擎，保证生成内容可预测且便于校验。
    """

    if not entries:
        body = "当前仓库未发现 TODO/FIXME/HACK 标记。\n"
        return TODO_HEADER + "\n" + body

    lines = [
        TODO_HEADER,
        "| 文件 | 行号 | 类型 | 摘要 |",
        "| --- | --- | --- | --- |",
    ]
    for entry in entries:
        lines.append(entry.to_markdown_row(workspace_root))
    lines.append("")
    return "\n".join(lines)


def ensure_todo_doc(entries: Sequence[TodoEntry], workspace_root: Path, update: bool) -> bool:
    """教案级说明：

    - **意图**：确保 `docs/TODO.md` 与最新扫描结果保持一致，形成可靠的审计入口。
    - **逻辑**：计算期望内容，与现有文件对比；若 `--update-todo` 被指定则直接覆写。
    - **契约**：
      - 输入：`entries` 为待办列表；`workspace_root` 指向仓库；`update` 控制是否写入；
      - 前置条件：`workspace_root/docs` 目录可写；
      - 后置条件：返回布尔值表示文档是否已同步。
    - **权衡**：允许在检查模式下失败以提醒开发者手动更新，而非静默修复，避免 CI 隐性漂移。
    """

    docs_dir = workspace_root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    todo_path = docs_dir / "TODO.md"
    expected_content = render_todo_markdown(entries, workspace_root)

    if update:
        todo_path.write_text(expected_content, encoding="utf-8")
        print("已更新 docs/TODO.md。")
        return True

    if not todo_path.exists():
        print("docs/TODO.md 不存在，请运行 `python tools/check_doc_density.py --update-todo` 生成。", file=sys.stderr)
        return False

    current_content = todo_path.read_text(encoding="utf-8")
    if current_content != expected_content:
        print("docs/TODO.md 与实际扫描结果不一致，请运行 `python tools/check_doc_density.py --update-todo` 更新。", file=sys.stderr)
        return False

    return True


# ---------------------------- 程序入口区域 -----------------------------


def main(argv: Sequence[str]) -> int:
    """教案级说明：

    - **意图**：串联所有子步骤，形成既可本地运行又适合 CI 的一致性检查流程。
    - **逻辑**：解析参数、统计密度、扫描待办并根据模式选择更新或比对文档，最后按
      检查结果返回状态码。
    - **契约**：
      - 输入：`argv` 为命令行参数；
      - 前置条件：脚本运行目录需位于仓库内；
      - 后置条件：返回 `0` 表示全部通过，`1` 表示至少一项失败。
    - **权衡**：保持顺序执行和清晰的错误信息，避免在失败时仍然生成副作用，方便开发
      者快速定位问题。
    """

    args = parse_arguments(argv)
    workspace_root = args.workspace.resolve()

    metadata = run_cargo_metadata(workspace_root)
    densities = collect_crate_densities(metadata, workspace_root)
    densities_ok = print_density_report(densities, args.threshold)

    todo_entries = scan_todo_entries(workspace_root)
    todo_ok = ensure_todo_doc(todo_entries, workspace_root, args.update_todo)

    if densities_ok and todo_ok:
        print("所有检查通过。")
        return 0

    if not densities_ok:
        print("存在文档密度未达标的 crate。", file=sys.stderr)
    if not todo_ok:
        print("TODO 汇总文件需要更新。", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
