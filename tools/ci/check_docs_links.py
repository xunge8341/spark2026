#!/usr/bin/env python3
"""Spark 文档链接离线校验脚本。

教案级注释（Why/How/What/风险汇总）：
1. 目标（Why）
   - Gate-2 要求 Markdown 链接零错误，本脚本提供离线守门能力。
   - 通过收敛相对路径，避免文档中的 404 与冗余导航。
2. 架构定位（Where）
   - 脚本位于 `tools/ci/`，由 CI 与开发者通过 `python3 tools/ci/check_docs_links.py` 调用。
   - 作用于仓库内全部 Markdown（`docs/`、顶层 README、ADR 等）。
3. 关键逻辑（How）
   - 正则匹配行内链接 `[text](target)`，忽略外部链接与锚点。
   - 将相对链接解析为绝对路径并检测文件是否存在。
   - 汇总缺失项并以非零退出码提醒 CI 失败。
4. 契约（What）
   - 输入：仓库工作区（可通过环境变量 `SPARK_DOC_ROOT` 覆盖根目录）。
   - 输出：标准输出提示扫描结果；存在缺失链接时返回值为 1。
   - 前置：需要 Python 3.9+；仓库若非 Git 管理，会退回使用当前目录。
   - 后置：退出 0 表示所有相对链接均有效。
5. 权衡与风险（Trade-offs）
   - 仅校验文件存在性，不检查锚点与网络可达性。
   - 引用式链接（`[id]: path`）暂未覆盖，后续 Gate 可扩展。
6. 维护提示
   - 如需忽略生成目录，请在 `IGNORED_DIRS` 中补充。
   - 修改逻辑需同步更新本注释，以便新成员快速上手。
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

# --------------------------- 常量与配置（Contract 定义） ---------------------------
IGNORED_PREFIXES: Sequence[str] = ("http://", "https://", "mailto:", "tel:")
ANCHOR_PREFIX = "#"
IGNORED_DIRS: Sequence[str] = (".git", "target", "crates/3p/spark2026-fuzz/target")
MARKDOWN_PATTERN = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


@dataclass
class LinkViolation:
    """记录校验失败项，便于统一格式输出。

    字段说明：
    - `source`: Markdown 文件路径，相对于仓库根目录。
    - `target`: 链接原始目标文本。
    - `resolved`: 解析后的绝对路径，便于调试与溯源。
    """

    source: Path
    target: str
    resolved: Path


def discover_repo_root() -> Path:
    """定位仓库根目录，兼容非 Git 环境。"""

    try:
        output = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True)
        return Path(output.strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        return Path.cwd()


def should_ignore(path: Path, repo_root: Path) -> bool:
    """判断路径是否位于忽略目录中。"""

    relative = path.relative_to(repo_root)
    return any(part in IGNORED_DIRS for part in relative.parts)


def iter_markdown_files(root: Path) -> Iterable[Path]:
    """遍历仓库内的 Markdown 文件。"""

    for path in root.rglob("*.md"):
        if should_ignore(path, root):
            continue
        yield path


def extract_links(markdown: str) -> Iterable[str]:
    """从 Markdown 文本中解析行内链接目标。"""

    for match in MARKDOWN_PATTERN.finditer(markdown):
        yield match.group(1)


def is_relative_link(link: str) -> bool:
    """判定链接是否为需要校验的相对路径。"""

    if not link:
        return False
    if link.startswith(ANCHOR_PREFIX):
        return False
    if any(link.startswith(prefix) for prefix in IGNORED_PREFIXES):
        return False
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", link):
        return False
    return True


def resolve_link(source_file: Path, link: str) -> Path:
    """根据源文件解析相对链接到绝对路径。"""

    path_part = link.split(ANCHOR_PREFIX, 1)[0]
    return (source_file.parent / path_part).resolve()


def validate_links(files: Iterable[Path], repo_root: Path) -> List[LinkViolation]:
    """执行核心校验逻辑，返回所有缺失链接记录。"""

    violations: List[LinkViolation] = []
    for md_file in files:
        content = md_file.read_text(encoding="utf-8")
        for raw_link in extract_links(content):
            if not is_relative_link(raw_link):
                continue
            resolved = resolve_link(md_file, raw_link)
            if not resolved.exists():
                violations.append(
                    LinkViolation(
                        source=md_file.relative_to(repo_root),
                        target=raw_link,
                        resolved=resolved,
                    )
                )
    return violations


def main(argv: Sequence[str]) -> int:
    """脚本主入口。"""

    repo_root = Path(os.environ.get("SPARK_DOC_ROOT", discover_repo_root()))
    files = list(iter_markdown_files(repo_root))
    print(f"[docs-linkcheck] scanning {len(files)} markdown files under {repo_root}")
    violations = validate_links(files, repo_root)

    if violations:
        print("[docs-linkcheck] detected broken links:")
        for issue in violations:
            print(f"  - {issue.source}: '{issue.target}' -> {issue.resolved}")
        print("[docs-linkcheck] total failures:", len(violations))
        return 1

    print("[docs-linkcheck] all links resolved successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
