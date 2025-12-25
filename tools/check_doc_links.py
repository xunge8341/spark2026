"""教案级文档链接校验脚本。

该模块聚焦于验证 `crates/*/README.md` 以及 `docs/` 目录下 Markdown 文档中的内部链接，
以便在 CI 阶段及时暴露断链问题。
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence, Tuple
from urllib.parse import unquote

# 统一的 Markdown 行内链接与引用式链接匹配器。
INLINE_LINK_PATTERN = re.compile(r"!??\[[^\]]+\]\(([^)]+)\)")
REFERENCE_LINK_PATTERN = re.compile(r"^\s*\[[^\]]+\]:\s*(\S+)")


@dataclass
class DocLinkIssue:
    """内部链接校验失败结果的数据模型。

    教案级注释约定：

    ### 意图（Why）
    - 描述一条内部链接的校验失败详情，以便调用者能够在汇总阶段给出精确的错误提示。
    - 该结构体承担“问题记录”职责，是脚本对外暴露的最小诊断单元。
    - 数据结构设计遵循不可变（dataclass frozen 不需要，此处保持简洁）模型模式，便于日志打印与测试。

    ### 逻辑说明（How）
    - `path`：当前被检查的 Markdown 文件路径。
    - `line_no`：触发问题的行号，基于 1 起始的自然计数，方便定位。
    - `link_target`：原始链接目标字符串，用于保留用户输入。
    - `reason`：给出失败原因（例如“文件不存在”或“指向目录”）。

    ### 契约（What）
    - 入参通过 dataclass 构造时，需要提供全部字段。
    - 无额外方法，对外只提供结构化数据，不承担业务逻辑。
    - 构造后不修改字段，调用者应视为只读。

    ### 设计考量（Trade-offs & Gotchas）
    - 选择 dataclass 便于自动生成 `__repr__`，改善调试体验。
    - 未冻结（`frozen=False`），因为无需额外约束，且保持默认行为兼容简单的单元测试。
    - 若未来需要在 issue 上扩展更多元数据，可直接新增字段并保持向后兼容。

    ### 可维护性（Readability）
    - 字段命名与含义保持直观，使得日志打印无需额外解释。
    """

    path: Path
    line_no: int
    link_target: str
    reason: str


def iter_markdown_files(repo_root: Path) -> Iterator[Path]:
    """枚举需要校验的 Markdown 文件集合。

    教案级注释要求对应说明：

    ### 意图（Why）
    - 限定本脚本关注的文档范围：`crates/*/README.md` 与 `docs/` 下所有 Markdown。
    - 将路径枚举逻辑集中到单处，防止分散实现导致范围不一致。

    ### 逻辑说明（How）
    1. 基于 `Path.glob` 遍历 `crates/*/README.md`。
    2. 基于 `Path.rglob` 遍历 `docs/` 目录内的 `*.md` 文件。
    3. 通过 `yield from` 顺序输出，保持懒加载避免一次性加载全部路径。

    ### 契约（What）
    - **参数**：`repo_root`（`Path`）——仓库根目录，要求存在且具备 `crates`、`docs` 子目录。
    - **返回值**：迭代器，惰性产出 `Path` 对象，每个对象指向一个需要校验的 Markdown 文件。
    - **前置条件**：调用者确保仓库结构符合约定；若目录缺失，`glob` 将返回空序列并不会抛错。
    - **后置条件**：遍历结束后不会留下文件句柄等资源泄露问题。

    ### 设计考量（Trade-offs & Gotchas）
    - 直接使用 `glob/rglob`，避免手写递归，提高可读性。
    - 生成器设计确保大规模文档场景下内存占用可控。
    - 若后续需要扩展额外目录，可在函数内集中调整，降低变更面。

    ### 可维护性（Readability）
    - 函数命名 `iter_markdown_files` 明确语义：返回一个迭代器而非完整列表。
    """

    crates_readmes = repo_root.glob("crates/*/README.md")
    docs_markdowns = repo_root.joinpath("docs").rglob("*.md")
    yield from crates_readmes
    yield from docs_markdowns


def extract_candidate_links(markdown_path: Path) -> Iterable[Tuple[int, str]]:
    """从 Markdown 文件中提取待校验的链接候选列表。

    ### 意图（Why）
    - 集中处理 Markdown 链接解析，确保校验逻辑与解析逻辑分离。
    - 该函数提供原始链接字符串及行号，为后续的路径验证提供上下文。

    ### 逻辑说明（How）
    1. 逐行读取文件，保证我们能提供准确的行号信息。
    2. 使用两类正则表达式匹配：行内链接与引用式链接定义。
    3. 每命中一次，产出 `(line_no, target)` 元组，目标字符串经过 `strip()` 去除空白。

    ### 契约（What）
    - **参数**：`markdown_path`（`Path`）——需要解析的 Markdown 文件路径，应指向 UTF-8 编码文件。
    - **返回值**：可迭代对象，内含若干 `(行号, 链接目标)` 元组。
    - **前置条件**：文件可读，且不会无限大（默认情况下整文件读取到内存即可）。
    - **后置条件**：不会修改原文件内容，亦不会保持文件句柄。

    ### 设计考量（Trade-offs & Gotchas）
    - 选择逐行读取以便早期流式处理，避免一次性正则匹配整文件导致调试困难。
    - 正则表达式覆盖 Markdown 常见写法，但极端情况（如嵌套括号）未完全支持；在约定式文档中足够可靠。
    - 若未来需要支持 HTML `<a>`，可在此函数添加额外匹配逻辑。

    ### 可维护性（Readability）
    - 返回结构简单，易于在测试或其他模块中重用。
    - 对象直接暴露原始目标字符串，避免过早解析导致信息丢失。
    """

    content = markdown_path.read_text(encoding="utf-8")
    for line_no, line in enumerate(content.splitlines(), start=1):
        for match in INLINE_LINK_PATTERN.finditer(line):
            yield line_no, match.group(1).strip()
        ref_match = REFERENCE_LINK_PATTERN.match(line)
        if ref_match:
            yield line_no, ref_match.group(1).strip()


def is_external_link(target: str) -> bool:
    """判断链接目标是否为外部链接或无需校验的场景。

    ### 意图（Why）
    - 避免脚本对 HTTP、邮件等外部链接重复劳动（这些由现有 lychee 负责）。
    - 让内部链接校验聚焦在相对路径与仓库内的文件引用。

    ### 逻辑说明（How）
    - 统一转为小写，匹配常见协议前缀（`http://`、`https://`、`mailto:` 等）。
    - 纯锚点（`#...`）表示文内跳转，不做进一步验证。

    ### 契约（What）
    - **参数**：`target`（`str`）——原始链接目标。
    - **返回值**：布尔值；`True` 表示外部链接，无需后续文件存在性校验。
    - **前置条件**：调用者确保传入的字符串已去除多余空白。
    - **后置条件**：函数不改变输入字符串，也不抛出异常。

    ### 设计考量（Trade-offs & Gotchas）
    - 选择字符串前缀匹配而非正则，提高性能与可读性。
    - 若未来出现新协议，可在列表中追加即可。

    ### 可维护性（Readability）
    - 函数体短小，便于单元测试与代码评审。
    """

    lowered = target.lower()
    return (
        lowered.startswith("http://")
        or lowered.startswith("https://")
        or lowered.startswith("mailto:")
        or lowered.startswith("tel:")
        or lowered.startswith("data:")
        or lowered.startswith("javascript:")
        or lowered.startswith("#")
    )


def normalize_target_path(base_path: Path, target: str) -> Path | None:
    """将 Markdown 链接目标解析为仓库内的绝对路径。

    ### 意图（Why）
    - 为后续的存在性校验提供统一路径表示。
    - 处理 URL 编码、锚点、查询串等细节，降低重复逻辑。

    ### 逻辑说明（How）
    1. 使用 `urllib.parse.unquote` 解码 `%20` 等转义。
    2. 去除片段标识（`#...`）与查询参数（`?...`），仅保留真实文件路径。
    3. 根据是否以 `/` 开头判断是仓库根目录路径还是相对路径，并返回规范化后的 `Path`。

    ### 契约（What）
    - **参数**：
      - `base_path`：当前 Markdown 文件路径，需为绝对路径，以便计算相对链接。
      - `target`：原始链接目标字符串。
    - **返回值**：若能解析出有效文件路径，返回 `Path`；若目标为空（仅锚点等），返回 `None`。
    - **前置条件**：调用方需确保 `target` 经过 `strip()`。
    - **后置条件**：不会触发实际文件访问，仅进行字符串解析。

    ### 设计考量（Trade-offs & Gotchas）
    - 对空路径（例如 `[](#anchor)`）返回 `None`，让调用者跳过后续校验。
    - 对目录路径保留末尾 `/`，后续逻辑会检查并提示“指向目录”。
    - 没有强制解析相对路径中的 `..`，交由 `Path.resolve`/`Path.parent.joinpath` 自然处理。

    ### 可维护性（Readability）
    - 把解析细节放在独立函数中，方便单元测试和复用。
    """

    decoded = unquote(target)
    # 去除锚点和查询串。
    path_part = decoded.split("#", 1)[0].split("?", 1)[0].strip()
    if not path_part:
        return None

    if path_part.startswith("/"):
        return Path(path_part.lstrip("/"))

    return base_path.parent.joinpath(path_part)


def validate_internal_links(repo_root: Path, markdown_path: Path) -> List[DocLinkIssue]:
    """校验单个 Markdown 文件中的内部链接。

    ### 意图（Why）
    - 实际执行存在性检测，及时定位断链。
    - 汇总所有问题，供上层统一输出并决定退出码。

    ### 逻辑说明（How）
    1. 调用 `extract_candidate_links` 获取所有候选链接。
    2. 过滤掉外部链接；对内部链接使用 `normalize_target_path` 解析。
    3. 将解析结果转换为相对于仓库根目录的 `Path`，并验证：
       - 路径是否位于仓库根目录内。
       - 目标是否存在（允许指向文件或目录）。
    4. 对每个失败条件生成 `DocLinkIssue` 记录。

    ### 契约（What）
    - **参数**：
      - `repo_root`：仓库根目录 `Path`，用于限制越界访问并检查文件存在性。
      - `markdown_path`：当前被校验的 Markdown 文件路径。
    - **返回值**：`DocLinkIssue` 列表，可能为空。
    - **前置条件**：路径均为绝对路径，且文件可读。
    - **后置条件**：不修改任何文件，纯读取操作。

    ### 设计考量（Trade-offs & Gotchas）
    - 使用 `Path.resolve()` 防止链接逃逸（例如通过 `../../..` 指向仓库外部）。
    - 允许目录链接，以兼容 GitHub 在展示目录内容时的默认行为。
    - 若未来需要对目录做额外校验，可在存在性检查之后补充逻辑。

    ### 可维护性（Readability）
    - 返回值集中，便于单元测试和日志打印。
    - 错误信息包含行号、原因，方便定位。
    """

    issues: List[DocLinkIssue] = []
    for line_no, raw_target in extract_candidate_links(markdown_path):
        if is_external_link(raw_target):
            continue
        normalized = normalize_target_path(markdown_path, raw_target)
        if normalized is None:
            continue

        try:
            resolved = normalized.resolve(strict=False)
        except RuntimeError:
            issues.append(
                DocLinkIssue(
                    path=markdown_path,
                    line_no=line_no,
                    link_target=raw_target,
                    reason="无法解析路径"
                )
            )
            continue

        try:
            repo_root_resolved = repo_root.resolve(strict=True)
        except FileNotFoundError:
            repo_root_resolved = repo_root.resolve(strict=False)

        if repo_root_resolved not in resolved.parents and resolved != repo_root_resolved:
            issues.append(
                DocLinkIssue(
                    path=markdown_path,
                    line_no=line_no,
                    link_target=raw_target,
                    reason="链接越界：目标不在仓库目录内"
                )
            )
            continue

        if not resolved.exists():
            issues.append(
                DocLinkIssue(
                    path=markdown_path,
                    line_no=line_no,
                    link_target=raw_target,
                    reason="目标路径不存在"
                )
            )
            continue

    return issues


def run_link_checks(repo_root: Path) -> Sequence[DocLinkIssue]:
    """执行全仓库内部链接校验并返回失败列表。

    ### 意图（Why）
    - 作为脚本入口的核心逻辑，整合文件枚举与单文件校验。
    - 便于未来在测试中直接调用，进行集成级别验证。

    ### 逻辑说明（How）
    1. 调用 `iter_markdown_files` 遍历目标 Markdown 文件。
    2. 对每个文件调用 `validate_internal_links`，并将问题追加到总列表。
    3. 最终返回累积的 `DocLinkIssue` 序列。

    ### 契约（What）
    - **参数**：`repo_root`（`Path`）——仓库根目录。
    - **返回值**：包含所有问题的列表；若无问题则返回空列表。
    - **前置条件**：仓库路径存在。
    - **后置条件**：仅读取文件，不修改任何状态。

    ### 设计考量（Trade-offs & Gotchas）
    - 使用列表而非生成器，便于主函数判断是否存在错误并输出数量。
    - 若未来需要性能优化，可在内部并行化，但当前规模下串行更易调试。

    ### 可维护性（Readability）
    - 函数名与职责一致，减少理解负担。
    """

    issues: List[DocLinkIssue] = []
    for markdown_path in iter_markdown_files(repo_root):
        issues.extend(validate_internal_links(repo_root, markdown_path))
    return issues


def build_argument_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。

    ### 意图（Why）
    - 为脚本提供可选的 `--root` 参数，便于在测试或子模块中复用。

    ### 逻辑说明（How）
    - 基于标准库 `argparse` 构建解析器，仅定义单个可选参数。

    ### 契约（What）
    - 返回值为配置好的 `ArgumentParser` 实例。
    - 参数 `--root` 默认为脚本当前工作目录。

    ### 设计考量（Trade-offs & Gotchas）
    - 采用简单接口即可满足 CI 需求，避免引入额外依赖。

    ### 可维护性（Readability）
    - 若需扩展参数，可在此集中修改。
    """

    parser = argparse.ArgumentParser(description="检查内部文档链接是否失效")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="仓库根目录，默认使用当前工作目录",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """脚本入口：解析参数、执行校验并控制退出码。

    ### 意图（Why）
    - 作为脚本的统一入口，兼容命令行与单元测试直接调用。
    - 根据校验结果决定是否以非零状态退出，保障 CI 能够拦截问题。

    ### 逻辑说明（How）
    1. 解析命令行参数，获取仓库根目录。
    2. 调用 `run_link_checks` 收集所有问题。
    3. 若存在问题，逐条输出详细信息并返回状态码 `1`；否则打印成功提示并返回 `0`。

    ### 契约（What）
    - **参数**：`argv`（可选序列）——便于测试注入自定义参数。
    - **返回值**：整数退出码；`0` 表示全部通过，非零表示存在断链。
    - **前置条件**：执行环境可读取目标文档。
    - **后置条件**：标准输出/错误打印检查结果，不修改文件系统。

    ### 设计考量（Trade-offs & Gotchas）
    - 采用显式的返回码，而非直接调用 `sys.exit`，便于编写单元测试。
    - 输出信息包含相对路径和行号，帮助开发者迅速定位问题。

    ### 可维护性（Readability）
    - 函数逻辑线性清晰，易于理解。
    """

    parser = build_argument_parser()
    args = parser.parse_args(argv)
    repo_root = args.root.resolve()
    issues = run_link_checks(repo_root)

    if issues:
        for issue in issues:
            relative_path = issue.path.resolve().relative_to(repo_root)
            print(
                f"[link-check] {relative_path}:{issue.line_no}: "
                f"'{issue.link_target}' -> {issue.reason}"
            )
        print(f"共发现 {len(issues)} 个失效内部链接。")
        return 1

    print("内部链接校验通过，未发现失效路径。")
    return 0


if __name__ == "__main__":  # pragma: no cover - 入口调用在 CI 中执行，无需覆盖率。
    sys.exit(main())
