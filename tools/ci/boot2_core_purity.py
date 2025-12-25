#!/usr/bin/env python3
"""教案级注释：spark-core 纯度（反网络/异步依赖）预热检查脚本。

Why / 架构定位 (R1.1-R1.3)
============================
- 目标：扫描 `crates/spark-core` 的源代码与 Cargo 清单，确保未引入
  `std::net`、`tokio`、`mio`、`socket2`、`time` 等会破坏核心库纯度的依赖或引用。
- 架构定位：脚本服务于 BOOT-2 预热阶段，补齐在 BOOT-1 尚未实施的“核心纯度”守卫，
  帮助提前量化技术债。
- 设计模式：采用“静态文本扫描 + 违规快照”策略，避免引入编译器解析依赖，保证在
  CI 环境下快速执行。

How / 执行逻辑 (R2.1-R2.2)
===========================
1. 解析命令行参数，定位仓库根目录与 JSON 报告输出路径。
2. 构建禁止引用的关键字列表，并同时检查源码 (`.rs`) 与 manifest (`Cargo.toml`)。
3. 对源码文件逐行扫描，记录命中禁用关键字的位置（文件相对路径 + 行号 + 关键字）。
4. 对 `Cargo.toml` 的 `[dependencies]`、`[dev-dependencies]` 等区块执行关键字匹配，
   若发现禁用依赖，同样记录违规。
5. 将所有违规项输出为 Markdown 表格与 JSON 结构，便于后续汇总。
6. 若无违规，退出码 0；若存在违规，退出码 1；若发生异常，例如路径不存在，则退出 2。

What / 契约定义 (R3.1-R3.3)
============================
- 输入参数：
  * `--workspace`：仓库根目录，可选，默认取脚本所在目录的上级。
  * `--json-report`：JSON 报告路径，默认 `ci-artifacts/boot2-core-purity.json`。
- 前置条件：
  * `crates/spark-core` 目录存在，且包含源代码与 `Cargo.toml`。
- 输出与后置条件：
  * 标准输出：违规摘要表格，显示每条违规的关键字、位置、说明。
  * JSON 报告：结构化记录所有违规，供 summary 步骤消费。
  * 退出码语义：0=无违规，1=存在违规，2=脚本异常。

Trade-offs / 设计权衡 (R4.1-R4.3)
=================================
- 采用文本扫描而非 AST 解析，牺牲部分精确度（可能误报注释中的关键字），但实现简单
  且执行迅速；后续可根据需求增强过滤逻辑。
- `time` 关键字仅匹配 `time::` 或 `use time`，避免误报 `std::time`。
- 记录所有违规详情而不是立即退出，方便在预热阶段全面观察技术债。

Risk Awareness / 风险提示 (R4.2)
=================================
- 文本扫描无法识别通过宏间接引入的依赖，需后续更严格的工具补充。
- 若未来允许测试代码使用部分依赖，需要在脚本中为 `tests/` 目录添加白名单。

Readability / 维护指引 (R5.1-R5.2)
===================================
- 将关键常量集中在模块顶部，函数粒度清晰（扫描源码、扫描 manifest、输出报告）。
- 所有函数都包含教案级 docstring，说明其职责与输入输出，便于维护者理解并扩展。
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

# 禁用关键字定义：key 为标识，value 为对应的正则模式。
_BANNED_SOURCE_PATTERNS: Dict[str, re.Pattern[str]] = {
    "std::net": re.compile(r"std::net"),
    "tokio": re.compile(r"\btokio::"),
    "mio": re.compile(r"\bmio::"),
    "socket2": re.compile(r"\bsocket2::"),
    # 捕获根级 time:: 引用，避免误报 std::time::（通过前瞻排除 ::time:: 前有冒号的情况）。
    "time": re.compile(r"(?<!:)\btime::"),
}

_BANNED_MANIFEST_KEYS: Dict[str, re.Pattern[str]] = {
    "tokio": re.compile(r"^tokio\b"),
    "mio": re.compile(r"^mio\b"),
    "socket2": re.compile(r"^socket2\b"),
    "time": re.compile(r"^time\b"),
}


@dataclass
class Violation:
    """教案级注释：表示一次 spark-core 纯度违规记录。

    Why (R1):
        - 用于在源码扫描与报告生成之间传递统一的数据结构，避免散落的 tuple。
        - 该结构在架构上位于“检测”与“汇总”模块之间，承担数据载体角色。
    How (R2):
        - 字段 `file` 存储相对路径，`line` 存储行号，`keyword` 标记违规关键字，
          `context` 则保留原始代码片段，帮助审阅者理解上下文。
    What (R3):
        - 构造时需要四个字段，均为只读属性；不提供额外方法。
    Trade-offs (R4):
        - 使用 dataclass 自动生成比较/打印方法，便于调试；代价是 Python 3.7+ 依赖，
          但对当前仓库不是问题。
    Readability (R5):
        - 字段命名语义清晰，遵循“位置 + 关键字 + 摘要”三段式结构。
    """

    file: str
    line: int
    keyword: str
    context: str


def _read_text_lines(path: Path) -> List[str]:
    """读取文件为行列表，统一使用 UTF-8 并在失败时抛出异常。

    Why (R1):
        - 源码/manifest 扫描都需要一致的文本读取方式，此函数集中处理编码与换行，
          避免在多个调用点重复实现。
    How (R2):
        - 使用 `Path.read_text` 强制 UTF-8 解码，并以 `splitlines()` 生成逐行列表。
    What (R3):
        - 输入：文件路径；输出：字符串列表。
        - 前置条件：文件存在且可读；否则由上层捕获异常。
    Trade-offs (R4):
        - 未显式处理不同平台换行，依赖 `splitlines` 的通用行为；对当前需求足够。
    Readability (R5):
        - 简洁的命名与单一职责，使代码意图清晰。
    """

    return path.read_text(encoding="utf-8").splitlines()


def scan_sources(src_root: Path, workspace: Path) -> List[Violation]:
    """扫描 spark-core 源码，寻找被禁用的关键字引用。

    Why (R1):
        - 该函数直接负责识别破坏纯度的 API 调用，是脚本的核心检测逻辑。
    How (R2):
        - 遍历 `src_root` 下的所有 `.rs` 文件，逐行匹配 `_BANNED_SOURCE_PATTERNS`。
        - 对每个命中项构造 `Violation`，记录相对路径与行号，并保留原始代码片段。
    What (R3):
        - 输入：源码根路径、工作区根路径。
        - 输出：违规列表（可能为空）。
        - 前置条件：路径存在且可读。
        - 后置条件：返回的 `Violation.file` 均为相对路径字符串。
    Trade-offs (R4):
        - 目前不区分注释与代码，可能产生少量误报；后续可通过简单的注释过滤优化。
    """

    violations: List[Violation] = []
    for source_path in sorted(src_root.rglob("*.rs")):
        lines = _read_text_lines(source_path)
        relative = source_path.relative_to(workspace).as_posix()
        for lineno, line in enumerate(lines, start=1):
            for keyword, pattern in _BANNED_SOURCE_PATTERNS.items():
                if pattern.search(line):
                    violations.append(
                        Violation(
                            file=relative,
                            line=lineno,
                            keyword=keyword,
                            context=line.strip(),
                        )
                    )
    return violations


def scan_manifest(manifest_path: Path, workspace: Path) -> List[Violation]:
    """扫描 Cargo.toml，检查禁用依赖项。

    Why (R1):
        - spark-core 的纯度不仅体现在源码引用，也体现在依赖声明。本函数负责捕获
          manifest 中可能引入的不合规第三方依赖。
    How (R2):
        - 逐行解析 `Cargo.toml`，在依赖声明区段内使用 `_BANNED_MANIFEST_KEYS` 进行匹配。
        - 一旦命中，即记录包含关键字与完整行内容的 `Violation`。
    What (R3):
        - 输入：manifest 路径与工作区根路径。
        - 输出：`Violation` 列表。
        - 前置条件：manifest 文件存在且可读。
        - 后置条件：返回的 `file` 字段为 manifest 相对路径。
    Trade-offs (R4):
        - 未引入 TOML 解析库，牺牲少量精确度（忽略 inline table 等复杂语法），换取
          更快执行与零依赖；如果未来需要精确支持，可替换为 `tomli` 解析。
    Readability (R5):
        - 清晰的变量命名（`stripped`、`relative`）以及注释解释过滤策略。
    """

    violations: List[Violation] = []
    lines = _read_text_lines(manifest_path)
    relative = manifest_path.relative_to(workspace).as_posix()
    in_relevant_section = False
    for lineno, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if stripped.startswith("[") and stripped.endswith("]"):
            header = stripped.strip("[]")
            in_relevant_section = False
            if header.startswith("dependencies") or header.startswith("dev-dependencies") or header.startswith("build-dependencies"):
                in_relevant_section = True
            elif ".dependencies" in header:
                in_relevant_section = True
            continue

        if not in_relevant_section:
            continue

        for keyword, pattern in _BANNED_MANIFEST_KEYS.items():
            if pattern.match(stripped):
                violations.append(
                    Violation(
                        file=relative,
                        line=lineno,
                        keyword=keyword,
                        context=stripped,
                    )
                )
    return violations


def emit_reports(violations: Sequence[Violation], json_path: Path) -> None:
    """输出纯度检查摘要与 JSON 报告。

    Why (R1):
        - 将检测阶段的原始数据转化为人类可读的概览与机器可消费的 JSON，是预热模式
          中与后续 summary 注释沟通的关键环节。
    How (R2):
        - 在终端打印 Markdown 风格表格；并以 `status + violations` 形式写入 JSON。
    What (R3):
        - 输入：违规序列与 JSON 路径。
        - 输出：无返回值，副作用为写入文件/打印信息。
        - 前置条件：`json_path` 目录可创建。
        - 后置条件：生成的 JSON 结构含 `id/title/status/violations` 字段。
    Trade-offs (R4):
        - 不在函数内终止流程，即便存在大量违规也继续输出，方便一次性了解全貌。
    Readability (R5):
        - 表头/字段命名与业务含义一致，便于后续团队成员拓展。
    """

    status = "pass" if not violations else "fail"
    print("检查目标: spark-core 禁用网络/异步依赖 (std::net/tokio/mio/socket2/time)")
    print("状态 | 位置 | 关键字 | 代码片段")
    print("-----|------|--------|--------")
    for violation in violations:
        location = f"{violation.file}:{violation.line}"
        print(f"WARN | {location} | {violation.keyword} | {violation.context}")

    payload = {
        "id": "core-purity",
        "title": "spark-core 纯度",
        "status": status,
        "violations": [
            {
                "location": f"{v.file}:{v.line}",
                "keyword": v.keyword,
                "context": v.context,
            }
            for v in violations
        ],
    }

    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """解析命令行参数。

    Why (R1):
        - 允许调用者自定义仓库根目录与 JSON 输出路径，使脚本既可在 CI 运行，也可本地
          试运行。
    How (R2):
        - 通过 argparse 定义两个可选参数，并为帮助信息提供中文描述。
    What (R3):
        - 输入：命令行参数序列。
        - 输出：具备 `workspace` 与 `json_report` 属性的 Namespace。
        - 前置条件：参数可转换为有效路径。
    Readability (R5):
        - 参数命名直接反映其含义，帮助同事快速理解。
    """

    parser = argparse.ArgumentParser(description="spark-core 纯度预热检查")
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="仓库根目录，默认取脚本所在目录的上级",
    )
    parser.add_argument(
        "--json-report",
        type=Path,
        default=Path("ci-artifacts/boot2-core-purity.json"),
        help="输出 JSON 报告路径",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """脚本主入口。

    Why (R1):
        - 串联参数解析、目录校验、扫描与报告输出，形成可直接由 CI 调用的一站式入口。
    How (R2):
        - 依次执行：解析参数 -> 校验目录 -> 扫描源码 -> 扫描 manifest -> 输出报告。
        - 捕获异常并输出带前缀的错误信息，便于 CI 日志检索。
    What (R3):
        - 输入：可选的参数序列。
        - 输出：退出码（0/1/2）。
        - 前置条件：调用者具备读取仓库的权限。
        - 后置条件：若无致命异常，一定生成 JSON 报告。
    Trade-offs (R4):
        - 遇到结构缺失时返回 2 而非 1，明确区分“脚本异常”与“检测到违规”。
    Readability (R5):
        - 清晰的变量命名与日志前缀，降低排错成本。
    """

    try:
        args = parse_args(argv or [])
        workspace = args.workspace
        crate_root = workspace / "crates" / "spark-core"
        if not crate_root.is_dir():
            print(f"[BOOT2-CORE-PURITY][ERROR] 找不到 spark-core 目录：{crate_root}")
            return 2
        source_root = crate_root / "src"
        manifest_path = crate_root / "Cargo.toml"
        if not source_root.is_dir() or not manifest_path.is_file():
            print("[BOOT2-CORE-PURITY][ERROR] spark-core 目录结构不完整。")
            return 2

        violations = []
        violations.extend(scan_sources(source_root, workspace))
        violations.extend(scan_manifest(manifest_path, workspace))
        emit_reports(violations, args.json_report)
        return 0 if not violations else 1
    except Exception as exc:  # pragma: no cover - 防御性兜底
        print(f"[BOOT2-CORE-PURITY][ERROR] 未预期异常：{exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
