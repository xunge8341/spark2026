#!/usr/bin/env python3
"""教案级注释：汇总 BOOT-2 预热检查的 Markdown 报告生成脚本。

Why (R1.1-R1.3)
===============
- 目标：收集命名 lint、spark-core 纯度、文档密度、公共 API 预算四类检查的 JSON 报告，
  生成统一的 Markdown 摘要，供 CI 在 PR 上发布非阻断注释。
- 架构定位：脚本位于 BOOT-2 pipeline 的最末端，与 `github-script` 步骤配合，实现
  “多检查 -> 单评论” 的体验。
- 设计理念：以可扩展的数据驱动方式加载报告，根据 `id` 选择摘要策略，保持未来新增
  检查时的可维护性。

How (R2.1-R2.2)
===============
1. 解析命令行参数，接收多个 `--report id=path` 形式的输入以及 Markdown 输出路径。
2. 逐个读取 JSON 报告，构造统一的数据模型（包含 id、title、status、payload）。
3. 根据检查类型生成摘要文本，并组装 Markdown 表格与提醒文案。
4. 将结果写入指定文件，并同步打印到标准输出以便排查。

What (R3.1-R3.3)
================
- 输入参数：
  * `--report`: 多次指定，格式为 `<id>=<path>`。
  * `--markdown`: 输出 Markdown 文件路径。
- 前置条件：各检查脚本已生成 JSON 报告且可读。
- 后置条件：输出 Markdown 文件包含标题、说明、表格，字段满足 summary 步骤需求。

Trade-offs (R4.1-R4.3)
======================
- 采用表格展示状态，兼顾可读性与紧凑性；若检查增多，表格仍易扩展。
- 摘要内容限制在重点信息（数量、示例），详细数据仍通过工件查看，避免评论过长。

Risk Awareness (R4.2)
=====================
- 如果某个 JSON 缺失或格式不符，脚本会以 `error` 行显示，提醒维护者关注。
- 新增检查时需实现对应的摘要函数，否则将使用通用降级提示。

Readability (R5.1-R5.2)
=======================
- 函数和变量命名语义清晰，docstring 说明输入输出；Markdown 模板使用 f-string 直观呈现。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

_STATUS_EMOJI = {
    "pass": "✅",
    "fail": "⚠️",
    "error": "❌",
}


@dataclass
class Report:
    """教案级注释：封装单个检查报告的结构化数据。

    Why (R1):
        - 在汇总阶段需要统一表示每个检查的 `id`、标题、状态与原始 payload，便于后续
          渲染逻辑消费。
    How (R2):
        - 使用 dataclass 自动生成构造器，保持字段顺序与类型清晰。
    What (R3):
        - 字段：`id`（字符串标识）、`title`（展示名称）、`status`（pass/fail/error）、
          `payload`（原始 JSON 字典）。
    Trade-offs (R4):
        - payload 使用 `Dict[str, object]`，牺牲静态类型精确度换取对不同检查的兼容性。
    Readability (R5):
        - 字段命名语义直观，后续维护者可快速理解用途。
    """

    id: str
    title: str
    status: str
    payload: Dict[str, object]


def load_report(path: Path) -> Dict[str, object]:
    """加载 JSON 报告文件并返回字典对象。

    Why (R1):
        - 提供统一的 IO 抽象，避免在多个调用点重复处理编码与解析逻辑。
    How (R2):
        - 使用 `Path.read_text(encoding='utf-8')` 读取文件，再调用 `json.loads` 解析。
    What (R3):
        - 输入：JSON 文件路径；输出：Python 字典。
        - 前置条件：文件存在且符合 JSON 格式。
    Trade-offs (R4):
        - 未捕获异常，交由调用者处理，以便在 summary 中记录错误信息。
    Readability (R5):
        - 函数名即语义，降低理解成本。
    """

    return json.loads(path.read_text(encoding="utf-8"))


def interpret_naming(payload: Dict[str, object]) -> str:
    """生成命名检查的摘要文本。

    Why (R1):
        - 将命名检查的原始数据转换为评论中易读的一句话概述。
    How (R2):
        - 读取违规数量并提取最多 5 个示例目录；若无违规则输出肯定语句。
    What (R3):
        - 输入：命名检查的 JSON payload。
        - 输出：中文摘要字符串。
    Trade-offs (R4):
        - 示例数量限制为 5，避免评论过长；详细列表需到工件查看。
    Readability (R5):
        - 直接返回完整句子，便于 PR 评论呈现。
    """

    invalid = int(payload.get("invalid", 0))
    if invalid == 0:
        return "所有 crate 目录均符合命名规范。"
    violations = payload.get("violations", [])
    samples = ", ".join(item.get("path", "?") for item in violations[:5]) if isinstance(violations, list) else ""
    sample_text = f" 示例：{samples}" if samples else ""
    return f"发现 {invalid} 个目录未匹配命名正则。{sample_text}"


def interpret_core_purity(payload: Dict[str, object]) -> str:
    """生成 spark-core 纯度检查的摘要。

    Why (R1):
        - 帮助评审者快速了解是否存在禁用依赖引用，并提示示例位置。
    How (R2):
        - 统计违规数量，若存在则列出前三个 `location`，否则返回通过信息。
    What (R3):
        - 输入：纯度检查 payload。
        - 输出：描述性字符串。
    Trade-offs (R4):
        - 示例限制为 3 个；详细日志仍需查看工件。
    Readability (R5):
        - 语言直接、结构清晰。
    """

    violations = payload.get("violations", [])
    count = len(violations) if isinstance(violations, list) else 0
    if count == 0:
        return "未检测到禁用依赖或 API。"
    samples = ", ".join(item.get("location", "?") for item in violations[:3]) if isinstance(violations, list) else ""
    return f"检测到 {count} 处禁用引用（示例位置：{samples}）。"


def interpret_doc_density(payload: Dict[str, object]) -> str:
    """生成文档密度检查摘要。

    Why (R1):
        - 在 PR 评论中概述 doc density 现状，提醒需补文档或更新 TODO。
    How (R2):
        - 判断 `failing_crates` 列表是否为空，拼接阈值与 TODO 提示。
    What (R3):
        - 输入：doc density payload。
        - 输出：描述字符串。
    Trade-offs (R4):
        - 最多列出前 5 个未达标 crate，保持评论精炼。
    Readability (R5):
        - 通过 `parts` 列表构建句子，逻辑清晰。
    """

    failing = payload.get("failing_crates", [])
    todo_outdated = bool(payload.get("todo_outdated", False))
    threshold = payload.get("threshold")
    parts: List[str] = []
    if isinstance(failing, list) and failing:
        parts.append(f"{len(failing)} 个 crate 未达到阈值（{', '.join(failing[:5])}）。")
    else:
        parts.append("所有 crate 的文档密度均在阈值之上。")
    if todo_outdated:
        parts.append("docs/TODO.md 需要同步更新。")
    if threshold:
        parts.append(f"阈值：{threshold}。")
    return " ".join(parts)


def interpret_public_api(payload: Dict[str, object]) -> str:
    """生成公共 API 预算摘要。

    Why (R1):
        - 向评审者快速说明公共 API diff 的当前状态，尤其是是否缺少工具或超预算。
    How (R2):
        - 若缺少工具直接返回提示；否则列出前三个 crate 的 breaking/budget 概况。
    What (R3):
        - 输入：公共 API payload。
        - 输出：概述字符串。
    Trade-offs (R4):
        - 仅展示前三个 crate，保持评论简洁；详细数据在日志中。
    Readability (R5):
        - 返回完整句子，避免歧义。
    """

    missing_tool = bool(payload.get("missing_tool", False))
    counts = payload.get("counts", {})
    if missing_tool:
        return "环境缺少 cargo-public-api，请安装后重试。"
    if isinstance(counts, dict) and counts:
        highlight = []
        for crate, value in list(counts.items())[:3]:
            budget = payload.get("budget", {}).get(crate)
            if budget is not None:
                highlight.append(f"{crate}: {value}/{budget}")
            else:
                highlight.append(f"{crate}: {value}")
        joined = ", ".join(highlight)
        return f"当前 diff 结果：{joined}。"
    return "未检测到公共 API 变更或未启用任何 crate 检查。"


def interpret_generic(payload: Dict[str, object]) -> str:
    """针对未知检查的降级摘要。

    Why (R1):
        - 保障即使新增检查未实现专用逻辑，也能给出提醒信息。
    How (R2):
        - 直接返回固定文本，引导查看日志。
    What (R3):
        - 输入：任意 payload；输出：固定字符串。
    Trade-offs (R4):
        - 未尝试解析 payload，避免误导。
    Readability (R5):
        - 文案简洁明确。
    """

    return "检查已执行，详见日志。"


_INTERPRETERS = {
    "naming": interpret_naming,
    "core-purity": interpret_core_purity,
    "doc-density": interpret_doc_density,
    "public-api": interpret_public_api,
}


def build_row(report: Report) -> str:
    """根据报告数据生成 Markdown 表格行。

    Why (R1):
        - 将结构化数据转换为表格格式，方便在评论中对齐展示。
    How (R2):
        - 根据 `status` 选择 emoji，调用对应解释器生成摘要，再拼接为 Markdown 行。
    What (R3):
        - 输入：`Report` 对象；输出：Markdown 行字符串。
    Trade-offs (R4):
        - 未对摘要内容做额外转义，假设检查输出不包含 Markdown 特殊字符；若出现，可
          在未来增加转义。
    Readability (R5):
        - 函数命名清晰，内部逻辑短小。
    """

    emoji = _STATUS_EMOJI.get(report.status, "❔")
    interpreter = _INTERPRETERS.get(report.id, interpret_generic)
    summary = interpreter(report.payload)
    return f"| {report.title} | {emoji} {report.status} | {summary} |"


def parse_report_arg(arg: str) -> Report:
    """解析 `<id>=<path>` 格式的参数并加载 JSON。

    Why (R1):
        - 支持通过命令行动态指定检查 id 与文件路径，增强脚本复用性。
    How (R2):
        - 拆分字符串获取 id 与路径，调用 `load_report` 获取数据，再构造 `Report`。
    What (R3):
        - 输入：参数字符串；输出：`Report` 对象。
        - 前置条件：字符串包含 '='，路径指向有效 JSON。
    Trade-offs (R4):
        - 遇到异常时抛出，由上层捕获并生成 error 行；保持主流程整洁。
    Readability (R5):
        - 变量命名直观。
    """

    if "=" not in arg:
        raise ValueError(f"--report 参数格式错误：{arg}")
    report_id, path_str = arg.split("=", 1)
    path = Path(path_str)
    data = load_report(path)
    return Report(
        id=report_id,
        title=str(data.get("title", report_id)),
        status=str(data.get("status", "error")),
        payload=data,
    )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """解析命令行参数。

    Why (R1):
        - 接收 report/markdown 配置，支持在 CI 与本地环境复用。
    How (R2):
        - 使用 argparse 定义多次出现的 `--report` 与必需的 `--markdown`。
    What (R3):
        - 输入：命令行参数序列；输出：Namespace。
    Readability (R5):
        - 参数描述中文化，方便团队成员理解。
    """

    parser = argparse.ArgumentParser(description="汇总 BOOT-2 预热检查")
    parser.add_argument(
        "--report",
        action="append",
        required=True,
        help="检查报告，格式 <id>=<path>",
    )
    parser.add_argument(
        "--markdown",
        type=Path,
        required=True,
        help="输出 Markdown 文件路径",
    )
    return parser.parse_args(argv)


def render_markdown(reports: List[Report]) -> str:
    """根据报告列表渲染 Markdown 文本。

    Why (R1):
        - 将多条检查信息整合为统一 Markdown 文档，是最终评论的核心内容。
    How (R2):
        - 先生成表头，再迭代每个 `Report` 调用 `build_row` 拼接表格行。
    What (R3):
        - 输入：`Report` 列表；输出：Markdown 字符串。
    Trade-offs (R4):
        - 当前输出固定提示语；若未来需要国际化，可在此调整模板。
    Readability (R5):
        - 模板字符串整洁，易于修改。
    """

    rows = "\n".join(build_row(report) for report in reports)
    header = (
        "### BOOT-2 预热报告（非阻断）\n"
        "> ⚠️ 该阶段仅收集警告信息，不会阻断 CI，如需详情请查看工作流工件。\n\n"
        "| 检查 | 状态 | 摘要 |\n"
        "|------|------|------|\n"
    )
    return f"{header}{rows}\n"


def main(argv: Sequence[str] | None = None) -> int:
    """脚本主入口：解析参数、加载报告并写入 Markdown。

    Why (R1):
        - 串联参数解析、报告加载与 Markdown 渲染，提供单一入口供 CI 调用。
    How (R2):
        - 遍历 `--report` 参数构建 `Report` 列表，捕获异常并降级为 error 行；随后写入
          文件并在日志中打印。
    What (R3):
        - 输入：可选参数序列；输出：整数退出码（恒为 0）。
    Trade-offs (R4):
        - 即便部分报告失败也继续渲染，确保评论仍包含提示信息。
    Readability (R5):
        - 控制流线性清晰，易于维护。
    """

    args = parse_args(argv or [])
    reports: List[Report] = []
    for item in args.report:
        try:
            report = parse_report_arg(item)
        except Exception as exc:  # pragma: no cover - 兜底保护
            reports.append(
                Report(
                    id=item,
                    title=item,
                    status="error",
                    payload={"error": str(exc)},
                )
            )
            continue
        reports.append(report)

    markdown = render_markdown(reports)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.write_text(markdown, encoding="utf-8")
    print(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
