#!/usr/bin/env python3
"""教案级注释：公共 API 预算检查日志的预热汇总脚本。

Why (R1.1-R1.3)
===============
- 目标：解析 `tools/ci/public_api_diff_budget.sh` 产生的日志，在 BOOT-2 阶段生成非阻断
  报告，帮助团队了解当前公共 API 破坏性变更的状况。
- 架构定位：作为 BOOT-2 预热流程的日志收敛层，与命名、纯度、文档密度的汇总逻辑并列。
- 设计理念：在不修改原有 shell 守护脚本的前提下，通过模式匹配提取关键信息，输出
  JSON + Markdown 摘要，后续由 summary 步骤统一注释到 PR。

How (R2.1-R2.2)
===============
1. 读取公共 API 检查日志，提取每个 crate 的 breaking 数量以及是否出现超预算信息。
2. 根据退出码判定整体状态（通过/失败/错误），并识别是否缺少工具等环境问题。
3. 输出终端摘要以及 JSON 报告供后续步骤消费。

What (R3.1-R3.3)
================
- 输入参数：
  * `--log`：公共 API 检查日志路径。
  * `--status`：原始脚本的退出码。
  * `--json-report`：输出 JSON 路径。
- 前置条件：日志文件存在且可读。
- 后置条件：生成 JSON 包含 `counts`（每个 crate 的 breaking 数量）、`status`、
  `missing_tool` 等字段。

Trade-offs (R4.1-R4.3)
======================
- 通过正则解析文本，简单但依赖日志格式稳定；相比直接调用 shell 函数，更易维护。
- 对未知退出码统一标记为 "error"，避免误导使用者。

Risk Awareness (R4.2)
=====================
- 若未来 shell 脚本调整日志前缀，需要同步更新匹配模式。
- 当工具缺失时，记录 `missing_tool=True`，提醒在后续阶段补齐依赖。

Readability (R5.1-R5.2)
=======================
- 函数粒度清晰，命名语义明确；输出日志携带统一前缀 `[BOOT2-PUBLIC-API]`。
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

_BREAKING_PATTERN = re.compile(r"crate '([^']+)' 检测到 ([0-9]+) 个 breaking 变更 \(预算 ([0-9]+)\)")
_BUDGET_EXCEEDED_PATTERN = re.compile(r"超出预算")
_MISSING_TOOL_PATTERN = re.compile(r"未检测到 cargo-public-api")


@dataclass
class PublicApiSummary:
    """教案级注释：承载公共 API 守护脚本解析结果的数据结构。

    Why (R1):
        - 统一保存每个 crate 的 breaking 数、预算、是否超限以及退出码，供后续生成报告。
    How (R2):
        - dataclass 自动生成初始化/比较函数，使 `parse_log` 的返回值表达清晰。
    What (R3):
        - 字段：`counts`（crate -> breaking 数）、`budget`（crate -> 预算）、`exceeded`
          （原始日志行）、`missing_tool`（bool）、`exit_code`（int）。
    Trade-offs (R4):
        - 为保持灵活性，`exceeded` 保存原始日志文本而非解析后的结构；虽然略显冗长，
          但能完整保留诊断信息。
    Readability (R5):
        - 字段命名语义直观，便于团队成员理解。
    """

    counts: Dict[str, int]
    budget: Dict[str, int]
    exceeded: List[str]
    missing_tool: bool
    exit_code: int

    @property
    def status(self) -> str:
        """根据退出码和解析结果推导总体状态。

        Why (R1):
            - summary 步骤需要统一的 pass/fail/error 状态，本属性提供集中判断。
        How (R2):
            - 退出码 0 视为 pass，1 视为 fail，其余归类为 error（如工具缺失时的 127）。
        What (R3):
            - 无参数，返回字符串状态。
        Trade-offs (R4):
            - 未细分更多退出码，保持实现简单；若未来脚本引入更多语义，可扩展此处。
        Readability (R5):
            - 函数体短小，并带注释说明语义。
        """

        if self.exit_code == 0:
            return "pass"
        if self.exit_code == 1:
            return "fail"
        return "error"


def parse_log(lines: List[str], exit_code: int) -> PublicApiSummary:
    """解析公共 API 检查日志。

    Why (R1):
        - 将 shell 输出转换为结构化数据，是后续生成 JSON 报告的关键步骤。
    How (R2):
        - 使用 `_BREAKING_PATTERN` 匹配每个 crate 的统计行。
        - 使用 `_BUDGET_EXCEEDED_PATTERN` 捕获超预算提示，保留原始文本。
        - 使用 `_MISSING_TOOL_PATTERN` 检测工具缺失。
    What (R3):
        - 输入：日志行列表与退出码；输出：`PublicApiSummary`。
        - 前置条件：日志采用 `[public-api-diff]` 前缀格式。
    Trade-offs (R4):
        - 未针对多 crate 同名行做去重，假设日志每个 crate 仅出现一次；若脚本调整，可
          改为追加列表。
    Readability (R5):
        - 变量命名语义明确（counts/budget/exceeded），便于理解。
    """

    counts: Dict[str, int] = {}
    budget: Dict[str, int] = {}
    exceeded: List[str] = []
    missing_tool = False

    for line in lines:
        line = line.rstrip()
        match = _BREAKING_PATTERN.search(line)
        if match:
            crate, count_str, budget_str = match.groups()
            counts[crate] = int(count_str)
            budget[crate] = int(budget_str)
            continue
        if _BUDGET_EXCEEDED_PATTERN.search(line):
            exceeded.append(line)
        if _MISSING_TOOL_PATTERN.search(line):
            missing_tool = True

    return PublicApiSummary(counts=counts, budget=budget, exceeded=exceeded, missing_tool=missing_tool, exit_code=exit_code)


def emit(summary: PublicApiSummary, json_path: Path) -> None:
    """输出终端摘要与 JSON 报告。

    Why (R1):
        - 将结构化数据转为终端摘要和 JSON 文件，是预热流程与最终 PR 注释之间的桥梁。
    How (R2):
        - 打印状态、工具缺失提示及每个 crate 的统计，并列出原始超预算日志。
        - 将相同数据写入 JSON，以供后续 summary 脚本读取。
    What (R3):
        - 输入：`summary` 与目标 JSON 路径；输出：无（副作用）。
    Trade-offs (R4):
        - 即使状态为 error 亦生成 JSON，确保 summary 能显示环境问题。
    Readability (R5):
        - 输出格式采用 Markdown 风格缩进，易读易复制。
    """

    print("检查目标: 公共 API 预算 (预热模式)")
    print(f"总体状态: {summary.status.upper()}")
    if summary.missing_tool:
        print("检测到 cargo-public-api 未安装。")
    if summary.counts:
        print("各 crate breaking 数：")
        for crate, count in summary.counts.items():
            budget = summary.budget.get(crate)
            budget_info = f"预算 {budget}" if budget is not None else "预算未知"
            print(f"- {crate}: {count} ({budget_info})")
    if summary.exceeded:
        print("日志中包含超预算提示：")
        for line in summary.exceeded:
            print(f"  • {line}")

    payload = {
        "id": "public-api",
        "title": "公共 API 预算",
        "status": summary.status,
        "counts": summary.counts,
        "budget": summary.budget,
        "budget_messages": summary.exceeded,
        "missing_tool": summary.missing_tool,
        "exit_code": summary.exit_code,
    }

    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """解析命令行参数。

    Why (R1):
        - 提供灵活的日志与输出路径配置，支持本地排查。
    How (R2):
        - 使用 argparse 定义强制参数 `--log`、`--status` 与可选 `--json-report`。
    What (R3):
        - 输入：参数序列；输出：Namespace。
    Readability (R5):
        - 参数帮助信息使用中文描述，降低沟通成本。
    """

    parser = argparse.ArgumentParser(description="解析公共 API 预算日志")
    parser.add_argument("--log", type=Path, required=True, help="公共 API 守护脚本输出日志路径")
    parser.add_argument("--status", type=int, required=True, help="公共 API 守护脚本退出码")
    parser.add_argument(
        "--json-report",
        type=Path,
        default=Path("ci-artifacts/boot2-public-api.json"),
        help="输出 JSON 报告路径",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """脚本主入口。

    Why (R1):
        - 串联参数解析、日志读取、解析与输出，使脚本可直接被 CI 调用。
    How (R2):
        - 若日志缺失则输出错误 JSON 并返回 2；否则调用 `parse_log` 与 `emit`。
    What (R3):
        - 输入：可选参数序列；输出：整数退出码。
    Trade-offs (R4):
        - 返回值区分 `2=脚本异常` 与 `0=成功`，其余信息交由 JSON 表达，保证预热流程不
          阻断主 CI。
    Readability (R5):
        - 错误信息统一带 `[BOOT2-PUBLIC-API]` 前缀，便于日志检索。
    """

    args = parse_args(argv or [])
    if not args.log.is_file():
        print(f"[BOOT2-PUBLIC-API][ERROR] 找不到日志文件：{args.log}")
        payload = {
            "id": "public-api",
            "title": "公共 API 预算",
            "status": "error",
            "error": "log-missing",
        }
        args.json_report.parent.mkdir(parents=True, exist_ok=True)
        args.json_report.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return 2

    lines = args.log.read_text(encoding="utf-8").splitlines()
    summary = parse_log(lines, args.status)
    emit(summary, args.json_report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
