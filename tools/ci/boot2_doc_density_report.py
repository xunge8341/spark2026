#!/usr/bin/env python3
"""教案级注释：将文档密度脚本输出转换为 BOOT-2 预热报告。

Why (R1.1-R1.3)
===============
- 目标：解析 `tools/check_doc_density.py` 生成的日志，将结果转换为结构化 JSON，
  供 CI summary 步骤生成非阻断注释，同时在终端打印重点摘要。
- 架构定位：位于 BOOT-2 pipeline 的“收敛层”，连接已有的 doc density 工具与新的
  预热反馈机制。
- 设计思路：遵循“日志解析 -> 统计 -> 输出”三段式流程，避免修改既有 doc density
  实现，降低耦合风险。

How (R2.1-R2.2)
===============
1. 读取 doc density 日志文件，提取阈值、每个 crate 的状态（PASS/FAIL），以及 TODO
   汇总是否需要更新。
2. 根据传入的退出码判断整体状态，结合解析结果生成总结消息。
3. 将解析到的数据写入 JSON 文件，同时在标准输出打印 Markdown 摘要。

What (R3.1-R3.3)
================
- 输入参数：
  * `--log`：doc density 脚本输出的日志路径。
  * `--status`：原始退出码，用于区分成功/失败/异常。
  * `--json-report`：生成的 JSON 文件路径。
- 前置条件：日志文件存在且可读，内容由 doc density 脚本生成。
- 后置条件：输出 JSON 包含字段 `threshold`、`failing_crates`、`todo_outdated`、`status`。

Trade-offs (R4.1-R4.3)
======================
- 采用正则/字符串解析而非结构化格式，牺牲部分健壮性，但避免改动已有脚本。
- 对 TODO 警告采取布尔标记方式，后续可在 summary 中以文字描述呈现。

Risk Awareness (R4.2)
=====================
- 若未来 doc density 输出格式调整，需要同步更新解析逻辑，否则可能漏报。
- 当日志缺失时视为异常，返回状态 "error" 并提示。

Readability (R5.1-R5.2)
=======================
- 使用带解释的函数 docstring，变量命名清晰，便于维护者快速理解流程。
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

_THRESHOLD_PATTERN = re.compile(r"文档密度阈值:\s*([0-9.]+%)")
_STATUS_PATTERN = re.compile(r"^(?P<crate>[a-z0-9_-]+)\s*\|.*\|\s*(?P<status>PASS|FAIL)\s*$")


@dataclass
class DocDensitySummary:
    """教案级注释：承载文档密度日志解析结果的数据结构。

    Why (R1):
        - 将 `parse_log` 得到的零散信息（阈值、失败列表、TODO 状态、退出码）组合成单一
          对象，方便后续的 `emit` 与 JSON 序列化逻辑使用。
    How (R2):
        - 使用 dataclass 自动生成初始化、比较与打印函数，减少样板代码。
    What (R3):
        - 字段：`threshold` (str|None)、`failing_crates` (List[str])、`todo_outdated` (bool)、
          `exit_code` (int)。
        - 前置条件：调用者确保字段语义正确；本结构不执行额外校验。
    Trade-offs (R4):
        - 选择在 dataclass 内提供 `status` 计算属性，以保持逻辑集中；代价是对外部消费者
          暴露了一层推导逻辑，需在 docstring 中说明。
    Readability (R5):
        - 字段命名与业务含义一致，降低理解成本。
    """

    threshold: str | None
    failing_crates: List[str]
    todo_outdated: bool
    exit_code: int

    @property
    def status(self) -> str:
        """根据解析结果推导总体状态。

        Why (R1):
            - 总结步骤需要统一的状态值（pass/fail/error），通过属性集中封装。
        How (R2):
            - 非 0/1 退出码视为 "error"；否则根据失败列表与 TODO 标记判定是否 fail。
        What (R3):
            - 无输入参数，返回字符串状态。
        Trade-offs (R4):
            - 将退出码 1 与失败视为同义，满足当前 doc density 行为；若未来脚本使用其他
              非零码，需要扩展判定逻辑。
        Readability (R5):
            - 命名为 `status` 且带注释，易于理解。
        """

        if self.exit_code not in (0, 1):
            return "error"
        return "pass" if not self.failing_crates and not self.todo_outdated and self.exit_code == 0 else "fail"


def parse_log(lines: List[str], exit_code: int) -> DocDensitySummary:
    """解析 doc density 日志。

    Why (R1):
        - 将原始文本转换为结构化信息，是生成 JSON 报告的核心步骤。
    How (R2):
        - 使用 `_THRESHOLD_PATTERN` 提取阈值。
        - 针对包含 PASS/FAIL 的表格行应用 `_STATUS_PATTERN`，记录失败 crate。
        - 检测是否存在 TODO 更新提示。
    What (R3):
        - 输入：日志行与退出码。
        - 输出：`DocDensitySummary` 对象。
        - 前置条件：日志格式与示例一致。
    Trade-offs (R4):
        - 当前仅识别 PASS/FAIL 状态，若未来增加 WARN 需扩展正则。
    Readability (R5):
        - 函数命名/变量命名直观，便于阅读。
    """

    threshold = None
    failing_crates: List[str] = []
    todo_outdated = False

    for line in lines:
        line = line.rstrip()
        threshold_match = _THRESHOLD_PATTERN.search(line)
        if threshold_match:
            threshold = threshold_match.group(1)
            continue

        status_match = _STATUS_PATTERN.match(line)
        if status_match and status_match.group("status") == "FAIL":
            failing_crates.append(status_match.group("crate"))
            continue

        if "TODO 汇总文件需要更新" in line:
            todo_outdated = True

    return DocDensitySummary(
        threshold=threshold,
        failing_crates=sorted(set(failing_crates)),
        todo_outdated=todo_outdated,
        exit_code=exit_code,
    )


def emit(summary: DocDensitySummary, json_path: Path) -> None:
    """输出终端摘要与 JSON 报告。

    Why (R1):
        - 将 `DocDensitySummary` 转化为终端可读信息与 JSON 文件，供 CI 工件与评论使用。
    How (R2):
        - 打印状态、阈值、失败列表与 TODO 提示；随后写入结构化 JSON。
    What (R3):
        - 输入：解析后的 summary 与 JSON 路径。
        - 输出：无返回值，副作用为打印/写文件。
        - 前置条件：目标目录可创建。
    Trade-offs (R4):
        - 不在函数内根据状态调整退出码，保持输出与控制流解耦。
    Readability (R5):
        - 表达清晰的日志前缀，便于阅读。
    """

    status = summary.status
    print("检查目标: 文档密度 (预热模式)")
    print(f"总体状态: {status.upper()}")
    if summary.threshold:
        print(f"阈值: {summary.threshold}")
    if summary.failing_crates:
        print("未达标 crate:")
        for crate in summary.failing_crates:
            print(f"- {crate}")
    if summary.todo_outdated:
        print("检测到 docs/TODO.md 需要更新。")

    payload = {
        "id": "doc-density",
        "title": "文档密度",
        "status": status,
        "threshold": summary.threshold,
        "failing_crates": summary.failing_crates,
        "todo_outdated": summary.todo_outdated,
        "exit_code": summary.exit_code,
    }

    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """解析命令行参数。

    Why (R1):
        - 支持 CI/本地自定义日志路径与输出路径，提高脚本复用性。
    How (R2):
        - 使用 argparse 定义三个参数，并提供中文帮助文本。
    What (R3):
        - 输入：参数序列；输出：Namespace。
    Readability (R5):
        - 参数名称直接反映功能，降低理解成本。
    """

    parser = argparse.ArgumentParser(description="解析文档密度日志")
    parser.add_argument("--log", type=Path, required=True, help="doc density 输出日志路径")
    parser.add_argument("--status", type=int, required=True, help="doc density 原始退出码")
    parser.add_argument(
        "--json-report",
        type=Path,
        default=Path("ci-artifacts/boot2-doc-density.json"),
        help="输出 JSON 报告路径",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """脚本主入口：读取日志、解析并生成报告。

    Why (R1):
        - 串联参数解析、日志读取、结果解析与输出，是 CI 调用的唯一入口。
    How (R2):
        - 若日志缺失则输出 error JSON；否则解析后交由 `emit` 输出。
    What (R3):
        - 输入：可选参数序列；输出：整数退出码。
    Trade-offs (R4):
        - 无论 doc density 原始状态如何，本脚本始终返回 0/2，保持预热模式不阻断；
          真正的 pass/fail 信息通过 JSON/日志呈现。
    Readability (R5):
        - 错误信息附带统一前缀 `[BOOT2-DOC-DENSITY]`，便于检索。
    """

    args = parse_args(argv or [])
    if not args.log.is_file():
        print(f"[BOOT2-DOC-DENSITY][ERROR] 找不到日志文件：{args.log}")
        payload = {
            "id": "doc-density",
            "title": "文档密度",
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
