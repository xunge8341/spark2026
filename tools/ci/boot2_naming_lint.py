#!/usr/bin/env python3
"""教案级注释：BOOT-2 命名规范预热检查脚本。

Why / 架构定位 (R1.1-R1.3)
============================
- 目标：扫描 `crates/` 目录下的 crate 根目录，校验其相对路径是否符合
  `^crates/spark-[a-z0-9-]+$` 命名规范，并在预热阶段产出“警告不阻断”报告。
- 架构定位：该脚本位于 CI bootstrapping（BOOT-2）阶段，用于补齐 BOOT-1 guardrails
  尚未覆盖的命名一致性质量红线，辅助未来逐步收紧发布质量门槛。
- 设计理念：采用“发现 -> 记录 -> 非阻断退出”流程，以 JSON + 文本双格式输出，
  方便后续 summary 步骤汇总并在 PR 上发布自动注释。

How / 执行逻辑 (R2.1-R2.2)
===========================
1. 解析命令行参数，定位工作区根路径并确认 `crates/` 目录存在。
2. 通过 glob 枚举 `crates/` 下所有含 `Cargo.toml` 的子目录（支持多级子目录），
   这些目录视为 crate 根目录。
3. 将每个 crate 根目录转换为以 POSIX 斜杠分隔的相对路径，利用编译好的正则表达式
   进行匹配。
4. 记录不符合规范的目录及原因，生成结构化 JSON 结果，同时输出人类可读的表格摘要。
5. 若存在违规，则返回退出码 1；否则退出码 0。无论成败，都会写入 JSON 报告，用于
   后续 CI 步骤生成非阻断注释。

What / 契约定义 (R3.1-R3.3)
============================
- 输入参数：
  * `--workspace`：仓库根目录，可选，默认为脚本所在目录的上级。
  * `--json-report`：输出 JSON 路径，默认 `ci-artifacts/boot2-naming.json`。
- 前置条件：
  * 工作区内存在 `crates/` 目录；目录下每个 crate 至少有一个 `Cargo.toml`。
- 输出与后置条件：
  * 标准输出：人类可读的违规摘要表格。
  * JSON 报告：包含所有扫描的 crate 以及违规详情。
  * 退出状态：存在违规时为 1，否则为 0；脚本本身故障（如路径不存在）时返回 2。

Trade-offs / 设计权衡 (R4.1-R4.3)
=================================
- 不直接扫描 `Cargo.toml` 的 `package.name`，因为目录命名与包名可能不同步，但 CI
  目前只要求目录规范；后续可扩展增加包名校验。
- 使用 glob 允许多级子目录，牺牲部分性能换取目录布局灵活性（当前仓库规模下开销
  可忽略）。
- 针对未来迁移阶段，保留所有违规条目的详细记录，方便对照改名计划；脚本不会在
  预热模式下阻断构建。

Risk Awareness / 风险提示 (R4.2)
=================================
- 若未来引入非常规目录（例如同步子仓库）且含 `Cargo.toml`，会被视为违规；可通过
  后续加入忽略列表解决。
- glob 扫描时默认 UTF-8 解码文件路径；若系统路径包含非 UTF-8 字节会抛出异常。

Readability / 维护指引 (R5.1-R5.2)
===================================
- 采用 dataclass + 清晰函数命名，配合结构化 docstring，便于后续维护者快速理解。
- JSON 输出字段包含文档链接式 key，方便 summary 脚本拼装 Markdown。
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

_PATH_PATTERN = re.compile(r"^crates/spark-[a-z0-9-]+$")


@dataclass
class CrateRecord:
    """教案级注释：封装单个 crate 目录检查结果的数据对象。

    Why (R1):
        - 在汇总阶段需要携带 crate 的相对路径、规范匹配状态以及违规原因，此 dataclass
          提供统一载体，避免在多个列表之间维护平行数据。
        - 该结构在整体流程中充当“数据交换格式”，供渲染与 JSON 序列化使用。
    How (R2):
        - 字段 `relative_path` 保存 POSIX 形式的路径字符串。
        - 字段 `is_valid` 表示是否匹配命名规范。
        - 字段 `reason` 对违规情况进行解释，若 `is_valid` 为 True 则置空字符串。
    What (R3):
        - 输入：构造函数接受上述三个字段。
        - 输出：作为不可变数据结构传递，无副作用。
    Trade-offs (R4):
        - 选择 dataclass 简化初始化与 `asdict` 序列化；若未来字段增多，可继续扩展。
    Readability (R5):
        - 字段命名与含义一一对应，避免魔法常量。
    """

    relative_path: str
    is_valid: bool
    reason: str


def discover_crate_roots(crates_root: Path) -> List[Path]:
    """扫描 crates 目录下的 crate 根目录。

    Why (R1):
        - 负责将文件系统层的目录结构映射为后续校验所需的 crate 根目录集合，是整个
          命名检查的第一步。
        - 在 BOOT-2 预热流程中，该函数帮助我们建立观察清单，以便后续迭代逐步整改。
    How (R2):
        - 使用 `Path.glob('**/Cargo.toml')` 查找所有 manifest，再取其父目录视为 crate 根。
        - 通过集合去重，避免同一目录被重复记录（例如工作区包含额外的 target 软链接）。
        - 最终返回排序后的列表，确保输出稳定（有利于 CI diff 和阅读）。
    What (R3):
        - 输入：`crates_root` 必须是存在的目录路径。
        - 输出：按相对路径字典序排序的 `Path` 列表。
        - 前置条件：调用者已确认 `crates_root` 存在且可访问。
        - 后置条件：返回的每个 Path 均指向实际存在的目录。
    Trade-offs (R4):
        - 采用递归 glob，可能会遍历 vendor/子模块，需在后续逻辑中过滤；目前仓库结构
          简洁，该选择足够稳健。
    Readability (R5):
        - 使用显式变量命名与注释，保持流程清晰。
    """

    if not crates_root.is_dir():
        raise FileNotFoundError(f"找不到 crates 目录：{crates_root}")

    crate_dirs = {manifest.parent.resolve() for manifest in crates_root.glob("**/Cargo.toml")}
    # 排序时转换为字符串，确保不同平台路径比较一致。
    return sorted(crate_dirs, key=lambda path: str(path))


def evaluate_paths(crate_dirs: Sequence[Path], workspace: Path) -> List[CrateRecord]:
    """根据命名规范评估 crate 目录列表。

    Why (R1):
        - 将原始目录列表转为包含命名匹配状态的结构化数据，是脚本的核心逻辑。
        - 输出数据既供终端展示，也供 JSON 报告使用，是后续总结的唯一数据源。
    How (R2):
        - 将绝对路径转换为相对工作区的 POSIX 路径字符串。
        - 使用 `_PATH_PATTERN` 判定是否符合命名规范。
        - 对不符合规范的情况，依据具体形态给出解释（例如“含有额外的子目录层级”）。
    What (R3):
        - 输入：crate 根目录序列与工作区根路径。
        - 输出：`CrateRecord` 列表，与输入顺序一致。
        - 前置条件：`workspace` 必须是 crate 目录的祖先路径。
        - 后置条件：输出列表长度与输入一致，且所有 `reason` 字段在 `is_valid=False` 时
          非空。
    Trade-offs (R4):
        - 违规原因目前采用启发式判断：优先识别是否存在额外子目录层级，其余情况给出
          通用提示。更复杂的分类可在后续迭代引入。
    Readability (R5):
        - 提供中间变量和注释，使逻辑自解释。
    """

    records: List[CrateRecord] = []
    for crate_dir in crate_dirs:
        relative_path = crate_dir.relative_to(workspace).as_posix()
        is_valid = bool(_PATH_PATTERN.fullmatch(relative_path))
        reason = ""
        if not is_valid:
            parts = relative_path.split("/")
            if len(parts) > 2:
                reason = "包含额外的目录层级，建议提升至 crates/ 直接子目录。"
            elif not relative_path.startswith("crates/spark-"):
                reason = "目录前缀未以 crates/spark- 开头，需统一命名。"
            else:
                reason = "未匹配命名正则，请检查大小写或字符范围。"
        records.append(CrateRecord(relative_path=relative_path, is_valid=is_valid, reason=reason))
    return records


def emit_reports(records: Sequence[CrateRecord], json_path: Path) -> None:
    """输出终端摘要与 JSON 报告。

    Why (R1):
        - 负责将评估结果转换为人类与机器均可消费的格式，是 BOOT-2 预热流程与后续
          summary 步骤之间的桥梁。
    How (R2):
        - 在终端输出带状态标识的表格，便于开发者快速浏览当前违规情况。
        - 将数据序列化为 JSON，包含元信息（检查标题、状态、违规列表等）。
    What (R3):
        - 输入：`records` 为完整的检查结果；`json_path` 为目标文件路径。
        - 前置条件：`json_path` 所在目录存在或可创建。
        - 后置条件：生成的 JSON 文件包含以下字段：
            {
              "id": "naming",
              "title": "命名 lint",
              "status": "pass" | "fail",
              "violations": [...],
              "total": <int>,
              "invalid": <int>
            }
    Trade-offs (R4):
        - JSON 中保留全部违规详情（含原因），为后续自动化整改提供数据。考虑到数据量
          较小，未额外压缩。
    Readability (R5):
        - 输出格式以 Markdown 风格表头呈现，易于复制到报告中。
    """

    total = len(records)
    invalid_records = [record for record in records if not record.is_valid]

    print("检查目标: crates 目录命名规范 (正则 ^crates/spark-[a-z0-9-]+$)")
    print("状态 | 目录 | 备注")
    print("-----|------|----")
    for record in records:
        status = "PASS" if record.is_valid else "WARN"
        note = record.reason if record.reason else ""
        print(f"{status} | {record.relative_path} | {note}")

    payload = {
        "id": "naming",
        "title": "命名 lint",
        "status": "pass" if not invalid_records else "fail",
        "total": total,
        "invalid": len(invalid_records),
        "violations": [
            {
                "path": record.relative_path,
                "reason": record.reason,
            }
            for record in invalid_records
        ],
    }

    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """解析命令行参数并返回命名空间对象。

    Why (R1):
        - 统一处理 CLI 输入，允许在 CI 与本地开发之间复用脚本。
    How (R2):
        - 使用 argparse 定义 `--workspace` 与 `--json-report` 参数，提供默认值与帮助文本。
    What (R3):
        - 输出：包含 `workspace` (Path) 与 `json_report` (Path) 字段的 Namespace。
        - 前置条件：调用者提供的路径字符串可转换为 Path。
    Readability (R5):
        - 参数描述使用中文说明，方便团队成员理解。
    """

    parser = argparse.ArgumentParser(description="BOOT-2 命名规范预热检查")
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="仓库根目录，默认取脚本所在目录的上级",
    )
    parser.add_argument(
        "--json-report",
        type=Path,
        default=Path("ci-artifacts/boot2-naming.json"),
        help="输出 JSON 报告路径",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """脚本主入口：串联参数解析、扫描与报告输出。

    Why (R1):
        - 为 CI job 提供单一入口点，便于通过 `python3 tools/ci/boot2_naming_lint.py` 调用。
    How (R2):
        - 依次执行：解析参数 -> 扫描目录 -> 评估命名 -> 输出报告。
        - 捕获关键异常并转换为用户友好的错误信息与退出码。
    What (R3):
        - 输入：命令行参数序列。
        - 输出：整数退出码（0=通过，1=存在违规，2=执行异常）。
        - 前置条件：调用者对仓库具有读取权限。
        - 后置条件：无论成功与否，若未发生致命错误，均会生成 JSON 报告。
    Trade-offs (R4):
        - 采用“fail-fast”策略，路径缺失时立即退出，避免产生误导性的空报告。
    Readability (R5):
        - 明确的 try/except 块与日志信息，便于排查问题。
    """

    try:
        args = parse_args(argv or [])
        crates_root = args.workspace / "crates"
        crate_dirs = discover_crate_roots(crates_root)
        records = evaluate_paths(crate_dirs, args.workspace)
        emit_reports(records, args.json_report)
        return 0 if all(record.is_valid for record in records) else 1
    except FileNotFoundError as exc:
        print(f"[BOOT2-NAMING][ERROR] {exc}")
        return 2
    except Exception as exc:  # pragma: no cover - 防御性兜底
        print(f"[BOOT2-NAMING][ERROR] 未预期的异常：{exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
