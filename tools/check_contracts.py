#!/usr/bin/env python3
"""CI 守卫：核心契约重定义检测器。

Why (意图说明):
- T-004A 要求在 CI 层面阻断 `spark-core` 之外重新声明核心契约类型，
  否则外部 crate 可通过 `pub struct Budget { .. }` 等方式伪造契约，
  破坏框架关于取消、预算、可观测性等 API 的单一事实来源。
- 本脚本位于 `tools/` 根目录，与其它守门脚本共享执行入口，
  通过白名单 + 文本搜索的轻量方案，让 CI 在几十毫秒内完成检测，
  不必引入 AST 解析器或编译步骤，确保流水线负担最小。

How (解析逻辑):
1. 读取 `contracts/core_contract_whitelist.toml` 中的 `names` 列表，
   这是 T-001A 预先整理的核心契约类型白名单。
2. 针对白名单中的每个类型名，调用 `rg`（ripgrep）执行正则搜索，
   匹配形如 `pub struct|enum|trait|type <Name>` 的声明。
3. 将所有命中位置转换为相对路径并校验是否落在 `crates/spark-core/` 下，
   一旦发现非核心路径的命中，就判定为违例并输出具体文件/行号。
4. 若某个白名单类型在仓库中完全找不到定义，则提示白名单可能过期，
   阻止 CI 静默通过，迫使维护者同步更新清单。

What (契约定义):
- 输入：无命令行参数；脚本默认从自身位置反推仓库根目录。
- 前置条件：
  * Python 3.11+（用于 `tomllib`），且环境中可执行 `rg`。
  * 白名单文件存在且至少包含一个类型名。
- 输出/后置条件：
  * 所有命中都位于 `crates/spark-core/` → 退出码 0。
  * 存在违规命中或白名单缺少对应定义 → 打印错误并返回非 0。

Trade-offs & Considerations (设计权衡):
- 采用 `rg` 而非手写解析器，是出于性能与维护成本平衡；
  正则匹配 `pub` 声明即可覆盖常见定义形式，若未来语法更复杂，
  可再引入 `syn` 等 AST 工具。
- 白名单使检测保持“显式”策略：新增契约类型时必须修改 SOT 文件，
  避免脚本误报或错漏。
- 目前仅允许 `crates/spark-core`，如需放行测试目录或特定 crate，
  可在 `ALLOWED_PREFIXES` 中追加条目，同时在注释里标明原因。

Maintenance tips (维护提示):
- 若 `rg` 不可用，脚本会给出明确指引；CI 镜像内已预装 ripgrep。
- 白名单更新后建议运行 `python3 tools/check_contracts.py` 自检，
  确认输出“全部契约定义位于 spark-core”。
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

import tomllib

# 允许存在契约定义的目录前缀（相对于仓库根目录）。
ALLOWED_PREFIXES: Sequence[Path] = (Path("crates") / "spark-core",)
WHITELIST_PATH = Path("contracts/core_contract_whitelist.toml")
RG_PATTERN_TEMPLATE = r"^\s*pub\s+(struct|enum|trait|type)\s+{name}\b"


@dataclass
class SymbolMatch:
    """记录 `rg` 返回的契约定义命中信息。

    Why:
        - 将搜索结果结构化，方便在检测阶段判断是否触发违规，
          并在日志中打印具体文件与行号，帮助开发者快速定位。
        - 数据类形式让代码与日志格式化保持一致，降低维护成本。
    How:
        - `path`：命中文件的绝对路径；
        - `line`：命中的行号（基于 ripgrep 输出解析）；
        - `snippet`：命中所在行的原文，辅助调试；
        - `item_kind`：声明的形态（struct/enum/trait/type），用于区分合法的不同语义。
    What:
        - 所有字段均为只读属性，不在脚本中再次修改。
    Contract:
        - 前置条件：`path` 指向仓库内的源码文件，`line >= 1`。
        - 后置条件：数据对象仅作为只读信息传递给诊断逻辑。
    Considerations:
        - 若未来需要携带列号，可扩展字段并在 `parse_rg_output` 中补充解析。
        - `item_kind` 帮助我们忽略“同名不同形态”的合法类型，例如宏生成的 `type` 别名。
    """

    symbol: str
    path: Path
    line: int
    snippet: str
    item_kind: str


class ContractCheckError(RuntimeError):
    """在契约守卫过程中出现的错误。

    Why:
        - 将可预期的脚本错误（如白名单缺失）与异常故障（如 `rg` 崩溃）
          区分开来，调用方可依据异常类型决定退出码与提示文案。
    How:
        - 继承 `RuntimeError` 以保持语义简单，捕获后转换为友好的输出。
    What:
        - 携带人类可读的错误信息，由调用者负责打印。
    Contract:
        - 不额外封装上下文，保持抛出栈清晰，便于调试。
    """


def ensure_rg_available() -> None:
    """校验 ripgrep 是否可用。

    Why:
        - `rg` 是核心依赖，若缺失必须在流程早期中断，
          避免进入检测阶段后才失败。
    How:
        - 通过 `shutil.which` 判断 `rg` 是否存在于 `PATH` 中。
    What:
        - 无返回值；若缺失则抛出 `ContractCheckError`。
    Contract:
        - 前置：运行环境应已配置 `PATH`。
        - 后置：保证后续调用 `subprocess.run(["rg", ...])` 不会因命令缺失失败。
    Considerations:
        - 若未来需要固定 `rg` 版本，可在此处扩展版本检测逻辑。
    """

    if shutil.which("rg") is None:
        raise ContractCheckError("未找到 ripgrep，请安装 `rg` 或在 CI 镜像中预装该工具。")


def load_whitelist(repo_root: Path) -> List[str]:
    """加载核心契约白名单。

    Why:
        - 白名单由架构师维护，脚本通过读取它来获取应当受保护的契约名称。
    How:
        - 使用 `tomllib` 解析 `contracts/core_contract_whitelist.toml`，
          读取 `names` 字段并进行基础校验。
    What:
        - 返回类型名列表，按照文件中出现的顺序保留顺序。
    Contract:
        - 前置：白名单文件存在且语法正确；
        - 后置：列表非空且所有元素为非空字符串。
    Considerations:
        - 若缺少字段或列表为空，会抛出 `ContractCheckError`，
          防止脚本在无保护名单的情况下继续运行。
    """

    whitelist_file = repo_root / WHITELIST_PATH
    if not whitelist_file.is_file():
        raise ContractCheckError(f"缺少白名单文件: {WHITELIST_PATH}。请先完成 T-001A 并提交清单。")

    try:
        data = tomllib.loads(whitelist_file.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:  # type: ignore[attr-defined]
        raise ContractCheckError(f"解析 {WHITELIST_PATH} 失败: {exc}。") from exc

    raw_names = data.get("names")
    if not isinstance(raw_names, list):
        raise ContractCheckError(f"{WHITELIST_PATH} 缺少 `names` 列表，请参考示例结构进行维护。")

    names: List[str] = []
    for idx, entry in enumerate(raw_names, start=1):
        if not isinstance(entry, str) or not entry.strip():
            raise ContractCheckError(
                f"{WHITELIST_PATH} 第 {idx} 个条目无效，预期为非空字符串。当前值: {entry!r}"
            )
        names.append(entry.strip())

    if not names:
        raise ContractCheckError(f"{WHITELIST_PATH} 的 `names` 为空，无法进行契约守卫。")

    return names


def search_symbol(repo_root: Path, symbol: str) -> List[SymbolMatch]:
    """调用 ripgrep 搜索契约定义。

    Why:
        - 将“白名单条目 → 实际定义位置”转化为结构化列表，便于后续判断是否越界。
    How:
        - 构造形如 `^\s*pub\s+(struct|enum|trait|type)\s+Name\b` 的正则表达式，
          使用 `rg` 在仓库根目录递归搜索，并解析输出为 `SymbolMatch` 对象。
    What:
        - 返回匹配列表，可能为空（表示白名单条目在仓库中不存在）。
    Contract:
        - 前置：`rg` 可执行，`repo_root` 指向仓库根。
        - 后置：所有 `SymbolMatch.path` 均为绝对路径且以 `repo_root` 作为前缀。
    Considerations:
        - 若 `rg` 返回码既非 0 也非 1，视为执行失败并抛出错误。
        - 正则仅匹配 `pub` 定义，若未来需要覆盖 `pub(crate)` 等，可扩展模式。
    """

    pattern = RG_PATTERN_TEMPLATE.format(name=re.escape(symbol))
    result = subprocess.run(
        [
            "rg",
            "--with-filename",
            "--line-number",
            "--no-heading",
            "--color",
            "never",
            pattern,
            str(repo_root),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode not in (0, 1):
        raise ContractCheckError(
            f"执行 ripgrep 搜索 `{symbol}` 失败 (退出码 {result.returncode})：{result.stderr.strip()}"
        )

    matches: List[SymbolMatch] = []
    kind_pattern = re.compile(r"^\s*pub\s+(struct|enum|trait|type)\b")

    for raw_line in result.stdout.splitlines():
        if not raw_line.strip():
            continue
        try:
            path_str, line_str, snippet = raw_line.split(":", 2)
            line_no = int(line_str)
        except ValueError as exc:
            raise ContractCheckError(
                f"无法解析 ripgrep 输出行：{raw_line!r}。请检查正则模式或 ripgrep 版本。"
            ) from exc

        kind_match = kind_pattern.match(snippet)
        if not kind_match:
            raise ContractCheckError(
                f"无法解析命中行的声明类型：{raw_line!r}。请确认正则表达式是否过于宽泛。"
            )

        matches.append(
            SymbolMatch(
                symbol=symbol,
                path=(Path(path_str).resolve()),
                line=line_no,
                snippet=snippet.strip(),
                item_kind=kind_match.group(1),
            )
        )

    return matches

def is_allowed_location(repo_root: Path, match: SymbolMatch) -> bool:
    """判断命中项是否位于允许的核心目录。"""

    try:
        relative = match.path.relative_to(repo_root)
    except ValueError:
        return False

    return any(relative.parts[: len(prefix.parts)] == prefix.parts for prefix in ALLOWED_PREFIXES)


def main() -> int:
    """脚本主入口：串联白名单加载、搜索与违规检测。

    Why:
        - 提供统一的执行流程，便于在 CI 与本地直接运行。
    How:
        - 计算仓库根目录 → 校验依赖 → 加载白名单 → 遍历类型执行搜索 → 汇总结果。
    What:
        - 成功时返回 0，失败时返回非 0 并输出诊断信息。
    Contract:
        - 前置：满足 `ensure_rg_available` 与 `load_whitelist` 的前置条件。
        - 后置：
          * 若出现违规/缺失，输出中文提示并返回 1；
          * 无异常则打印通过信息。
    Considerations:
        - 为保持输出一致性，所有日志都使用 `print`。
    """

    repo_root = Path(__file__).resolve().parent.parent

    try:
        ensure_rg_available()
        names = load_whitelist(repo_root)
    except ContractCheckError as exc:
        print(f"[contract-guard][ERROR] {exc}", file=sys.stderr)
        return 1

    violations: List[SymbolMatch] = []
    missing: List[str] = []

    for name in names:
        try:
            matches = search_symbol(repo_root, name)
        except ContractCheckError as exc:
            print(f"[contract-guard][ERROR] {exc}", file=sys.stderr)
            return 1

        if not matches:
            missing.append(name)
            continue

        allowed_matches = [m for m in matches if is_allowed_location(repo_root, m)]
        external_matches = [m for m in matches if not is_allowed_location(repo_root, m)]

        if not allowed_matches:
            missing.append(name)
            violations.extend(external_matches)
            continue

        allowed_kinds = {m.item_kind for m in allowed_matches}

        for match in external_matches:
            if allowed_kinds and match.item_kind not in allowed_kinds:
                continue
            violations.append(match)

    if violations or missing:
        if violations:
            print("[contract-guard][ERROR] 检测到以下契约类型在非 spark-core 路径被重新定义：", file=sys.stderr)
            for violation in violations:
                try:
                    rel_path = violation.path.relative_to(repo_root)
                except ValueError:
                    rel_path = violation.path
                print(
                    f"  - {violation.symbol}: {rel_path}:{violation.line} :: {violation.snippet}",
                    file=sys.stderr,
                )
        if missing:
            print(
                "[contract-guard][ERROR] 白名单中的契约类型未在仓库中找到定义: "
                + ", ".join(sorted(missing)),
                file=sys.stderr,
            )
        print("[contract-guard][SUGGEST] 请仅在 crates/spark-core 内维护核心契约，或更新白名单以反映最新现状。", file=sys.stderr)
        return 1

    print("[contract-guard][OK] 所有核心契约类型仅在 crates/spark-core 中定义。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
