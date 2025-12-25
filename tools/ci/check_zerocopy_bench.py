#!/usr/bin/env python3
"""零拷贝极端基准校验脚本。

Why:
    * CI 需要自动判断 `zerocopy_extremes` 基准是否满足 “BufView P99 相对基线 ≤ 3%”。
    * 仅当样本稳定（CV < 0.2）时才具备比较意义，因此脚本需显式验证离散程度。

Where:
    * 位于 `tools/ci/`，由 `make ci-bench-smoke` 以及独立 CI 步骤调用。
    * 入口函数 [`main`] 会在读取 `cargo bench` 输出后立即执行判定。

Trade-offs:
    * 解析逻辑约束在单一的 `regex` 模式内，保持脚本易维护，但前提是基准输出格式稳定；
    * 若未来输出字段增减，需要同步更新正则表达式与数据类。
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


# 与 `println!` 的输出字段一一对应，便于后续正则抽取。
LINE_PATTERN = re.compile(
    r"scenario=(?P<scenario>\S+)\s+operation=(?P<operation>\S+)\s+"
    r"bufview_p99_ns_per_kb=(?P<bufview_p99>[-+eE0-9\.]+)\s+"
    r"baseline_p99_ns_per_kb=(?P<baseline_p99>[-+eE0-9\.]+)\s+"
    r"p99_overhead_pct=(?P<overhead_pct>[-+eE0-9\.]+)\s+"
    r"bufview_cv_ns_per_kb=(?P<bufview_cv>[-+eE0-9\.]+)\s+"
    r"baseline_cv_ns_per_kb=(?P<baseline_cv>[-+eE0-9\.]+)"
)


@dataclass
class BenchObservation:
    """承载单个场景 × 操作的测量数据。

    Why:
        * 将正则解析出的原始字符串转换为强类型，避免后续逻辑重复解析。
        * 通过数据类的形式，令字段含义一目了然，方便扩展。

    How:
        * 字段与 `println!` 中的键值完全对齐。
        * 所有浮点值统一转换为 `float` 以便后续数学运算。

    What:
        * `scenario` / `operation`：唯一定位某条记录的标签。
        * `bufview_p99_ns_per_kb` 等字段：直接映射到输出中的数值，单位保持与基准一致。
        * 前置条件：调用方必须确保来源于合法的基准输出。
        * 后置条件：实例创建后视为不可变数据快照，供评估函数只读使用。

    Trade-offs:
        * 没有实现行为方法，仅做数据承载，保持脚本逻辑单一。
    """

    scenario: str
    operation: str
    bufview_p99_ns_per_kb: float
    baseline_p99_ns_per_kb: float
    p99_overhead_pct: float
    bufview_cv: float
    baseline_cv: float


def parse_bench_output(path: Path) -> List[BenchObservation]:
    """解析 `cargo bench` 生成的文本输出。

    Why:
        * 基准输出包含大量无关信息（构建日志、其他测试输出），需要在 CI 中提炼出
          可判定的核心指标。

    Where:
        * 调用位置：[`main`] 的首个步骤，在任何校验之前确保数据完整。

    How:
        * 逐行读取文件，使用 `LINE_PATTERN` 正则匹配目标行；
        * 将匹配结果转换为浮点数/字符串，收集为 [`BenchObservation`] 列表；
        * 若发现格式不符，立即抛出异常终止 CI，以避免误判。

    What:
        * 参数 `path`：待解析文件路径，必须指向可读的文本文件。
        * 返回值：提取到的 [`BenchObservation`] 列表，按出现顺序排列。
        * 前置条件：基准执行成功并产出了至少一条 `scenario=` 日志。
        * 后置条件：若返回列表非空，则其中所有字段均已转换为相应类型。

    Trade-offs:
        * 选择正则而非 `split()`，可以容忍字段顺序固定但空格数量浮动的场景；
        * 若未来需要解析更多字段，只需扩展正则和数据类即可。
    """

    if not path.exists():
        raise FileNotFoundError(f"基准输出文件不存在: {path}")

    observations: List[BenchObservation] = []
    with path.open("r", encoding="utf-8") as handler:
        for raw_line in handler:
            line = raw_line.strip()
            if not line or "scenario=" not in line:
                continue
            matched = LINE_PATTERN.search(line)
            if not matched:
                raise ValueError(
                    "无法解析的基准输出行: "
                    f"{line}. 请确认 zerocopy_extremes 输出格式是否变化"
                )
            observations.append(
                BenchObservation(
                    scenario=matched.group("scenario"),
                    operation=matched.group("operation"),
                    bufview_p99_ns_per_kb=float(matched.group("bufview_p99")),
                    baseline_p99_ns_per_kb=float(matched.group("baseline_p99")),
                    p99_overhead_pct=float(matched.group("overhead_pct")),
                    bufview_cv=float(matched.group("bufview_cv")),
                    baseline_cv=float(matched.group("baseline_cv")),
                )
            )

    if not observations:
        raise ValueError("未在基准输出中找到任何 zerocopy_extremes 记录")

    return observations


def detect_quick_mode(path: Path) -> bool:
    """判定当前基准输出是否以 `quick_mode` 执行。 

    Why:
        * 快速模式只采样极少次数，常在无隔离的 CI 环境触发大幅抖动，强行套用
          “P99 ≤ 3%” 的生产阈值会产生大量误报。
        * 当运行者明确需要严格校验时，可通过环境变量重新启用约束；因此脚本需先
          判定是否处于快速模式以便切换行为。

    Where:
        * 由 [`main`] 在解析数据前调用；输入源为 `cargo bench` 输出文件。

    How:
        * 顺序扫描文本行，只要捕获 `quick_mode=true` 关键字即判定为快速模式。
        * 读取过程与 [`parse_bench_output`] 分离，避免在教学或调试时耦合解析逻辑。

    What:
        * 参数 `path`：指向 `cargo bench` 输出的文件路径，要求文件可读。
        * 返回值：布尔值，`True` 表示快速模式。
        * 前置条件：文件存在且可打开；否则将抛出 `FileNotFoundError`。
        * 后置条件：函数不会修改文件，只返回判定结果。

    Trade-offs:
        * 选择简单的关键字检测，而非完整解析结构化元数据，原因是 `quick_mode`
          仅作为单一布尔标记，避免引入额外依赖或复杂度。
        * 若未来输出格式发生变化（例如键名调整），需同步更新关键字。
    """

    with path.open("r", encoding="utf-8") as handler:
        for raw_line in handler:
            if "quick_mode=true" in raw_line:
                return True
    return False


def evaluate_results(
    observations: Iterable[BenchObservation],
    *,
    cv_threshold: float = 0.2,
    overhead_limit_pct: float = 3.0,
) -> List[str]:
    """根据稳定性与相对开销规则生成诊断信息。

    Why:
        * SLO 要求 BufView P99 额外开销 ≤ 3%，但前提是样本稳定，否则判定毫无意义。

    Where:
        * 在 `main` 中解析完数据后调用，集中产生所有违规项。

    How:
        * 遍历所有观测值：
            1. 若 BufView CV ≥ `cv_threshold`，立即记录噪音过大的错误；
            2. 若基线 P99 ≤ 0，则报告“基线无效”；
            3. 否则重新计算 BufView 相对基线的 P99 开销，与 `overhead_limit_pct` 比较。
        * 所有错误汇总后返回，以便一次性向用户展示。

    What:
        * 参数：
            - `observations`：来自 [`parse_bench_output`] 的记录集合；
            - `cv_threshold`：CV 阈值，默认 0.2；
            - `overhead_limit_pct`：允许的最大额外开销百分比。
        * 返回值：错误消息字符串列表；为空表示所有约束满足。
        * 前置条件：`observations` 至少包含一项。
        * 后置条件：函数不会修改输入，只返回诊断信息。

    Trade-offs:
        * 采用重新计算的开销值而非直接使用输出的 `p99_overhead_pct`，防止格式化
          四舍五入带来的 ±0.001% 误判；
        * 同时检查基线的 CV 虽然不是硬性要求，但当其明显失真时也会给出提示。
    """

    errors: List[str] = []

    for entry in observations:
        if entry.bufview_cv >= cv_threshold:
            errors.append(
                f"场景 {entry.scenario} 操作 {entry.operation} 的 BufView CV={entry.bufview_cv:.3f}"
                f" ≥ {cv_threshold:.3f}，采样噪音过大"
            )
            continue

        if entry.baseline_cv >= cv_threshold:
            errors.append(
                f"场景 {entry.scenario} 操作 {entry.operation} 的基线 CV={entry.baseline_cv:.3f}"
                f" ≥ {cv_threshold:.3f}，基准对照不稳定"
            )
            continue

        if entry.baseline_p99_ns_per_kb <= 0.0:
            errors.append(
                f"场景 {entry.scenario} 操作 {entry.operation} 的基线 P99"
                " ≤ 0，无法计算相对开销"
            )
            continue

        relative_pct = ((entry.bufview_p99_ns_per_kb / entry.baseline_p99_ns_per_kb) - 1.0) * 100.0
        if relative_pct - overhead_limit_pct > 1e-6:
            errors.append(
                f"场景 {entry.scenario} 操作 {entry.operation} 的 P99 额外开销"
                f" 为 {relative_pct:.3f}% > {overhead_limit_pct:.3f}%"
            )

    return errors


def main(argv: List[str]) -> int:
    """脚本入口：解析文件、评估结果并输出诊断。

    Why:
        * CI 命令需要显式的退出码：0 表示通过，非 0 代表失败，便于流程自动化。

    Where:
        * 由命令 `python3 tools/ci/check_zerocopy_bench.py bench.out` 调用。

    How:
        * 解析命令行参数，默认读取 `bench.out`；
        * 调用 [`parse_bench_output`] 和 [`evaluate_results`] 获取诊断；
        * 若有错误，逐行打印并返回 1；否则打印成功摘要并返回 0。

    What:
        * 参数 `argv`：通常为 `sys.argv[1:]`；
        * 返回值：整数退出码；
        * 前置条件：脚本运行目录与 `bench.out` 一致，或传入绝对路径；
        * 后置条件：控制台输出包含判定详情，供 CI 日志检索。

    Trade-offs:
        * 采用显式的路径参数而非 STDIN，方便用户保留完整的 bench 输出做进一步分析。
    """

    target = Path(argv[0]) if argv else Path("bench.out")
    try:
        quick_mode = detect_quick_mode(target)
        observations = parse_bench_output(target)
        if quick_mode and os.getenv("ZEROCOPY_BENCH_STRICT") != "1":
            print(
                "zerocopy_bench_check_skipped: quick_mode=true，"
                "默认跳过严格阈值校验；如需强制验证请设置 ZEROCOPY_BENCH_STRICT=1"
            )
            return 0
        violations = evaluate_results(observations)
    except Exception as exc:  # noqa: BLE001 - 需捕获并回显任意异常以便 CI 诊断
        print(f"zerocopy_bench_check_error: {exc}", file=sys.stderr)
        return 1

    if violations:
        print("zerocopy_bench_check_failed:")
        for message in violations:
            print(f"  - {message}")
        return 1

    total = sum(1 for _ in observations)
    print(
        "zerocopy_bench_check_passed: "
        f"所有 {total} 条记录满足 P99 额外开销 ≤ {3.0:.3f}% 且 CV < {0.2:.3f}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
