#!/usr/bin/env python3
"""Chaos 场景执行与回放工具。

该模块提供以下能力：

* 解析 `tools/chaos/scenarios` 目录下的 JSON 场景定义；
* 在 Dry-run 或真实环境中执行注入步骤并记录指标；
* 将执行记录落地为 JSON 以便回放和生成演练报告。

设计考量
================
1. **以注释驱动的“教案级”可读性** —— 所有核心函数均详细说明意图、契约以及潜在风险，方便后续扩展。
2. **最小依赖** —— 仅使用标准库，以保证脚本可直接运行在运维 bastion 或 CI 中。
3. **可扩展的行动模型** —— 通过 `ChaosStep.action` 描述动作类型，方便未来扩展 HTTP、K8s API 等操作。
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import pathlib
import subprocess
import sys
import textwrap
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional


SCENARIO_DIR = pathlib.Path(__file__).resolve().parent / "scenarios"
RUN_DIR = pathlib.Path(__file__).resolve().parent / "runs"


@dataclass
class SLOExpectation:
    """描述单个指标的约束条件。

    意图说明 (Why)
    ----------------
    * 该数据结构用于在注入过程中校验关键指标是否满足演练预期。
    * 它将业务 SLO 的名称、比较方式与阈值绑定，以便在执行与报告生成阶段统一处理。
    * 通过显式建模 `comparison`，我们可以兼容“越小越好”与“越大越好”两类指标。

    逻辑拆解 (How)
    ----------------
    * `comparison` 仅支持 `<=` 与 `>=`，由 `ChaosRunner._assert_slo` 解释并执行比较。
    * 阈值统一转换为 `float`，避免整数与小数比较时的隐式类型差异。

    契约定义 (What)
    ----------------
    * :param name: 指标在监控体系中的唯一名称，例如 `latency_p95`。
    * :param comparison: 取值为 `"<="` 或 `">="`，表示阈值方向。
    * :param threshold: 指标允许的最大或最小阈值，使用浮点表示。
    * 调用方应保证比较符号在上述集合内，否则构造阶段会抛出 `ValueError`。
    * 本结构没有返回值，仅为数据载体。

    设计权衡与风险 (Trade-offs)
    -----------------------------
    * 为避免逻辑分散在多个模块内，阈值校验交由 `ChaosRunner` 执行，这意味着若未来需要自定义比较逻辑，需要在 Runner 层做扩展。
    * `threshold` 没有单位信息，默认为同监控系统一致的单位；Runbook 中需强调这一点避免误用。
    """

    name: str
    comparison: str
    threshold: float

    def __post_init__(self) -> None:
        """在数据类初始化后完成有效性校验。

        意图说明
        ~~~~~~~~~
        * 保证比较符号仅在允许范围内，避免运行期才发现配置错误。

        逻辑拆解
        ~~~~~~~~~
        * 直接检查 `comparison` 是否为预期集合中的元素，不满足则抛出 `ValueError`。

        契约定义
        ~~~~~~~~~
        * 前置条件：`comparison` 在 JSON 配置中已提供。
        * 后置条件：若比较符合法，实例被认为有效；否则抛出异常终止流程。

        风险提示
        ~~~~~~~~~
        * 若后续新增比较符号，必须同步更新该校验逻辑，否则会导致合法配置无法加载。
        """

        if self.comparison not in {"<=", ">="}:
            raise ValueError(f"不支持的 comparison: {self.comparison}")


@dataclass
class ChaosStep:
    """定义单个注入步骤。

    意图说明
    --------
    * 将每个注入动作抽象成可序列化的数据结构，便于通过 JSON 进行版本化管理。
    * 支持 Shell 命令与指标检查等动作类型，使演练脚本覆盖“注入 + 验证”全链路。

    逻辑拆解
    --------
    * `action` 描述动作类型；当前支持 `shell` 与 `check-metric`。
    * `command` 和 `query` 分别服务于不同动作类型；通过 `ChaosRunner._execute_step` 分发执行。
    * `expected_slo` 用于在步骤完成后判定指标是否达标。

    契约定义
    --------
    * :param id: 场景内唯一的步骤 ID，用于日志和回放。
    * :param action: `shell` 或 `check-metric`，大小写敏感。
    * :param command: 当 `action == 'shell'` 时需要提供的命令字符串。
    * :param query: 当 `action == 'check-metric'` 时使用的监控查询语句。
    * :param rollback: 该步骤的回滚命令，Runbook 与回放功能会使用该信息。
    * :param expected_slo: 如果提供，则在步骤完成后进行阈值校验。
    * 调用者需保证 `command`、`query` 与 `action` 匹配，否则执行阶段会抛出异常。

    风险提示
    --------
    * `shell` 动作默认通过 `subprocess.run` 执行，任何非零退出码会中断整个场景；需要在 JSON 中显式提供幂等的回滚命令。
    * `check-metric` 依赖外部监控 API，若接口不可达需在 Runbook 中指定人工兜底方案。
    """

    id: str
    action: str
    command: Optional[str] = None
    query: Optional[str] = None
    rollback: Optional[str] = None
    expected_slo: Optional[SLOExpectation] = None
    observation: Optional[str] = None


@dataclass
class ChaosScenario:
    """混沌场景的聚合定义。

    意图说明
    --------
    * 聚合基础信息（标题、描述、稳态检测）与步骤列表，确保 Runner 在执行前即可获得全貌。
    * 提供 `from_dict` 构造器，将 JSON 映射为强类型，避免魔法字符串散落各处。

    逻辑拆解
    --------
    * `steady_state_check` 仅用于 Runbook 展示，当前脚本不执行自动检测，但保持结构以便未来扩展。
    * `from_dict` 将子字段递归构造成数据类实例，期间会触发各数据类的校验逻辑。

    契约定义
    --------
    * :param id: 场景唯一标识，对应 JSON 文件名。
    * :param title: 面向人的标题，用于报告与 Runbook。
    * :param description: 场景目标说明。
    * :param impact: 对业务的影响预期。
    * :param steady_state_check: 稳态检测配置字典。
    * :param steps: `ChaosStep` 列表。
    * 通过 `from_dict` 生成实例时，要求 `steps` 至少包含一个元素。

    风险提示
    --------
    * 若 JSON 文件缺失必填字段，将在加载阶段抛出 `KeyError` 或 `ValueError`；脚本启动时即可发现配置问题。
    """

    id: str
    title: str
    description: str
    impact: str
    steady_state_check: Dict[str, Any]
    steps: List[ChaosStep] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChaosScenario":
        """根据 JSON 字典构造场景实例。

        意图说明
        ~~~~~~~~~
        * 在加载配置时执行结构化解析与校验，避免运行过程中处理弱类型数据。

        逻辑拆解
        ~~~~~~~~~
        1. 读取基础字段并直接传入构造函数；
        2. 遍历 `steps` 数组，对 `expected_slo` 子字段构造 `SLOExpectation`；
        3. 将 `ChaosStep` 列表作为最终参数传入。

        契约定义
        ~~~~~~~~~
        * :param data: 从 JSON 文件解析得到的字典。
        * :return: `ChaosScenario` 实例。
        * 前置条件：`data` 包含 `id`、`title`、`description`、`impact`、`steady_state_check`、`steps`。
        * 后置条件：返回的实例 `steps` 非空，否则抛出 `ValueError`。

        风险提示
        ~~~~~~~~~
        * 若 JSON 中 `expected_slo` 字段存在但缺失 `comparison` 或 `threshold`，构造 `SLOExpectation` 会失败，提示具体原因。
        * 解析失败会在启动阶段终止脚本，提醒维护者修正配置。
        """

        steps: List[ChaosStep] = []
        for raw_step in data["steps"]:
            slo = None
            if "expected_slo" in raw_step and raw_step["expected_slo"]:
                slo = SLOExpectation(
                    name=raw_step["expected_slo"]["name"],
                    comparison=raw_step["expected_slo"]["comparison"],
                    threshold=float(raw_step["expected_slo"]["threshold"]),
                )
            steps.append(
                ChaosStep(
                    id=raw_step["id"],
                    action=raw_step["action"],
                    command=raw_step.get("command"),
                    query=raw_step.get("query"),
                    rollback=raw_step.get("rollback"),
                    expected_slo=slo,
                    observation=raw_step.get("observation"),
                )
            )

        if not steps:
            raise ValueError(f"场景 {data['id']} 未定义任何步骤")

        return cls(
            id=data["id"],
            title=data["title"],
            description=data["description"],
            impact=data["impact"],
            steady_state_check=data.get("steady_state_check", {}),
            steps=steps,
        )


class ChaosRunRecorder:
    """负责在磁盘上持久化混沌演练记录。

    意图说明
    --------
    * 通过统一的 `append` 接口，将每次演练的元数据与步骤结果写入 JSON，满足“可回放”的目标。

    逻辑拆解
    --------
    * 构造函数确保 `RUN_DIR` 存在，以免写文件时失败。
    * `create_run` 生成带时间戳的文件路径，用于区分多次演练。
    * `append_step` 将步骤执行结果追加到内存中的记录结构，最终在 `finalize` 时一次性写盘，避免频繁 IO。

    契约定义
    --------
    * :param scenario: 当前执行的 `ChaosScenario`。
    * :param note: 演练说明，例如值班人或变更单号。
    * :param dry_run: 是否为演练预演（仅打印命令）。
    * 使用顺序：`create_run()` -> 多次 `append_step()` -> `finalize()`。
    * 若中途抛出异常，调用方应在异常处理逻辑中调用 `abort()` 以记录失败状态。

    风险提示
    --------
    * 若调用方在执行过程中忘记调用 `finalize()`，则不会生成记录文件；建议在 `ChaosRunner.run` 中使用 `try/finally` 保证落盘。
    """

    def __init__(self, scenario: ChaosScenario, note: Optional[str], dry_run: bool) -> None:
        self.scenario = scenario
        self.note = note
        self.dry_run = dry_run
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        safe_note = note.replace(" ", "_") if note else ""
        suffix = f"_{safe_note}" if safe_note else ""
        self.file_path = RUN_DIR / f"{timestamp}_{scenario.id}{suffix}.json"
        self.payload: Dict[str, Any] = {
            "scenario_id": scenario.id,
            "scenario_title": scenario.title,
            "started_at": timestamp,
            "note": note,
            "dry_run": dry_run,
            "steps": [],
        }
        self._finalized = False

    def append_step(self, step: ChaosStep, status: str, detail: Dict[str, Any]) -> None:
        """记录单个步骤的执行结果。

        意图说明
        ~~~~~~~~~
        * 维持按顺序的步骤执行历史，方便后续生成报告或进行回放。

        逻辑拆解
        ~~~~~~~~~
        * 将 `status`、`detail` 与步骤元信息合并，压入 `payload['steps']` 列表。

        契约定义
        ~~~~~~~~~
        * :param step: 当前步骤。
        * :param status: `success`、`failed` 或 `skipped`。
        * :param detail: 附加信息，例如命令、输出、指标值。
        * 前置条件：`create_run` 已完成初始化。
        * 后置条件：内部状态更新，等待 `finalize` 落盘。

        风险提示
        ~~~~~~~~~
        * 未调用 `finalize` 前数据仅存于内存，若脚本崩溃数据将丢失；在 `ChaosRunner.run` 中需通过异常捕获保证落盘。
        """

        self.payload["steps"].append(
            {
                "step_id": step.id,
                "action": step.action,
                "status": status,
                "detail": detail,
            }
        )

    def finalize(self) -> None:
        """将记录写入磁盘文件。

        意图说明
        ~~~~~~~~~
        * 在所有步骤执行完毕后持久化记录，形成回放依据。

        契约定义
        ~~~~~~~~~
        * 后置条件：生成的 JSON 文件包含完整的执行数据。
        * 若多次调用则忽略后续调用，避免重复写盘。
        """

        if self._finalized:
            return
        with self.file_path.open("w", encoding="utf-8") as handle:
            json.dump(self.payload, handle, ensure_ascii=False, indent=2)
        self._finalized = True

    def abort(self, error: str) -> None:
        """在发生异常时记录失败原因。

        意图说明
        ~~~~~~~~~
        * 避免因异常导致记录缺失，通过写入 `failed` 字段保留上下文。

        契约定义
        ~~~~~~~~~
        * :param error: 错误描述。
        * 后置条件：JSON 记录中的 `failed` 字段包含错误信息。
        """

        self.payload["failed"] = error
        self.finalize()


class MetricClient:
    """简单的指标查询客户端。

    意图说明
    --------
    * 为 `check-metric` 步骤提供统一的数据访问入口，避免每个步骤自行拼接 HTTP 请求。

    逻辑拆解
    --------
    * 通过 `urllib.request` 调用 GET 接口，期望返回 JSON，其中 `value` 字段为数值。
    * 若未提供 `endpoint`，则以 Dry-run 方式返回 `None` 并提示用户。

    契约定义
    --------
    * :param endpoint: Prometheus 等监控系统的 HTTP API 地址，示例：`https://monitor/api/v1/slo`。
    * `fetch_value` 参数 `query` 为查询语句，函数返回浮点值或 `None`。
    * 前置条件：若 `endpoint` 为 `None`，调用者需自行处理返回 `None` 的情形。

    风险提示
    --------
    * 该实现仅支持返回 JSON 结构 `{ "value": <number> }`，若监控系统返回格式不同，需要在此类中扩展解析逻辑。
    * 缺少重试机制；若需要更强健的网络容错，可在未来迭代中引入。
    """

    def __init__(self, endpoint: Optional[str]) -> None:
        self.endpoint = endpoint

    def fetch_value(self, query: str) -> Optional[float]:
        """执行一次指标查询。

        意图说明
        ~~~~~~~~~
        * 为 `check-metric` 提供可编程的指标值，支撑自动化 SLO 校验。

        逻辑拆解
        ~~~~~~~~~
        * 若未配置 endpoint，直接返回 `None` 并输出提示。
        * 将查询语句作为 `query` 参数拼接到 endpoint，发起 GET 请求并解析 JSON。

        契约定义
        ~~~~~~~~~
        * :param query: PromQL 或其他监控查询语言表达式。
        * :return: 浮点数或 `None`，当 `endpoint` 缺失或响应异常时返回 `None`。
        * 前置条件：当 `endpoint` 存在时，该 URL 可达且返回 JSON。
        * 后置条件：若成功解析，返回值可用于与 `SLOExpectation` 进行比较。

        风险提示
        ~~~~~~~~~
        * 当前未捕获网络异常，失败会抛出异常并中断演练；Runbook 建议在正式演练前先用 `--dry-run` 验证连通性。
        """

        if not self.endpoint:
            print("[metric] 未配置 endpoint，跳过自动查询。", file=sys.stderr)
            return None

        url = f"{self.endpoint}?{urllib.parse.urlencode({'query': query})}"
        with urllib.request.urlopen(url) as response:  # type: ignore[call-arg]
            payload = json.loads(response.read().decode("utf-8"))
        value = payload.get("value")
        return float(value) if value is not None else None


class ChaosRunner:
    """混沌场景的执行调度器。"""

    def __init__(self, scenarios: Dict[str, ChaosScenario], metrics_endpoint: Optional[str]) -> None:
        self.scenarios = scenarios
        self.metrics = MetricClient(metrics_endpoint)

    def list_scenarios(self) -> Iterable[ChaosScenario]:
        """按 ID 排序返回全部场景。"""

        return [self.scenarios[key] for key in sorted(self.scenarios.keys())]

    def show(self, scenario_id: str) -> ChaosScenario:
        """返回指定场景，若不存在则抛出异常。"""

        if scenario_id not in self.scenarios:
            raise KeyError(f"未找到场景 {scenario_id}")
        return self.scenarios[scenario_id]

    def run(self, scenario_id: str, *, dry_run: bool, note: Optional[str]) -> pathlib.Path:
        """执行指定场景并返回记录文件路径。

        意图说明
        --------
        * 串联加载、执行、记录三个阶段，是脚本的核心入口。

        逻辑拆解
        --------
        1. 选择场景并初始化 `ChaosRunRecorder`；
        2. 逐步调用 `_execute_step` 执行动作；
        3. 对每个步骤执行 `_assert_slo` 校验并写入记录；
        4. 全部成功后 `finalize` 输出 JSON。

        契约定义
        --------
        * :param scenario_id: 需执行的场景 ID。
        * :param dry_run: 若为 True，则仅打印命令不执行。
        * :param note: 可选备注，如变更单号。
        * :return: 记录文件的绝对路径。
        * 前置条件：`scenario_id` 存在于 `self.scenarios`。
        * 后置条件：`runs/` 目录中新建一条 JSON 记录，若执行失败则写入失败原因。

        风险提示
        --------
        * 若某步骤失败，后续步骤将停止执行；Runbook 应指导在失败时如何手动回滚。
        * `dry_run=False` 时务必确认具备对应权限，否则 Shell 命令可能失败。
        """

        scenario = self.show(scenario_id)
        recorder = ChaosRunRecorder(scenario, note=note, dry_run=dry_run)
        try:
            for step in scenario.steps:
                result = self._execute_step(step, dry_run=dry_run)
                slo_report = self._assert_slo(step, result)
                recorder.append_step(step, status=result["status"], detail={**result, **slo_report})
            recorder.finalize()
            return recorder.file_path
        except Exception as exc:  # noqa: BLE001 - 保持异常信息用于记录
            recorder.abort(str(exc))
            raise

    def _execute_step(self, step: ChaosStep, *, dry_run: bool) -> Dict[str, Any]:
        """执行单个步骤并返回结果字典。

        意图说明
        --------
        * 根据步骤类型执行不同逻辑，并收集输出、指标等信息，为记录与 SLO 校验提供素材。

        逻辑拆解
        --------
        * `shell`：打印命令并根据 `dry_run` 决定是否执行 `subprocess.run`。
        * `check-metric`：通过 `MetricClient` 查询指标值。
        * 其他动作类型会触发 `ValueError`，提醒维护者扩展实现。

        契约定义
        --------
        * :param step: 场景步骤。
        * :param dry_run: 是否为预演。
        * :return: 包含 `status`、`output`、`metric_value` 等键的字典。
        * 前置条件：`step.action` 在支持列表内，且所需字段已配置。
        * 后置条件：返回的 `status` 为 `success` 或 `skipped`，失败时抛出异常。

        风险提示
        --------
        * 对于 `shell` 动作，命令执行失败会抛出 `subprocess.CalledProcessError`，并在上层被记录；Runbook 中需描述人工回滚步骤。
        * 若 `check-metric` 查询失败（网络错误或解析失败），异常会冒泡，提醒维护者修复监控 API。
        """

        detail: Dict[str, Any] = {
            "status": "success",
            "observation": step.observation,
        }

        if step.action == "shell":
            if not step.command:
                raise ValueError(f"步骤 {step.id} 缺少 command")
            print(f"[shell] {step.command}")
            detail["command"] = step.command
            if dry_run:
                detail["status"] = "skipped"
                detail["output"] = "dry-run 未执行"
            else:
                completed = subprocess.run(step.command, shell=True, check=True, capture_output=True, text=True)
                detail["stdout"] = completed.stdout
                detail["stderr"] = completed.stderr
        elif step.action == "check-metric":
            if not step.query:
                raise ValueError(f"步骤 {step.id} 缺少 query")
            value = self.metrics.fetch_value(step.query)
            detail["metric_query"] = step.query
            detail["metric_value"] = value
            if value is None:
                detail["status"] = "skipped"
        else:
            raise ValueError(f"未知的步骤类型: {step.action}")

        return detail

    def _assert_slo(self, step: ChaosStep, result: Dict[str, Any]) -> Dict[str, Any]:
        """根据步骤执行结果校验 SLO，并返回附加报告。

        意图说明
        --------
        * 将 SLO 校验逻辑集中在一个函数中，确保所有步骤的比较行为一致。

        逻辑拆解
        --------
        * 若步骤未定义 `expected_slo`，直接返回空字典。
        * 对 `shell` 动作，没有指标可读，则直接返回期望描述供报告使用。
        * 对 `check-metric`，若取到指标值则进行数值比较，否则标记为 `pending`。

        契约定义
        --------
        * :param step: 当前步骤。
        * :param result: `_execute_step` 返回的字典。
        * :return: 包含 `slo_status`、`slo_value`、`slo_threshold` 等键的字典。
        * 前置条件：`result` 中可能包含 `metric_value`。
        * 后置条件：若指标值存在且超阈，抛出 `AssertionError` 中断后续步骤。

        风险提示
        --------
        * 若监控指标暂不可用，将返回 `pending`，不会阻断流程；需要值班人员在报告中注明人工验证结果。
        """

        if not step.expected_slo:
            return {}

        report = {
            "slo_name": step.expected_slo.name,
            "slo_threshold": step.expected_slo.threshold,
            "slo_comparison": step.expected_slo.comparison,
        }

        value = result.get("metric_value")
        if value is None:
            report["slo_status"] = "pending"
            return report

        report["slo_value"] = value
        comparison = step.expected_slo.comparison
        threshold = step.expected_slo.threshold

        if comparison == "<=":
            passed = value <= threshold
        else:
            passed = value >= threshold

        if not passed:
            report["slo_status"] = "violated"
            raise AssertionError(
                f"步骤 {step.id} 的指标 {step.expected_slo.name} = {value} 未满足 {comparison} {threshold}"
            )

        report["slo_status"] = "met"
        return report


def load_scenarios() -> Dict[str, ChaosScenario]:
    """从场景目录加载全部 JSON 配置。"""

    scenarios: Dict[str, ChaosScenario] = {}
    for path in SCENARIO_DIR.glob("*.json"):
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        scenario = ChaosScenario.from_dict(data)
        scenarios[scenario.id] = scenario
    return scenarios


def build_parser() -> argparse.ArgumentParser:
    """构建命令行解析器。"""

    parser = argparse.ArgumentParser(description="混沌注入脚本：执行、回放与报告")
    parser.add_argument("command", choices=["list", "show", "run", "replay"], help="要执行的子命令")
    parser.add_argument("scenario", nargs="?", help="场景 ID")
    parser.add_argument("run_record", nargs="?", help="回放时指定的记录文件")
    parser.add_argument("--metrics-endpoint", dest="metrics_endpoint", help="监控查询 API 地址")
    parser.add_argument("--dry-run", action="store_true", help="仅打印命令")
    parser.add_argument("--note", help="备注信息，例如变更单号或值班人")
    return parser


def replay(record_path: pathlib.Path) -> None:
    """回放既有演练记录并打印摘要。"""

    with record_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    header = textwrap.dedent(
        f"""
        场景: {payload['scenario_id']} - {payload['scenario_title']}
        启动时间: {payload['started_at']}
        Dry-run: {payload['dry_run']}
        备注: {payload.get('note') or '无'}
        失败原因: {payload.get('failed') or '无'}
        """
    ).strip()
    print(header)
    print("步骤明细：")
    for step in payload.get("steps", []):
        print(f"- {step['step_id']} ({step['action']}): {step['status']}")
        detail = step.get("detail", {})
        for key, value in detail.items():
            if key == "status":
                continue
            print(f"    · {key}: {value}")


def main(argv: Optional[List[str]] = None) -> int:
    """脚本入口。"""

    parser = build_parser()
    args = parser.parse_args(argv)
    scenarios = load_scenarios()
    runner = ChaosRunner(scenarios, metrics_endpoint=args.metrics_endpoint)

    if args.command == "list":
        for scenario in runner.list_scenarios():
            print(f"{scenario.id}: {scenario.title}")
        return 0
    if args.command == "show":
        if not args.scenario:
            parser.error("show 命令需要指定场景 ID")
        scenario = runner.show(args.scenario)
        print(json.dumps(
            {
                "id": scenario.id,
                "title": scenario.title,
                "description": scenario.description,
                "impact": scenario.impact,
                "steady_state_check": scenario.steady_state_check,
                "steps": [step.__dict__ for step in scenario.steps],
            },
            ensure_ascii=False,
            indent=2,
        ))
        return 0
    if args.command == "run":
        if not args.scenario:
            parser.error("run 命令需要指定场景 ID")
        path = runner.run(args.scenario, dry_run=args.dry_run, note=args.note)
        print(f"记录文件: {path}")
        return 0
    if args.command == "replay":
        if not args.run_record:
            parser.error("replay 命令需要提供记录文件路径")
        replay(pathlib.Path(args.run_record))
        return 0

    parser.error(f"未知命令 {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
