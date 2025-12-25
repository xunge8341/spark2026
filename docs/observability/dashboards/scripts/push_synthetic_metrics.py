#!/usr/bin/env python3
"""向 Prometheus Pushgateway 写入 Spark 样例指标。

该脚本用于 docs/observability/dashboards/README.md 中的“五分钟快速启动”。
- 通过环境变量调整流量规模与异常比例；
- 每次推送覆盖 Service / Codec / Transport / Limits 的核心指标；
- 确保告警 `SparkServiceErrorRateHigh`、`SparkCodecErrorSpike`、`SparkTransportChannelFailures` 均可触发与恢复。
"""
from __future__ import annotations

import os
import random
import sys
import time
import urllib.request
from dataclasses import dataclass
from typing import Dict, Iterable, List


@dataclass
class Histogram:
    """维护 Prometheus 直方图的累计值。

    教案说明：

    - **意图与定位**：抽象出 Pushgateway 演示脚本内部“指标累加器”的职责，确保多次推送之间保持单调递增，贴合 Prometheus 对直方图的契约；位置位于样例栈的 synthetic metrics 层。
    - **设计要点**：通过保存 `_bucket_totals`、`_count`、`_sum` 三类内部状态，将增量观测转换为累计值；采用 dataclass 简化属性定义，避免手写 `__init__`。
    - **输入输出契约**：构造函数要求 `bounds` 与 `values` 长度匹配（最后一项代表 `+Inf`）；`labels` 为静态标签集，确保 Pushgateway 推送时不缺字段。
    - **权衡说明**：相比直接使用第三方库（如 `prometheus_client`），本实现内嵌轻量逻辑，减少依赖；代价是需要手动维护桶分布，但更易讲解和改造。
    - **维护提示**：任何新增的直方图需保证 `values` 总长度 = `len(bounds) + 1`，否则 `__post_init__` 会抛出异常，避免静默出错。

    Attributes:
        name: 基础指标名（不含 `_bucket` 后缀）。
        bounds: 有限桶的上界列表（最后一个桶为 +Inf 自动推导）。
        values: 用于估算 `_sum` 的代表值，长度须为 `len(bounds) + 1`。
        labels: 应用于该直方图的固定标签。
    """

    name: str
    bounds: List[float]
    values: List[float]
    labels: Dict[str, str]

    def __post_init__(self) -> None:
        if len(self.values) != len(self.bounds) + 1:
            raise ValueError("values length must equal bounds length + 1")
        self._bucket_totals: List[float] = [0.0 for _ in self.bounds]
        self._count: float = 0.0
        self._sum: float = 0.0

    def observe(self, total: int, weights: Iterable[float]) -> None:
        """根据权重分配样本并更新直方图。

        - **意图**：在生成合成数据时，用可读的权重描述“采样分布”，转换为 Prometheus 所需的累计桶值，支撑 Dashboard 中的分位数计算。
        - **前置条件**：`total` ≥ 0 且 `weights` 的长度等于 `len(bounds)+1`；调用方须保证该轮观测的事件数与上游流量相符。
        - **实现逻辑**：
          1. 归一化/修正权重，调用 `_allocate_counts` 得到整数分配；
          2. 迭代累加生成“<= bound”的累计值，更新 `_bucket_totals`；
          3. 同步维护 `_count` 与 `_sum`，为 `_count`、`_sum` 度量提供单调递增的原始值。
        - **后置条件**：所有桶的数值都会大于等于上一轮调用；若 `total=0` 则不会产生任何 side effect。
        - **边界提醒**：若传入负权重或 total=0，函数会分别进行纠正或提前返回；这是为了避免生成无意义或降序的指标值。
        """

        weights_list = list(weights)
        if total < 0:
            raise ValueError("total must be non-negative")
        if len(weights_list) != len(self.bounds) + 1:
            raise ValueError("weights length mismatch")
        if total == 0:
            return

        allocations = _allocate_counts(total, weights_list)
        cumulative = 0
        for idx, increment in enumerate(allocations[:-1]):
            cumulative += increment
            self._bucket_totals[idx] += cumulative
        self._count += total
        self._sum += sum(alloc * val for alloc, val in zip(allocations, self.values))

    def render(self) -> List[str]:
        """输出 Prometheus 文本格式的行列表。"""

        lines: List[str] = []
        for bound, total in zip(self.bounds, self._bucket_totals):
            labels = dict(self.labels)
            labels["le"] = _format_bound(bound)
            lines.append(_format_sample(f"{self.name}_bucket", labels, total))
        labels = dict(self.labels)
        labels["le"] = "+Inf"
        lines.append(_format_sample(f"{self.name}_bucket", labels, self._count))
        lines.append(_format_sample(f"{self.name}_sum", self.labels, self._sum))
        lines.append(_format_sample(f"{self.name}_count", self.labels, self._count))
        return lines


def _allocate_counts(total: int, weights: List[float]) -> List[int]:
    """按照权重分配整数计数，保证总和等于 `total`。

    - **意图**：解决“浮点权重 → 整数桶计数”的离散化问题，避免 Pushgateway 接收小数导致的指标错误。
    - **实现策略**：先基于权重算出理想值，再用“取整 + 分配剩余”的办法保持总和；剩余部分通过比较小数部分大小进行轮询补偿，最大化还原比例。
    - **输入契约**：允许传入负权重，但会在内部强制提升为 0，保证结果合理；`total` 为非负整数。
    - **后置条件**：返回列表长度与输入权重一致、所有元素皆为非负整数且总和等于 `total`。
    - **性能考量**：该函数在 15 秒一次的推送节奏下开销极低，因此优先选择可读性更高的 Python 实现。
    """

    raw = [max(0.0, w) for w in weights]
    weight_sum = sum(raw)
    if weight_sum <= 0:
        # 平均分配
        avg = total / len(raw)
        raw = [avg for _ in raw]
    else:
        raw = [w * total / weight_sum for w in raw]

    ints = [int(x) for x in raw]
    remainder = total - sum(ints)
    # 根据小数部分从大到小补齐剩余的样本
    fractions = sorted(
        ((raw[i] - ints[i], i) for i in range(len(raw))),
        key=lambda item: item[0],
        reverse=True,
    )
    for idx in range(remainder):
        target = fractions[idx % len(fractions)][1]
        ints[target] += 1
    return ints


def _format_bound(bound: float) -> str:
    """格式化直方图桶边界。

    - **意图**：兼容 Prometheus 文本协议对 `le` 标签的要求，整数边界不带小数，浮点保持精度。
    - **输入/输出**：接受任意 `float`，返回字符串；调用方负责传入语义正确的边界。
    - **注意事项**：若未来调整为高精度浮点，可在此统一改写格式策略，避免散落多处的格式化逻辑。
    """

    return "{:.0f}".format(bound) if bound.is_integer() else str(bound)


def _format_sample(name: str, labels: Dict[str, str], value: float) -> str:
    """构造 Prometheus 文本协议中的一行样本。

    - **意图**：统一处理标签排序与数值格式，减少重复字符串拼接代码。
    - **前置条件**：`labels` 必须是可序列化为字符串的键值对；本函数不会逃逸特殊字符，调用方应提前清洗。
    - **实现细节**：按照标签名排序保证输出稳定性，便于 diff 与调试；数值保留 6 位小数，兼顾可读性与精度。
    - **后置条件**：返回形如 `metric_name{label="value"} 123.456000` 的字符串。
    """

    label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{label_str}}} {value:.6f}"


def push_metrics(payload: str, url: str) -> None:
    """通过 HTTP PUT 将指标写入 Pushgateway。

    - **意图**：封装网络交互细节，调用者只需提供文本格式 payload。
    - **前置条件**：`payload` 已满足 Prometheus 文本协议；`url` 指向受信任的 Pushgateway endpoint。
    - **实现细节**：使用标准库 `urllib.request`，避免额外依赖；由于目标是本地样例环境，跳过证书校验。
    - **后置条件**：若请求成功，Pushgateway 即刻可被 Prometheus 拉取；异常会向上抛出，由调用者决定是否重试。
    - **风险提示**：若 URL 不可达，将抛出 `URLError`；在主循环中需捕获并打印，避免脚本直接退出。
    """

    data = payload.encode("utf-8")
    request = urllib.request.Request(url, data=data, method="PUT")
    with urllib.request.urlopen(request) as response:  # noqa: S310 - 受信任的内部地址
        response.read()


def main() -> int:
    """脚本入口：生成并推送合成指标。

    - **意图**：为演示环境提供可控制的指标流量，确保 Dashboard、告警、Runbook 可以在 5 分钟内联动。
    - **高层逻辑**：
      1. 读取环境变量初始化基线（请求速率、错误比例、握手 P90 等）；
      2. 为 Service/Codec/Transport/Limits 构造直方图与计数器状态；
      3. 在无限循环中计算增量 → 生成文本 → 调用 `push_metrics`；
      4. 使用随机扰动模拟真实波动，并输出时间戳日志方便演练。
    - **契约**：返回 `0` 表示正常退出（或被 `Ctrl+C` 中断）；任何异常会打印后继续下一轮推送。
    - **权衡**：脚本以可读性为主，没有额外优化极端性能；在 15s 间隔下 CPU 占用可忽略。
    - **边界提醒**：若 Prometheus 未连接 Pushgateway，指标不会出现；Runbook 演练前需确认 `docker compose` 栈已启动。
    """

    pushgateway_url = os.getenv(
        "PUSHGATEWAY_URL",
        "http://localhost:9091/metrics/job/spark_demo/instance/local",
    )
    interval = float(os.getenv("PUSH_INTERVAL", "15"))
    base_rps = float(os.getenv("BASE_RPS", "120"))
    error_ratio = float(os.getenv("ERROR_RATIO", "0.02"))
    codec_error_ratio = float(os.getenv("CODEC_ERROR_RATIO", "0.006"))
    failure_rate = float(os.getenv("FAILURE_RATE", "0.004"))
    handshake_p90 = float(os.getenv("HANDSHAKE_P90", "45"))

    rng = random.Random()

    service_labels = {
        "service_name": os.getenv("SERVICE_NAME", "spark-demo"),
        "route_id": os.getenv("ROUTE_ID", "v1"),
        "operation": os.getenv("OPERATION", "GET"),
        "protocol": os.getenv("PROTOCOL", "http"),
        "status_code": "200",
        "outcome": "success",
    }
    error_labels = dict(service_labels)
    error_labels["outcome"] = "error"
    error_labels["status_code"] = "500"

    codec_labels = {
        "codec_name": os.getenv("CODEC_NAME", "serde-json"),
        "codec_mode": "encode",
        "content_type": os.getenv("CONTENT_TYPE", "application/json"),
    }
    codec_decode_labels = dict(codec_labels)
    codec_decode_labels["codec_mode"] = "decode"

    transport_labels = {
        "transport_protocol": os.getenv("TRANSPORT_PROTOCOL", "tcp"),
        "listener_id": os.getenv("LISTENER_ID", "ingress:8443"),
        "peer_role": "server",
    }
    client_transport_labels = dict(transport_labels)
    client_transport_labels["peer_role"] = "client"

    request_hist = Histogram(
        name="spark_request_duration",
        bounds=[10, 20, 50, 100, 200, 500, 1000],
        values=[7, 15, 35, 75, 150, 350, 700, 1100],
        labels={k: v for k, v in service_labels.items() if k not in {"outcome", "status_code"}},
    )
    codec_encode_hist = Histogram(
        name="spark_codec_encode_duration",
        bounds=[5, 10, 20, 40, 80, 160],
        values=[3, 8, 15, 30, 60, 140, 220],
        labels=codec_labels,
    )
    codec_decode_hist = Histogram(
        name="spark_codec_decode_duration",
        bounds=[5, 10, 20, 40, 80, 160],
        values=[4, 9, 17, 34, 70, 150, 260],
        labels=codec_decode_labels,
    )
    handshake_hist = Histogram(
        name="spark_transport_handshake_duration",
        bounds=[20, 40, 80, 160, 320],
        values=[12, 25, 55, 120, 220, 400],
        labels={
            "transport_protocol": transport_labels["transport_protocol"],
            "listener_id": transport_labels["listener_id"],
            "peer_role": transport_labels["peer_role"],
            "result": "ok",
        },
    )
    if handshake_p90 > 0:
        scale = handshake_p90 / 90.0
        handshake_hist.values = [value * scale for value in handshake_hist.values]

    success_counter = 0
    error_counter = 0
    inbound_bytes = 0
    outbound_bytes = 0
    codec_encode_bytes = 0
    codec_decode_bytes = 0
    codec_encode_errors = 0
    codec_decode_errors = 0
    channel_attempts = 0
    channel_failures = 0
    transport_bytes_in = 0
    transport_bytes_out = 0

    limits_usage = 55.0
    limits_limit = 100.0
    queue_depth = 0.0
    limits_hit = 0

    while True:
        delta = max(1, int(round(base_rps * interval * (1 + rng.uniform(-0.05, 0.05)))))
        error_delta = max(0, int(round(delta * max(0.0, error_ratio) * (1 + rng.uniform(-0.3, 0.3)))))
        error_delta = min(error_delta, delta)
        success_delta = delta - error_delta

        # 更新请求相关计数
        success_counter += success_delta
        error_counter += error_delta
        inbound_bytes += success_delta * rng.randint(1200, 1900) + error_delta * rng.randint(400, 700)
        outbound_bytes += success_delta * rng.randint(1800, 2600) + error_delta * rng.randint(600, 900)

        request_hist.observe(delta, [0.06, 0.18, 0.32, 0.22, 0.12, 0.06, 0.03, 0.01])

        # 编解码指标
        codec_total = delta + rng.randint(-20, 20)
        codec_total = max(codec_total, 0)
        codec_error_delta = max(
            0,
            int(round(codec_total * max(0.0, codec_error_ratio) * (1 + rng.uniform(-0.2, 0.2)))),
        )
        codec_encode_hist.observe(max(codec_total, 0), [0.12, 0.25, 0.28, 0.2, 0.1, 0.04, 0.01])
        codec_decode_hist.observe(max(codec_total, 0), [0.1, 0.22, 0.3, 0.22, 0.1, 0.05, 0.01])
        codec_encode_bytes += max(codec_total, 0) * rng.randint(800, 1400)
        codec_decode_bytes += max(codec_total, 0) * rng.randint(900, 1500)
        codec_encode_errors += codec_error_delta // 2
        codec_decode_errors += codec_error_delta - codec_error_delta // 2

        # 传输层指标
        channel_attempts += max(delta + rng.randint(-15, 20), 0)
        failure_delta = max(0, int(round(delta * max(0.0, failure_rate) * (1 + rng.uniform(-0.25, 0.25)))))
        channel_failures += failure_delta
        transport_bytes_in += delta * rng.randint(4000, 6000)
        transport_bytes_out += delta * rng.randint(4200, 6400)
        handshake_hist.observe(max(delta - failure_delta, 0), [0.08, 0.32, 0.36, 0.18, 0.05, 0.01])

        # 资源限额：在 80% 左右波动，并偶尔触发 hit
        limits_usage += rng.uniform(-1.5, 1.5)
        limits_usage = min(max(limits_usage, 40.0), 92.0)
        if limits_usage > 85:
            queue_depth = max(queue_depth + rng.uniform(1, 4), 0.0)
            if rng.random() < 0.3:
                limits_hit += 1
        else:
            queue_depth = max(queue_depth - rng.uniform(0.5, 1.5), 0.0)

        inflight = int(max(0, rng.gauss(25, 4)))

        lines: List[str] = []
        lines.append("# TYPE spark_request_total counter")
        lines.append(_format_sample("spark_request_total", service_labels, success_counter))
        lines.append(_format_sample("spark_request_total", error_labels, error_counter))
        lines.append("# TYPE spark_request_inflight gauge")
        lines.append(_format_sample("spark_request_inflight", {k: service_labels[k] for k in ("service_name", "route_id", "operation", "protocol")}, inflight))
        lines.append("# TYPE spark_request_duration histogram")
        lines.extend(request_hist.render())
        lines.append("# TYPE spark_bytes_inbound counter")
        lines.append(_format_sample("spark_bytes_inbound", {k: service_labels[k] for k in ("service_name", "route_id", "operation", "protocol")}, inbound_bytes))
        lines.append("# TYPE spark_bytes_outbound counter")
        lines.append(_format_sample("spark_bytes_outbound", {k: service_labels[k] for k in ("service_name", "route_id", "operation", "protocol")}, outbound_bytes))

        lines.append("# TYPE spark_codec_encode_duration histogram")
        lines.extend(codec_encode_hist.render())
        lines.append("# TYPE spark_codec_decode_duration histogram")
        lines.extend(codec_decode_hist.render())
        lines.append("# TYPE spark_codec_encode_bytes counter")
        lines.append(_format_sample("spark_codec_encode_bytes", codec_labels, codec_encode_bytes))
        lines.append("# TYPE spark_codec_decode_bytes counter")
        lines.append(_format_sample("spark_codec_decode_bytes", codec_decode_labels, codec_decode_bytes))
        lines.append("# TYPE spark_codec_encode_errors counter")
        lines.append(_format_sample("spark_codec_encode_errors", {**codec_labels, "error_kind": "serialization"}, codec_encode_errors))
        lines.append("# TYPE spark_codec_decode_errors counter")
        lines.append(_format_sample("spark_codec_decode_errors", {**codec_decode_labels, "error_kind": "deserialization"}, codec_decode_errors))
        lines.append("# TYPE spark_codec_encode_duration_count counter")
        lines.append(_format_sample("spark_codec_encode_duration_count", codec_labels, codec_encode_hist._count))
        lines.append("# TYPE spark_codec_encode_duration_sum counter")
        lines.append(_format_sample("spark_codec_encode_duration_sum", codec_labels, codec_encode_hist._sum))
        lines.append("# TYPE spark_codec_decode_duration_count counter")
        lines.append(_format_sample("spark_codec_decode_duration_count", codec_decode_labels, codec_decode_hist._count))
        lines.append("# TYPE spark_codec_decode_duration_sum counter")
        lines.append(_format_sample("spark_codec_decode_duration_sum", codec_decode_labels, codec_decode_hist._sum))

        lines.append("# TYPE spark_transport_connections gauge")
        server_connections = 180 + rng.randint(-10, 10)
        client_connections = 60 + rng.randint(-5, 5)
        lines.append(_format_sample("spark_transport_connections", transport_labels, server_connections))
        lines.append(_format_sample("spark_transport_connections", client_transport_labels, client_connections))
        lines.append("# TYPE spark_transport_channel_attempts counter")
        lines.append(_format_sample("spark_transport_channel_attempts", {**transport_labels, "result": "ok"}, channel_attempts))
        lines.append("# TYPE spark_transport_channel_failures counter")
        lines.append(_format_sample("spark_transport_channel_failures", {**transport_labels, "error_kind": "timeout"}, channel_failures))
        lines.append("# TYPE spark_transport_bytes_inbound counter")
        lines.append(_format_sample("spark_transport_bytes_inbound", transport_labels, transport_bytes_in))
        lines.append("# TYPE spark_transport_bytes_outbound counter")
        lines.append(_format_sample("spark_transport_bytes_outbound", transport_labels, transport_bytes_out))
        lines.append("# TYPE spark_transport_handshake_duration histogram")
        lines.extend(handshake_hist.render())

        limit_labels = {
            "limit_resource": "concurrency",
            "limit_action": "shed",
        }
        lines.append("# TYPE spark_limits_usage gauge")
        lines.append(_format_sample("spark_limits_usage", limit_labels, limits_usage))
        lines.append("# TYPE spark_limits_limit gauge")
        lines.append(_format_sample("spark_limits_limit", limit_labels, limits_limit))
        lines.append("# TYPE spark_limits_queue_depth gauge")
        lines.append(_format_sample("spark_limits_queue_depth", limit_labels, queue_depth))
        lines.append("# TYPE spark_limits_hit counter")
        lines.append(_format_sample("spark_limits_hit", limit_labels, limits_hit))

        payload = "\n".join(lines) + "\n"
        try:
            push_metrics(payload, pushgateway_url)
            print(time.strftime("%H:%M:%S"), "pushed synthetic metrics", flush=True)
        except Exception as exc:  # noqa: BLE001 - 直接向终端输出错误
            print(f"推送失败: {exc}", file=sys.stderr)
        time.sleep(max(interval, 1.0))

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n已停止推送。")
        sys.exit(0)
