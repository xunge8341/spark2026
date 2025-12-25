"""
错误矩阵映射：从错误码到分类模板。

Why:
    - 客户端可在 TCK 中验证错误处理逻辑是否与服务端一致。
How:
    - 由生成器读取 contracts/error_matrix.toml 自动产出；包含 Why/How 文档以便教学。
What:
    - ERROR_MATRIX: Dict[str, dict]，键为错误码，值含 kind/wait_ms 等字段。
"""

from typing import Dict

ERROR_MATRIX: Dict[str, dict] = {
    "app.backpressure_applied": {"busy":"downstream","kind":"retryable","rationale":"业务端主动背压，需遵守等待窗口。","reason":"下游正在施加背压，遵循等待窗口","tuning":"可结合速率控制调整等待时间。","wait_ms":180},
    "app.routing_failed": {"kind":"non_retryable","rationale":"目标服务不存在或路由失败，需业务人工干预。","tuning":"在具备兜底副本时可标记为 Retryable。"},
    "app.unauthorized": {"class":"authorization","kind":"security","rationale":"权限不足，记录安全事件并关闭通道。","tuning":"可通过自定义 Handler 触发补偿。"},
    "cluster.leader_lost": {"busy":"upstream","kind":"retryable","rationale":"集群拓扑暂时不可用，建议延后重试。","reason":"集群节点暂不可用，稍后重试","tuning":"可根据集群健康状况调整等待。","wait_ms":250},
    "cluster.network_partition": {"busy":"upstream","kind":"retryable","rationale":"集群拓扑暂时不可用，建议延后重试。","reason":"集群节点暂不可用，稍后重试","tuning":"可根据集群健康状况调整等待。","wait_ms":250},
    "cluster.node_unavailable": {"busy":"upstream","kind":"retryable","rationale":"集群拓扑暂时不可用，建议延后重试。","reason":"集群节点暂不可用，稍后重试","tuning":"可根据集群健康状况调整等待。","wait_ms":250},
    "cluster.queue_overflow": {"budget":"flow","kind":"resource_exhausted","rationale":"集群队列满，触发速率背压。","tuning":"可扩容队列或自定义 BudgetKind。"},
    "cluster.service_not_found": {"kind":"non_retryable","rationale":"目标服务不存在或路由失败，需业务人工干预。","tuning":"在具备兜底副本时可标记为 Retryable。"},
    "discovery.stale_read": {"busy":"upstream","kind":"retryable","rationale":"服务发现数据陈旧，等待刷新后再试。","reason":"服务发现数据陈旧，等待刷新","tuning":"可结合监控加长等待时长。","wait_ms":120},
    "protocol.budget_exceeded": {"budget":"decode","kind":"resource_exhausted","rationale":"解码预算耗尽，触发背压。","tuning":"可在 CallContext 注册定制预算。"},
    "protocol.decode": {"close_message":"检测到协议契约违规，已触发优雅关闭","kind":"protocol_violation","rationale":"协议契约被破坏，必须关闭连接。","tuning":"若协议允许纠正，可改写为 Retryable。"},
    "protocol.negotiation": {"close_message":"检测到协议契约违规，已触发优雅关闭","kind":"protocol_violation","rationale":"协议契约被破坏，必须关闭连接。","tuning":"若协议允许纠正，可改写为 Retryable。"},
    "protocol.type_mismatch": {"close_message":"检测到协议契约违规，已触发优雅关闭","kind":"protocol_violation","rationale":"协议契约被破坏，必须关闭连接。","tuning":"若协议允许纠正，可改写为 Retryable。"},
    "router.version_conflict": {"close_message":"检测到协议契约违规，已触发优雅关闭","kind":"protocol_violation","rationale":"协议契约被破坏，必须关闭连接。","tuning":"若协议允许纠正，可改写为 Retryable。"},
    "runtime.shutdown": {"kind":"cancelled","rationale":"运行时已进入关闭流程，终止后续逻辑。","tuning":"若需等待收尾，可改写为 Retryable。"},
    "transport.io": {"busy":"downstream","kind":"retryable","rationale":"典型 TCP/QUIC I/O 故障，建议短暂退避。","reason":"传输层 I/O 故障，等待链路恢复后重试","tuning":"可根据重试策略调整等待窗口。","wait_ms":150},
    "transport.timeout": {"kind":"timeout","rationale":"传输层请求超时，触发调用取消。","tuning":"若需继续等待，可显式改写分类。"},
}
