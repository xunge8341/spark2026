# Runbook：SparkTransportChannelFailures（传输层通道失败）

- **告警来源**：Prometheus 规则 [`SparkTransportChannelFailures`](../observability/alerts.yaml)
- **对应仪表盘**：`transport-health.json`（UID: `spark-transport-health`）
- **目标恢复时间**：20 分钟内恢复握手成功率 > 99%
- **升级联系人**：网络与安全团队 (@spark-network)

## 1. 快速确认（T+0 ~ 5 分钟）

1. 打开 Grafana 仪表盘的「连接尝试/失败」面板，确认失败曲线持续高于 0。
2. 查看变量 `listener_id`、`transport_protocol`，确定是否集中在单个入口或跨集群。
3. 在告警信息中记录的 `error_kind`（如 `timeout`、`tls_handshake`）可作为初步线索。

## 2. 定位排查（T+5 ~ 12 分钟）

| 步骤 | 操作 | 说明 |
| --- | --- | --- |
| A | 检查证书状态 | 执行 `openssl s_client -connect <host>:<port>`，确认证书是否过期或链不完整。 |
| B | 查看「握手 P90 延迟」 | 若延迟陡增，可能为网络抖动或 TLS 协商异常。 |
| C | 核对防火墙/安全组 | 通过云平台或 NetOps 工具确认是否新增拒绝策略。 |
| D | 回放最近变更 | 检查是否更换了监听地址、升级了传输协议版本或证书。 |
| E | 分析客户端日志 | 若 `peer_role=client` 同样失败，需联系客户端团队排查版本兼容性。 |

推荐 PromQL：

```promql
sum by (error_kind) (increase(spark_transport_channel_failures{listener_id="$listener"}[5m]))
```

## 3. 恢复动作

1. **证书恢复**：若为证书过期，立刻切换至备用证书或执行热更新，完成后重启对应 listener。
2. **回滚网络策略**：撤销最近的 ACL/防火墙策略变更，并请求 NetOps 协助确认路由健康。
3. **降级协议**：将 `transport_protocol` 暂时回退到上一稳定版本（如从 `quic` → `tcp`）。
4. **扩容负载均衡**：若为容量瓶颈，扩容前端负载均衡或增加 listener 副本数。

## 4. 恢复验证

- 「连接尝试/失败」面板中的失败曲线需回落至 0，并维持至少 10 分钟。
- `spark_transport_handshake_duration` 的 P90 延迟恢复至基线（可对比历史 1h）。
- Prometheus 告警状态恢复为 `inactive` 后，在值班频道公告恢复时间与根因。

## 5. 后续跟进

1. 证书管理：确认证书轮换流程，增加过期前 30 天的提前预警。
2. 网络监控：评估是否需补充四层健康探测或跨 Region 链路观测。
3. 文档与自动化：更新部署手册中的网络依赖项，并考虑加入连接失败自愈脚本。
