# Spark 运行值班 Runbook 总览

本目录提供 Spark 平台在值班/应急场景下的标准化处置指南，覆盖“服务错误率升高”、“Codec 解码错误飙升”、“传输层连接失败”三类高频告警。每份 Runbook 均配套：

- **仪表盘入口**：指向 `docs/observability/dashboards/` 中的对应 JSON，便于快速定位问题面板；
- **排查步骤**：按照“确认 → 定位 → 恢复 → 验证”分阶段行动；
- **回溯资料**：整理常见触发原因、推荐的日志/命令以及升级路径。

> **演练建议**：结合 `docs/observability/dashboards/sample-stack.yaml` 启动样例环境，并运行脚本 `scripts/push_synthetic_metrics.sh` 触发告警，确保 5 分钟内完成定位与恢复演练。

## Runbook 列表

| 告警名称 | Runbook | 场景摘要 |
| --- | --- | --- |
| `SparkServiceErrorRateHigh` | [服务错误率升高](service-error-rate.md) | 上游依赖异常、版本回滚、资源限流导致的 5xx 激增 |
| `SparkCodecErrorSpike` | [Codec 解码错误飙升](codec-error-spike.md) | 新协议/字段上线导致反序列化失败 |
| `SparkTransportChannelFailures` | [传输层通道失败](transport-channel-failures.md) | TLS、证书或网络策略异常，握手阶段持续失败 |

## 值班通用约定

1. **时间线记录**：所有重大操作需在值班频道记录时间点、命令与结果，便于复盘。
2. **回滚优先**：当确认最近 30 分钟内存在上线/配置变更且关联错误，请优先执行可逆操作（回滚、切换流量、降级）。
3. **双人确认**：涉及生产数据写入、证书替换、配置热更新等高风险操作须由另一名值班同学复核。
4. **升级路径**：若 Runbook 指定的恢复步骤 15 分钟内无效，请根据表格中的“升级联系人”通知对应团队负责人。
