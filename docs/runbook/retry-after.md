# Runbook：SparkServiceRetryAfterBurst（RetryAfter 短期爆发）

- **告警来源**：Prometheus 规则 [`SparkServiceRetryAfterBurst`](../observability/prometheus-rules.yml)
- **对应仪表盘**：`observability/grafana/ready-state.json`（UID: `spark-ready-state-overview`）
- **目标恢复时间**：告警触发后 10 分钟内将 RetryAfter 的出现频率压回基线（≤1 次/分钟）
- **升级联系人**：平台服务 Owner (@spark-platform-oncall)

## 1. 快速确认（T+0 ~ 5 分钟）

1. 打开 Grafana 仪表盘「ReadyState 占比（5m increase）」与「ReadyState 事件次数（5m increase）」面板，确认告警对应的服务/路由在最近 15 分钟内出现多次 RetryAfter，并观察 Ready 占比是否同步下滑（若 `SparkServiceReadyBaselineDrop` 告警亦触发，应优先处理背压根因）。
2. 在 Prometheus 中执行 `sum by (service_name, route_id) (increase(spark_request_retry_after_total{service_name="$service",route_id="$route"}[5m]))` 复核次数趋势。
3. 检查是否为计划内运维变更（发布、限流演练、数据库维护等），若是，请及时更新告警静默窗口。

> 若告警伴随 ReadyState 的 `budget_exhausted` 或错误率升高，应优先参考对应 Runbook，并同步协同基础设施值班团队。

## 2. 定位排查（T+5 ~ 15 分钟）

| 步骤 | 操作 | 说明 |
| --- | --- | --- |
| A | 对比 ReadyState 细分标签 | 在仪表盘中查看 `ready.detail`，判断是 `after`（短期退避）还是 `custom`（业务自定义）。 |
| B | 检查请求队列深度 | 通过 `spark_limits_queue_depth` 或自建指标确认队列是否逼近上限。 |
| C | 审核预算配置 | 查看服务配置中的并发/预算参数，确认是否近期调整过限额。 |
| D | 追踪下游响应 | 若 `ready.detail="upstream"` 或 `downstream`，联动依赖方检查最近的错误日志与限流策略。 |
| E | 还原用户行为 | 从访问日志或 A/B 系统确认是否为特定租户/产品新功能引发的突发流量。 |

## 3. 恢复动作（T+15 ~ 30 分钟）

1. **扩展退避窗口**：若短期需求高峰导致退避，可临时将客户端或网关的退避策略改为指数回退，并确保最大退避时间不超过 SLA。
2. **临时限流/熔断**：针对恶意或异常流量，启用更严格的限流阈值，必要时对特定租户进行熔断，避免拖垮核心流量。
3. **服务降级**：若下游能力受限，可启用缓存、返回兜底数据或关闭非关键特性，以释放预算。
4. **扩容与配额提升**：评估是否需要临时扩容实例、提升预算上限或解除自动化降级限制（需与 Capacity Owner 双人确认）。

## 4. 恢复验证

- RetryAfter 告警应在 2 个评估周期内降为 `inactive`，Prometheus `ALERTS{alertname="SparkServiceRetryAfterBurst"}` 恢复正常。
- Grafana 中 RetryAfter 总量和延迟分布需回落至基线，`spark_request_ready_state` 中 `ready.state="retry_after"` 比例低于 1%。
- 将采取的退避/限流/降级措施记录在值班频道，包含开始时间、结束时间与影响范围。

## 5. 后续跟进

1. 追踪根因：在 24 小时内完成根因分析，包括触发条件、被动影响及恢复策略评估。
2. 改进策略：若为预算配置不足，评估是否需要自动扩缩容或更细粒度的配额策略；若为客户端退避策略不足，推动 SDK 升级。
3. 文档更新：如本 Runbook 步骤存在缺口，请提交 PR 更新，并同步在平台知识库打标。
