# Runbook：SparkServiceErrorRateHigh（服务错误率升高）

- **告警来源**：Prometheus 规则 [`SparkServiceErrorRateHigh`](../observability/alerts.yaml)
- **对应仪表盘**：`core-runtime.json`（UID: `spark-core-runtime`）
- **目标恢复时间**：告警触发后 15 分钟内将错误率压回 <2%
- **升级联系人**：平台服务 Owner (@spark-platform-oncall)

## 1. 快速确认（T+0 ~ 5 分钟）

1. 打开 Grafana 仪表盘「服务成功率」面板，确认曲线低于 95% 且与告警时间吻合。
2. 对比变量 `service_name`、`route_id`，确认是否只影响单一 API 或多条路径。
3. 检查最近 30 分钟内是否有相关上线/配置变更（可在发布平台或 #spark-release 频道查询）。

> 如存在全局性大面积下降，立即进入“紧急降级/限流”流程，优先保障核心链路。

## 2. 定位排查（T+5 ~ 10 分钟）

| 步骤 | 操作 | 说明 |
| --- | --- | --- |
| A | 查看「请求吞吐量 (RPS)」面板 | 判断是否伴随流量突增导致资源压力。 |
| B | 过滤 `status_code` 标签 | 在 Prometheus 中执行 `sum by (status_code) (rate(spark_request_total{service_name="$service",route_id="$route"}[5m]))`，识别主要错误码。 |
| C | 关联「入站/出站字节」面板 | 若字节量暴涨，检查是否有异常大包或上传风暴。 |
| D | 检查依赖错误 | 使用链路追踪或应用日志，确认是否由于下游（如存储、外部 API）报错。 |
| E | 核对资源限额 | 对照 Runbook《资源限额》或监控中 `spark_limits_usage`，避免因限流触发。 |

## 3. 恢复动作（T+10 ~ 15 分钟）

1. **回滚/降级**：若定位到最近上线引入问题，执行快速回滚或开启灰度降级开关。
2. **限流/熔断**：若为下游依赖故障，可根据限流策略暂时降低流量或启用缓存兜底。
3. **扩容实例**：在确认资源不足时，扩容对应的服务副本或提升限额（需遵守双人确认）。
4. **数据清洗**：针对异常 payload，可部署临时过滤规则或拒绝策略，防止错误持续扩大。

## 4. 恢复验证

- Grafana 中的「成功率」需在 10 分钟内稳定回升至 98% 以上。
- 告警应在 2 个评估周期内自动恢复，Prometheus `ALERTS{alertname="SparkServiceErrorRateHigh"}` 状态变为 `inactive`。
- 新增的回滚/限流操作应在值班频道记录，包括开始/结束时间与影响评估。

## 5. 后续跟进

1. 复盘会议：24 小时内补充复盘材料，包含根因、影响面、改进项。
2. 自动化：评估是否需要为类似场景补充单元测试、流量回放或额外告警。
3. 文档更新：如本 Runbook 步骤有缺失或不适用，请提交 PR 补充。
