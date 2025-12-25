# Runbook：SparkPipelineEpochDrift（Pipeline 纪元落后）

- **告警来源**：Prometheus 规则 [`SparkPipelineEpochDrift`](../observability/prometheus-rules.yml)
- **对应仪表盘**：`observability/grafana/pipeline.json`（UID: `spark-pipeline-epoch`）
- **目标恢复时间**：告警触发后 15 分钟内让数据面 `epoch` 追平最新 `config_epoch`
- **升级联系人**：平台配置 Owner (@spark-config-oncall)

## 1. 快速确认（T+0 ~ 5 分钟）

1. 在 Grafana 仪表盘查看「Pipeline 纪元对比」面板，确认数据面 `epoch` 是否明显落后于 `config_epoch`，并记录差值。
2. 在「实例级纪元明细」面板筛选 `instance` 标签，确定是否只有个别副本未拉取到最新配置。
3. 登入控制面变更系统，确认是否存在刚刚发布的配置，记录发布时间、目标服务与滚动批次。

> 若 ReadyState 告警同时触发，应优先排查是否因旧配置导致限流或退避策略异常，以避免扩大影响面。

## 2. 定位排查（T+5 ~ 15 分钟）

| 步骤 | 操作 | 说明 |
| --- | --- | --- |
| A | 检查控制面事件流 | 通过 `spark_config_update_events_total` 或审计日志确认变更是否成功下发，排除控制面阻塞。 |
| B | 复查数据面日志 | 在仍滞后的实例上检索关键字 `config_epoch` / `hot_reload`，确认是否因为解析失败或权限不足导致回滚。 |
| C | 验证配置存储 | 若使用外部配置中心（如 Etcd、Consul），检查对应 Key 是否存在最新版本，并确认 watch 连接正常。 |
| D | 检测网络与负载 | 查看实例与控制面之间的网络延迟/丢包、CPU 利用率，判断是否因资源不足导致热更新延迟。 |
| E | 核对 Pipeline 拓扑 | 若仅部分 Pipeline 落后，使用 `spark_pipeline_registry_snapshot` 指标确认 Handler 是否缺失，排除拓扑差异。 |
| F | 检查数据面日志 | 在落后实例上检索 `INFO pipeline.mutation applied` 日志，确认最近一次热插拔纪元与 `spark_pipeline_epoch` 一致；若日志缺失，优先排查变更是否未提交。 |

## 3. 恢复动作（T+15 ~ 30 分钟）

1. **重试配置下发**：在控制面重新触发发布或对落后实例执行单点刷新，确保最新配置重新广播；刷新后应看到 `spark_pipeline_mutation_total{op="add"}` 累加一次。
2. **清理滞后实例**：若个别副本持续无法更新，考虑逐个摘除并重启，期间需关注流量迁移与容量冗余。重启完成后，`spark_pipeline_epoch` 应重新与控制面对齐。
3. **回滚故障变更**：若确认新配置存在语法/兼容问题导致加载失败，应立即回滚至上一纪元，并记录失败原因。回滚后，观察数据面是否出现 `pipeline.mutation applied epoch=<旧值>` 日志，以及 `spark_pipeline_mutation_total{op="replace"}`、`{op="remove"}` 的对应跳变。
4. **加固监控**：为复现案例补充 `spark_pipeline_epoch` / `config_epoch` 的发布事件日志，并订阅 `pipeline.mutation applied` 事件，便于未来快速定位。

## 4. 恢复验证

- `SparkPipelineEpochDrift` 告警在两个评估周期内恢复为 `inactive`，`clamp_min(config - epoch, 0)` 返回 0。
- Grafana 上 `epoch` 与 `config_epoch` 曲线重合，实例明细面板无持续落后副本。
- 变更系统显示最新发布批次状态为 `Completed`，且所有相关实例健康检查通过。

## 5. 后续跟进

1. 撰写事后分析：记录故障时间线、根因、恢复动作及防范措施，纳入配置变更回顾。
2. 自动化改进：若落后由控制面故障引起，增加针对 `spark_config_update_events_total` 的连续失败告警或发布超时告警。
3. 文档更新：完善配置发布手册与 SDK 指南，确保开发者了解 `config_epoch` 与运行时 `epoch` 的含义及依赖顺序。
