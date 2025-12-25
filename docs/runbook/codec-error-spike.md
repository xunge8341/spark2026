# Runbook：SparkCodecErrorSpike（Codec 解码错误飙升）

- **告警来源**：Prometheus 规则 [`SparkCodecErrorSpike`](../observability/alerts.yaml)
- **对应仪表盘**：`codec-and-payload.json`（UID: `spark-codec-payload`）
- **目标恢复时间**：10 分钟内将解码错误率降至 1% 以下
- **升级联系人**：编解码负责人 (@spark-runtime-codec)

## 1. 快速确认（T+0 ~ 5 分钟）

1. 在 Grafana 打开「编解码错误率」面板，确认 `codec_name`、`content_type` 标签与告警保持一致。
2. 检查是否伴随“异常错误类型分布”中 `error_kind` 单一化（如 `version_mismatch`）。
3. 回顾最近上线是否包含协议字段变更、Schema 迁移、Codec 插件升级。

## 2. 定位排查（T+5 ~ 10 分钟）

| 步骤 | 操作 | 说明 |
| --- | --- | --- |
| A | 在「编解码 P95 延迟」面板观察是否同时升高 | 若延迟与错误同时升高，可能为 CPU 饱和或解压缩风暴。 |
| B | 抓取示例 payload | 使用 `kubectl logs` 或 trace 采样定位异常请求，确认是否存在字段缺失/格式错误。 |
| C | 核对 Schema 版本 | 对比当前服务与上下游的 `schema_version` 配置，确认是否一致。 |
| D | 检查降级策略 | 查看是否禁用了“兼容模式”或“回退旧版本”功能。 |

推荐 PromQL：

```promql
sum by (error_kind) (rate(spark_codec_decode_errors{codec_name="$codec", content_type="$type"}[5m]))
```

## 3. 恢复动作

1. **快速回退 Schema/Codec**：将服务配置中的 `codec_version` 回退至上一稳定版本，或启用兼容模式。
2. **过滤异常请求**：通过 API 网关或业务逻辑拒绝格式不合法的请求，减少错误冲击面。
3. **申请限流/灰度**：若由特定客户端触发，可临时封禁对应 `client_id` 或限制速率。
4. **发布热补丁**：针对简单字段映射问题，可热修补解析逻辑（需遵守变更流程）。

## 4. 恢复验证

- `codec-and-payload` 仪表盘中的错误率曲线需降至 1% 以下并保持稳定。
- Prometheus 告警状态转为 `inactive` 后仍需观察 10 分钟，确保无反弹。
- 相关修复、兼容性开关调整必须在值班频道同步，并创建后续任务跟踪永久修复。

## 5. 后续跟进

1. 补充 Schema 校验：在 CI/CD 中加入向后兼容性检查，防止未来重复问题。
2. 增加验收样例：扩展合成负载脚本 `push_synthetic_metrics.py` 的覆盖场景，包含新增字段。
3. 更新文档：若本 Runbook 步骤不足，请提交更新并注明触发案例。
