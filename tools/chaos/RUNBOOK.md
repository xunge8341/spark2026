# T21 混沌演练 Runbook

## 演练节奏

- **频率**：每周至少一次，建议安排在工作日低峰时段。
- **参与角色**：
  - 值班工程师：执行脚本、监控指标、撰写报告；
  - 业务代表：确认业务侧异常是否可接受；
  - 平台 SRE：负责回滚与工具维护。

## 演练前检查清单

1. 通过 `make ci-lints` 确认最新工具链无静态检查异常。
2. 使用 `tools/chaos/chaos_runner.py list` 确认场景状态，并检查是否有新的 JSON 更新。
3. 登录监控系统，确认与本次演练相关的 SLO 查询语句有数据返回。
4. 预留至少 30 分钟窗口，并通知业务方。
5. 若为生产环境，完成审批单并在 `--note` 中记录单号。

## 执行步骤

1. Dry-run 验证：

   ```bash
   tools/chaos/chaos_runner.py run <scenario_id> --dry-run --note "change-1234"
   ```

2. 正式执行（示例：Service Mesh 延迟注入）：

   ```bash
   tools/chaos/chaos_runner.py run network_latency --metrics-endpoint "https://monitoring.example.com/api/v1/slo" --note "change-1234"
   ```

3. 监控告警：确认关键 SLO（如 `latency_p95`、`write_success_rate`）未超过阈值；若超过，应在 2 分钟内自动告警。
4. 演练结束后执行回滚命令（通常为场景 JSON 中 `rollback` 字段），并确认服务恢复稳态。
5. 通过 `tools/chaos/chaos_runner.py replay <记录文件>` 生成演练摘要。

## 报告模板

| 项目 | 内容 |
| ---- | ---- |
| 场景 | `network_latency` |
| 演练时间 | `2024-05-01 10:00 ~ 10:30` |
| 负责人 | `alice` |
| SLO 状态 | `latency_p95 <= 250ms ✅` |
| 告警 | `Grafana 告警在 1m 内触发并自动关闭` |
| 异常处理 | `无` |
| 改进项 | `补充 Mesh 延迟监控 Dashboard` |

## 故障处理

- **SLO 超阈**：立即停止演练，执行回滚；将状态更新至事故群并触发事后复盘。
- **命令执行失败**：根据 JSON 中的 `rollback` 手动恢复，并在报告中记录失败原因。
- **监控不可用**：改为人工验证，并在报告中标注“手动确认”。同时创建故障单跟踪监控系统问题。

## 持续改进

- 每次演练后需在周会同步结果，并根据演练记录更新场景脚本或 SLO 阈值。
- 建议将 `tools/chaos/runs/` 目录同步至集中存储（如 S3、OSS），便于长期追踪。
- 对新增业务或基础设施组件，需在上线前补充对应的混沌场景并完成至少一次演练。
