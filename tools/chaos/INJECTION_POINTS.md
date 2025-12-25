# 混沌注入点规范

本文档明确 Spark 栈在混沌演练中的标准注入点，保障演练可重复、可追踪。

## 目标

- 统一注入点命名与操作方式；
- 为 SLO 绑定提供稳定的指标来源；
- 支撑 T21 每周演练的回放与报告生成。

## 命名规范

| 层级 | 示例 | 说明 |
| ---- | ---- | ---- |
| `mesh/<service>/<effect>` | `mesh/edge-gateway/latency` | Service Mesh 侧的网络延迟或包丢注入 |
| `k8s/<namespace>/<resource>/<failure>` | `k8s/data/db-primary/termination` | 通过 K8s 控制面触发的 Pod/Deployment 故障 |
| `infra/<component>/<failure>` | `infra/mysql/primary-freeze` | 基础设施层，如数据库、消息队列 |

所有注入点均需映射到唯一的 SLO 名称，写入 `tools/chaos/scenarios/*.json` 的 `expected_slo.name` 字段。

## 操作原则

1. **最小影响**：优先使用限流、延迟等可控参数，必要时通过 `--dry-run` 先验证命令合法性。
2. **显式回滚**：每个注入步骤必须给出回滚命令，确保失败时可快速恢复。
3. **指标可读**：注入前确认关联 SLO 在监控系统中可查询，避免演练时才发现数据缺失。
4. **权限校验**：在正式演练前由平台团队验证执行账户是否具备所需权限（例如 `tc`、`kubectl`、`systemctl` 等）。

## 指标映射建议

| 注入点 | 推荐 SLO | 默认阈值 | 备注 |
| ------ | -------- | -------- | ---- |
| `mesh/*/latency` | `latency_p95` | `<= 250` ms | 关注熔断与告警行为 |
| `k8s/*/termination` | `write_success_rate` | `>= 0.98` | 核心写请求成功率 |
| `infra/mysql/primary-freeze` | `db_replica_lag_seconds` | `<= 5` s | 确认回切后的数据一致性 |

## 记录与回放

- 所有演练须使用 `tools/chaos/chaos_runner.py run <scenario>` 执行，生成的 JSON 记录保存在 `tools/chaos/runs/`。
- 记录文件命名格式：`<UTC 时间>_<场景 ID>_<备注>.json`，备注可填写变更单号或值班人。
- 如需回放，执行 `tools/chaos/chaos_runner.py replay <记录文件>`。

## 风险与例外

- 若注入点涉及生产流量，须提前在 Runbook 中记录业务窗口并获得审批。
- 当 SLO 阈值临时调整时，需同步更新场景 JSON；否则自动校验会误报失败。
- 若监控 API 不稳定，可在演练前通过 `--dry-run --metrics-endpoint <url>` 验证连通性。
