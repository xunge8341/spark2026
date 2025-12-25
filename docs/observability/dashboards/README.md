# Spark 样例 Grafana 仪表盘使用说明

本目录提供 **3 份开箱即用的 Grafana JSON**，覆盖 Service、Codec、Transport 三个域。配套 `docs/observability/alerts.yaml` 与 `docs/runbook/`，可在 5 分钟内启动自检环境并完成三类典型告警的演练。

## 1. 仪表盘一览

| 文件 | 仪表盘 UID | 覆盖场景 | 推荐搭配告警 |
| --- | --- | --- | --- |
| `core-runtime.json` | `spark-core-runtime` | 服务吞吐、成功率、P99 延迟、字节量、并发数 | `SparkServiceErrorRateHigh` |
| `codec-and-payload.json` | `spark-codec-payload` | 编解码 P95、错误率、载荷分布、异常类型 | `SparkCodecErrorSpike` |
| `ready-state-retry-after.json` | `spark-ready-state-retry-after` | ReadyState 占比、RetryAfter 次数与退避分布 | `SparkServiceRetryAfterBurst` |
| `transport-health.json` | `spark-transport-health` | 通道数量、握手耗时、吞吐、失败类型 | `SparkTransportChannelFailures` |

> **命名规范提示**：所有面板引用的指标名称与标签与《[指标契约 v1.0](../metrics.md)》保持一致，可直接用于验收和基准对比。

## 2. 五分钟快速启动

1. **准备环境**：安装 Docker Compose ≥ 2.5，并确保本地 `3000`、`9090` 端口可用。
2. **启动基础设施**：
   ```bash
   docker compose -f docs/observability/dashboards/sample-stack.yaml up -d
   ```
   - Grafana 默认监听 `http://localhost:3000`，账户 `admin`/`admin`。
   - Prometheus 监听 `http://localhost:9090`，预置了 Spark 指标的合成采集任务。
3. **导入仪表盘**：登录 Grafana → **Dashboards → Import** → 依次粘贴 JSON 文件内容 → 选择数据源 `Prometheus`。
4. **生成演示流量**：
   ```bash
   ./docs/observability/dashboards/scripts/push_synthetic_metrics.sh
   ```
   脚本会持续向 `pushgateway:9091` 写入 `spark_request_*`、`spark_codec_*`、`spark_transport_*` 指标，Grafana 面板将在 1 分钟内出现曲线。
5. **触发告警演练**：
   - 调整脚本中的 `ERROR_RATIO` 或 `FAILURE_RATE`，等待 Prometheus 评估周期（默认 ≤2 分钟）。
   - Grafana Alerting 与 Prometheus `alerts` 页面均可验证告警触发与恢复。

> **环境清理**：演练完成后执行 `docker compose -f docs/observability/dashboards/sample-stack.yaml down -v`。

## 3. 目录结构

```
.
├── README.md                      # 本说明
├── codec-and-payload.json         # 编解码领域仪表盘
├── core-runtime.json              # 服务调用链仪表盘
├── transport-health.json          # 传输层仪表盘
├── sample-stack.yaml              # Grafana + Prometheus + Pushgateway 样例编排
└── scripts
    └── push_synthetic_metrics.sh  # 生成示例指标，满足 5 分钟出图要求
```

## 4. 与 Runbook 联动

- 告警触发 → 查询对应仪表盘 → 参考 `docs/runbook/` 中的处置步骤。
- Runbook 对应的指标、关键标签、排查命令均在仪表盘 tooltip 中给出，避免信息割裂。

> **建议**：将仪表盘与告警的链接写入 On-call 值班文档中，保障交接时可直接点击跳转。
