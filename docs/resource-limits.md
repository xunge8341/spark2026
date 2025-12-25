# 资源限额与配额治理（T20）

> 目标：为连接 / 内存 / 句柄等关键资源提供统一的限额定义、配置入口与观测指标，满足“达到上限时行为与配置一致、指标能准确反映丢弃/降级比”的验收标准。

## 1. 默认上限与策略一览

| 资源 | 默认上限 | 硬上限 | 默认策略 | 队列容量（如适用） | 说明 |
| --- | --- | --- | --- | --- | --- |
| 连接 (`connections`) | 4096 | 65535 | 排队 (`queue`) | 512 | 面向网络连接的并发控制，默认允许短暂突发并进入排队。 |
| 内存 (`memory_bytes`) | 512 MiB | 32 GiB | 降级 (`degrade`) | - | 达到上限后要求业务切换至轻量响应或缓存读路径。 |
| 句柄 (`file_handles`) | 4096 | 65535 | 拒绝 (`reject`) | - | 为防止触发操作系统 `nofile` 限制，默认直接拒绝新增句柄。 |

> **策略含义**：
> - `reject`：直接返回错误/背压，避免继续消耗资源。
> - `queue`：进入等待队列，若队列溢出将自动转为拒绝。
> - `degrade`：允许继续处理，但需走降级逻辑（例如返回缓存、副本数据）。

## 2. 配置键与取值

| 配置键 | 作用域 | 取值类型 | 说明 |
| --- | --- | --- | --- |
| `runtime::limits.connections.limit` | `runtime` | 整数（≥1） | 覆盖连接上限。 |
| `runtime::limits.connections.action` | `runtime` | 字符串：`reject`/`queue`/`degrade` | 指定连接资源的超限策略。 |
| `runtime::limits.connections.queue_capacity` | `runtime` | 整数（≥1） | 调整排队容量（仅当策略为 `queue` 时生效）。 |
| `runtime::limits.memory_bytes.limit` | `runtime` | 整数（字节） | 覆盖内存上限。 |
| `runtime::limits.memory_bytes.action` | `runtime` | 字符串：同上 | 内存资源的策略。 |
| `runtime::limits.file_handles.limit` | `runtime` | 整数（≥1） | 覆盖句柄上限。 |
| `runtime::limits.file_handles.action` | `runtime` | 字符串：同上 | 句柄资源的策略。 |

### 示例配置（YAML 表达）

```yaml
runtime:
  limits.connections.limit: 8192
  limits.connections.action: queue
  limits.connections.queue_capacity: 1024
  limits.memory_bytes.limit: 1073741824   # 1 GiB
  limits.memory_bytes.action: degrade
  limits.file_handles.action: reject
```

> Builder 在解析上述配置时会校验：
> 1. 数值不为 0 且未超过硬上限；
> 2. 当 action=`queue` 时必须存在有效的 `queue_capacity`（或回退至默认容量）；
> 3. 单独配置 `queue_capacity` 而策略非 `queue` 会触发校验错误。

## 3. 指标契约与监控

新增指标均收录于 `spark-core` 的可观测性契约与 `docs/observability/metrics.md`：

| 指标 | 类型 | 说明 | 关键标签 |
| --- | --- | --- | --- |
| `spark.limits.usage` | Gauge (`units`) | 当前资源占用 | `limit.resource` `limit.action` |
| `spark.limits.limit` | Gauge (`units`) | 配置上限（便于算使用率） | 同上 |
| `spark.limits.hit` | Counter (`events`) | 达到上限的次数 | 同上 |
| `spark.limits.drop` | Counter (`events`) | 触发拒绝的次数 | 同上 |
| `spark.limits.degrade` | Counter (`events`) | 触发降级的次数 | 同上 |
| `spark.limits.queue.depth` | Gauge (`entries`) | 排队策略下的队列深度 | 同上 |

> **丢弃/降级比计算**：`drop_ratio = spark.limits.drop / spark.limits.hit`，`degrade_ratio = spark.limits.degrade / spark.limits.hit`。

## 4. 行为验证（测试用例）

| 用例 | 位置 | 场景 | 期望 |
| --- | --- | --- | --- |
| `default_plan_matches_resource` | `crates/spark-core/src/governance/limits.rs` | 校验默认上限与策略 | 连接默认排队、内存默认降级、句柄默认拒绝。 |
| `evaluate_queue_allows_until_capacity` | 同上 | 队列策略容量处理 | 队列未满时返回 `Queued`，达到容量后拒绝。 |
| `metrics_hook_records_drop_and_usage` | 同上 | 指标打点 | 达到限额时统计 `hit`、`drop` 并更新使用量 Gauge。 |
| `settings_parse_overrides` | 同上 | 配置解析 | YAML/Layer 覆盖 action/limit 后结果正确。 |

> 按照验收要求，在压测中逼近上限即可通过指标与决策日志验证行为是否与配置一致。

