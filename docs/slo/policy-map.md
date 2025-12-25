# T18｜SLO→策略映射（限流 / 降级 / 熔断 / 重试）

> 关联依赖：T11／T13／T14；本文档面向运行时、混沌工程与运维平台团队，描述如何以表驱动的方式把 SLO 预算映射为机器可执行的策略动作。

## 目标（Why）

- **自治执行**：当 SLO 预算触发违约时，运行时能依据配置表自动执行限流、降级、熔断或重试策略，避免人工介入。
- **可热更新**：策略映射存放在 `slo.policy_table` 配置键下，标记为 `hot_reloadable`，支持在混沌实验期间动态调整。
- **可审计**：每次策略装载都会生成 [`SloPolicyReloadReport`](../../crates/spark-core/src/platform/runtime/slo.rs)，并在 `snapshots/slo-policy-baseline.json` 中提供回归基线。

## 策略映射表（What）

| 预算占比（剩余 / 上限） | 触发级别 | 默认策略动作 | 说明 |
| --- | --- | --- | --- |
| `≤ 40%` | 预警 | `RateLimit { limit_percent }` | 降低入口速率，保留关键链路。
| `≤ 20%` | 严重 | `CircuitBreak { open_for }` | 启动熔断窗口，等待预算恢复。
| `≤ 50%`（解码预算） | 降级 | `Retry { max_attempts, backoff }` | 以退避重试争取预算回收。
| `≥ 恢复阈值` | 恢复 | `Deactivate` | 自动撤销上表策略，恢复正常。

> 上述区间来自默认配置；实际项目可通过配置表自定义更多档位。触发逻辑采用迟滞模型（激活阈值 / 恢复阈值），避免抖动。

## 配置键与热更新（How）

- **配置键**：`slo.policy_table`（[`slo_policy_table_key()`](../../crates/spark-core/src/platform/runtime/slo.rs)）。
- **作用域**：[`ConfigScope::Runtime`](../../crates/spark-core/src/governance/configuration/key.rs)。
- **契约**：顶层与 `rules` 列表的 `ConfigMetadata.hot_reloadable` 必须为 `true`，否则解析会返回 [`SloPolicyConfigError`](../../crates/spark-core/src/platform/runtime/slo.rs)。
- **结构**：
  ```json
  {
    "rules": [
      {
        "id": "flow.rate-limit",
        "budget": "flow",
        "summary": "流量预警限流",
        "trigger": {
          "activate_below_percent": 40,
          "deactivate_above_percent": 60
        },
        "action": {
          "kind": "rate_limit",
          "limit_percent": 60
        }
      }
    ]
  }
  ```

## 运行时绑定

| 对象 | 位置 | 说明 |
| --- | --- | --- |
| [`SloPolicyManager`](../../crates/spark-core/src/platform/runtime/slo.rs) | `spark_core::runtime` | 维护规则表、支持热更新与策略评估。 |
| [`SloPolicyDirective`](../../crates/spark-core/src/platform/runtime/slo.rs) | `spark_core::runtime` | 运行时执行的激活 / 恢复指令，封装策略动作。 |
| [`SloPolicyReloadReport`](../../crates/spark-core/src/platform/runtime/slo.rs) | `spark_core::runtime` | 记录新增 / 移除 / 复用的规则 ID，便于可观测性记账。 |

运行时组件只需在收到 [`BudgetSnapshot`](../../crates/spark-core/src/kernel/contract.rs) 或 [`BudgetDecision`](../../crates/spark-core/src/kernel/contract.rs) 后调用 `SloPolicyManager::evaluate_snapshot`，即可获得需要执行的策略集合。

## 演示样例

```rust
use spark_core::runtime::{
    slo_policy_table_key, SloPolicyManager, SloPolicyAction,
};
use spark_core::configuration::{ConfigMetadata, ConfigValue};
use spark_core::types::{BudgetKind, BudgetSnapshot};
use alloc::{borrow::Cow, vec};

fn meta() -> ConfigMetadata {
    ConfigMetadata { hot_reloadable: true, ..ConfigMetadata::default() }
}

let policy_value = ConfigValue::Dictionary(vec![
    (Cow::Borrowed("rules"), ConfigValue::List(vec![ /* 参照上方 JSON 构造规则 */ ], meta())),
], meta());

let manager = SloPolicyManager::new();
manager.apply_config(&policy_value)?; // 热加载策略

let directives = manager.evaluate_snapshot(&BudgetSnapshot::new(BudgetKind::Flow, 30, 100));
assert!(directives.iter().any(|d| matches!(d.action(), SloPolicyAction::RateLimit { .. }))); // 自动限流
```

> `snapshots/slo-policy-baseline.json` 提供了上述样例的激活 / 恢复序列及热更新报告，作为回归测试基线；详见 `crates/spark-core/tests/slo_policy_map.rs`。

## 验收提示

1. **混沌触发 → 策略执行**：混沌脚本应造成人工 SLO 违约，观察 `SloPolicyManager` 输出的 [`SloPolicyDirective::is_engaged()`](../../crates/spark-core/src/platform/runtime/slo.rs) 与各执行器的响应日志。
2. **自动恢复**：当预算恢复至 `deactivate_above_percent` 以上时，`SloPolicyDirective` 会返回 `engaged = false`，策略自动撤回。
3. **规则热更新**：通过配置中心推送新表，`SloPolicyReloadReport` 中的 `added/removed/retained` 应匹配变更内容；更新后立即重新评估预算状态。

---

> 若扩展新的策略动作，请同步更新本文档、`SloPolicyAction` 注释以及回归基线，确保契约一致。
