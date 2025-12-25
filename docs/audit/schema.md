# 审计事件 Schema v1.0

> 版本：v1.0（对齐 `spark-core` 0.1.0）

## 设计背景

- **不可篡改**：所有事件通过 `state_prev_hash` → `state_curr_hash` 构建哈希链，任何遗漏都会在下一条事件上暴露。
- **最小字段集**：围绕“谁在何时对哪类资源做了什么改动”抽象最小必要字段，可在后续版本向后兼容地扩展。
- **可回放**：事件内携带脱敏后的配置差异（`changes`），配合回放工具即可复现样例资源。

## 字段说明

| 字段 | 类型 | 说明 |
| ---- | ---- | ---- |
| `event_id` | `String` | 事件唯一 ID，推荐 `profile:sequence:new_version_hash`。 |
| `sequence` | `u64` | 数据源自增序号。 |
| `entity.kind` | `String` | 实体类型，示例：`configuration.profile`。 |
| `entity.id` | `String` | 实体唯一标识，例如 ProfileId。 |
| `entity.labels` | `Vec<(String, String)>` | 可选扩展标签（租户、区域等）。 |
| `action` | `String` | 动作名称，v1 固定为 `apply_changeset`，后续可扩展。 |
| `state_prev_hash` | `String` | 变更前快照哈希（SHA-256 Hex）。 |
| `state_curr_hash` | `String` | 变更后快照哈希。 |
| `actor.id` | `String` | 操作者标识（IAM 用户、服务账号等）。 |
| `actor.display_name` | `Option<String>` | 人类可读名称。 |
| `actor.tenant` | `Option<String>` | 多租户上下文，可选。 |
| `occurred_at` | `u64` | Unix 毫秒时间戳。 |
| `tsa_evidence` | `Option<{ provider, evidence, issued_at }>` | TSA 可信时间锚点，可选。 |
| `changes.created/updated` | `Vec<{ key, value }>` | 新增/更新项的完整配置（包含元数据）。 |
| `changes.deleted` | `Vec<{ key }>` | 被删除的配置键。 |

> 字段在 `spark_core::audit::AuditEventV1` 中有一一对应的实现，详见源码注释。【F:crates/spark-core/src/governance/audit/mod.rs†L47-L164】

## 事件样例

```json
{
  "event_id": "test:2:5c497a6c",
  "sequence": 2,
  "entity": {
    "kind": "configuration.profile",
    "id": "test",
    "labels": []
  },
  "action": "apply_changeset",
  "state_prev_hash": "a4d4c88e6b3d0c9f...",
  "state_curr_hash": "5c497a6c2cc83d51...",
  "actor": {
    "id": "system",
    "display_name": "System",
    "tenant": null
  },
  "occurred_at": 1700000000000,
  "tsa_evidence": null,
  "changes": {
    "created": [],
    "updated": [
      {
        "key": {
          "domain": "demo",
          "name": "message",
          "scope": "global",
          "summary": "demo message"
        },
        "value": {
          "kind": "text",
          "value": "world",
          "metadata": {
            "hot_reloadable": false,
            "encrypted": false,
            "experimental": false,
            "tags": []
          }
        }
      }
    ],
    "deleted": []
  }
}
```

## 事件产生流程

1. `ConfigurationBuilder::with_audit_pipeline` 注入 `AuditPipeline`，提供 `AuditRecorder` 与上下文（实体类型、操作者等）。【F:crates/spark-core/src/governance/configuration/builder.rs†L566-L586】
2. `LayeredConfiguration::apply_change` 在应用增量前计算 `state_prev_hash`，并验证是否与 `audit_chain_tip` 一致，若不一致直接返回 `ConfigurationErrorKind::Conflict`。【F:crates/spark-core/src/governance/configuration/builder.rs†L905-L949】
3. 生成 `AuditChangeSet`，写入 Recorder；Recorder 失败时自动回滚 Layer 与版本号并返回 `ConfigurationErrorKind::Audit`。【F:crates/spark-core/src/governance/configuration/builder.rs†L951-L992】
4. 成功写入后更新链尾哈希并广播变更，确保审计与实际状态同步前进。 

## 回放与重发策略

- **回放**：使用 `audit_replay` 工具读取事件日志并重建配置快照，支持指定初始基线、输出最终状态。【F:crates/spark-core/src/bin/audit_replay.rs†L1-L157】
- **哈希断裂检测**：若 `state_prev_hash` 与当前状态不符，工具会立即终止并提示断点行，同时可通过 `--gap-report` 生成补发请求 JSON，供上游重新推送缺失事件。
- **自动重试建议**：当框架侧 Recorder 失败时，`apply_change` 返回 `ConfigurationErrorKind::Audit`，上游可捕获后触发“全量重载 + 重新发事件”策略。

## 与 Observability 契约的衔接

`DEFAULT_OBSERVABILITY_CONTRACT` 已同步升级审计字段列表，方便日志/指标侧按键采集事件元数据。【F:crates/spark-core/src/kernel/contract.rs†L493-L507】

## 附录：基线文件格式

回放工具基线文件采用 `AuditChangeEntry` 数组，示例：

```json
[
  {
    "key": {
      "domain": "demo",
      "name": "message",
      "scope": "global",
      "summary": "demo message"
    },
    "value": {
      "kind": "text",
      "value": "hello",
      "metadata": {
        "hot_reloadable": false,
        "encrypted": false,
        "experimental": false,
        "tags": []
      }
    }
  }
]
```

> 若基线为空，可省略 `--baseline` 参数，工具会从空状态开始累积事件。
