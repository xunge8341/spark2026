# 传输握手协商流程（T22）

## 设计目标
- **版本前向兼容**：通过 `Version { major, minor, patch }` 判断主版本兼容性，在多版本灰度时优雅降级。
- **能力位图协商**：以 `CapabilityBitmap` 建模能力集合，保证新增能力不会破坏旧实现。
- **审计可追溯**：协商成功或失败均写入 `AuditEventV1`，满足 T05/T12 对哈希链的一致性要求。

## 核心结构
| 类型 | 作用 |
| ---- | ---- |
| `Version` | 语义化版本封装，主版本不同即拒绝握手。 |
| `Capability`/`CapabilityBitmap` | 能力位图集合，内置常量涵盖多路复用、压缩、零拷贝等能力，可通过 `Capability::custom` 扩展。 |
| `HandshakeOffer` | 每一方宣告的版本、必选能力、可选能力。 |
| `HandshakeOutcome` | 协商结果，包含启用能力与 `DowngradeReport`。 |
| `NegotiationAuditContext` | 审计上下文，负责生成带哈希链的握手事件。 |
| `negotiate` | 执行协商流程，返回结果或 `HandshakeError`。 |

## 协商顺序
1. **输入准备**：双方分别构造 `HandshakeOffer`，明确必选与可选能力。
2. **可选审计上下文**：若需要记录事件，构建 `NegotiationAuditContext` 并传入 `negotiate`。
3. **版本校验**：`negotiate` 首先比较主版本，不匹配则返回 `HandshakeErrorKind::MajorVersionMismatch`。
4. **能力校验**：验证双方是否满足对方必选能力，缺失会返回对应的 `HandshakeErrorKind`。
5. **降级决策**：取双方可用能力交集生成 `HandshakeOutcome`，并计算 `DowngradeReport`。
6. **审计落盘**：若提供上下文，自动写入成功/失败事件，附带版本、能力位图与降级信息。

## 成功示例：版本兼容 + 部分能力降级
```rust
use alloc::{sync::Arc, vec};
use spark_core::audit::{AuditActor, InMemoryAuditRecorder};
use spark_core::transport::{
    Capability, CapabilityBitmap, HandshakeOffer, NegotiationAuditContext, Version, negotiate,
};

// 准备能力位图
fn bitmap(capabilities: &[Capability]) -> CapabilityBitmap {
    let mut map = CapabilityBitmap::empty();
    for capability in capabilities {
        map.insert(*capability);
    }
    map
}

let local_offer = HandshakeOffer::new(
    Version::new(1, 2, 0),
    bitmap(&[Capability::MULTIPLEXING]),
    bitmap(&[Capability::ZERO_COPY]),
);
let remote_offer = HandshakeOffer::new(
    Version::new(1, 3, 4),
    bitmap(&[Capability::MULTIPLEXING, Capability::COMPRESSION]),
    CapabilityBitmap::empty(),
);

let recorder = Arc::new(InMemoryAuditRecorder::new());
let actor = AuditActor { id: "system".into(), display_name: None, tenant: None };
let mut audit = NegotiationAuditContext::new(recorder.clone(), "transport.connection", "conn-42", actor);

let outcome = negotiate(&local_offer, &remote_offer, 1_730_000_000_000, Some(&mut audit)).unwrap();
assert_eq!(outcome.version(), Version::new(1, 2, 0));
assert!(outcome.downgrade().local().bits() != 0); // 本地零拷贝被降级

let events = recorder.events();
assert_eq!(events.len(), 1);
assert_eq!(events[0].action, "transport.handshake.succeeded");
```
> **说明**：双方主版本一致，协商出的版本为较小值 `1.2.0`。本地的零拷贝能力因对端未声明而降级，事件被写入审计链。

## 失败示例：主版本冲突
```rust
use spark_core::transport::{CapabilityBitmap, HandshakeOffer, Version, negotiate};

let local = HandshakeOffer::new(
    Version::new(2, 0, 0),
    CapabilityBitmap::empty(),
    CapabilityBitmap::empty(),
);
let remote = HandshakeOffer::new(
    Version::new(1, 9, 3),
    CapabilityBitmap::empty(),
    CapabilityBitmap::empty(),
);

let error = negotiate(&local, &remote, 1_730_000_000_000, None).unwrap_err();
match error.kind() {
    spark_core::transport::HandshakeErrorKind::MajorVersionMismatch { .. } => {}
    other => panic!("unexpected error: {other:?}"),
}
```
> **说明**：主版本不同直接失败，未触发审计（因为未提供 `NegotiationAuditContext`）。

## 审计字段说明
| 字段 | 含义 |
| ---- | ---- |
| `handshake.negotiated_version` | 协商出的最终版本。 |
| `handshake.capabilities.enabled` | 最终启用能力位图（十六进制字符串）。 |
| `handshake.capabilities.local_downgraded` | 本地被降级的可选能力位图。 |
| `handshake.capabilities.remote_downgraded` | 对端被降级的可选能力位图。 |
| `handshake.peer_version` / `handshake.local_version` | 双方宣告版本。 |
| `handshake.failure.reason` | 仅失败事件携带，包含 `HandshakeError` 描述。 |

## 最佳实践
- 在服务端与客户端同时采用 `NegotiationAuditContext`，可实现端到端的协商追踪。
- 新增能力时，仅需为新索引分配唯一编号并更新可选位图，旧版本会自动降级。
- 若业务需要“强制升级”，可在 `DowngradeReport::is_lossless()` 为 `false` 时拒绝继续建立通道。
