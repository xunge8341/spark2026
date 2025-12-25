//! 传输握手协商契约测试，覆盖版本兼容、能力降级与审计写入。

use super::support::*;
use spark_core::audit::{AuditActor, AuditChangeEntry, AuditRecorder, InMemoryAuditRecorder};
use std::sync::Arc;
use spark_core::configuration::ConfigValue;
use spark_core::transport::{
    Capability, CapabilityBitmap, HandshakeErrorKind, HandshakeOffer, NegotiationAuditContext,
    Version, negotiate,
};

fn bitmap(capabilities: &[Capability]) -> CapabilityBitmap {
    let mut bits = CapabilityBitmap::empty();
    for capability in capabilities {
        bits.insert(*capability);
    }
    bits
}

fn find_change<'a>(changes: &'a [AuditChangeEntry], name: &str) -> Option<&'a AuditChangeEntry> {
    changes
        .iter()
        .find(|entry| entry.key.name() == name && entry.key.domain() == "transport")
}

#[test]
fn negotiate_success_with_audit_and_downgrade() {
    let local = HandshakeOffer::new(
        Version::new(1, 4, 0),
        bitmap(&[Capability::MULTIPLEXING]),
        bitmap(&[Capability::ZERO_COPY]),
    );
    let remote = HandshakeOffer::new(
        Version::new(1, 2, 3),
        bitmap(&[Capability::MULTIPLEXING]),
        bitmap(&[Capability::COMPRESSION]),
    );

    let recorder: Arc<InMemoryAuditRecorder> = Arc::new(InMemoryAuditRecorder::new());
    let actor = AuditActor { id: "system".into(), display_name: None, tenant: None };
    let mut audit = NegotiationAuditContext::new(
        Arc::clone(&recorder) as Arc<dyn AuditRecorder>,
        "transport.connection",
        "conn-success",
        actor,
    );

    let outcome = negotiate(&local, &remote, 1_730_000_123_000, Some(&mut audit)).expect("handshake succeeds");
    assert_eq!(outcome.version(), Version::new(1, 2, 3));
    assert_eq!(outcome.capabilities().bits(), bitmap(&[Capability::MULTIPLEXING]).bits());
    assert_eq!(
        outcome.downgrade().local().bits(),
        bitmap(&[Capability::ZERO_COPY]).bits()
    );
    assert_eq!(
        outcome.downgrade().remote().bits(),
        bitmap(&[Capability::COMPRESSION]).bits()
    );

    let events = recorder.events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.action, "transport.handshake.succeeded");
    let update = &event.changes.updated;
    let local_downgrade = find_change(update, "handshake.capabilities.local_downgraded").unwrap();
    match &local_downgrade.value {
        ConfigValue::Text(value, _) => assert!(value.contains("0x")),
        other => panic!("unexpected value: {other:?}"),
    }
    let enabled = find_change(update, "handshake.capabilities.enabled").unwrap();
    match &enabled.value {
        ConfigValue::Text(value, _) => assert!(value.contains("0001")),
        other => panic!("unexpected value: {other:?}"),
    }
}

#[test]
fn fail_on_major_version_mismatch() {
    let local = HandshakeOffer::new(
        Version::new(2, 0, 0),
        CapabilityBitmap::empty(),
        CapabilityBitmap::empty(),
    );
    let remote = HandshakeOffer::new(
        Version::new(1, 9, 9),
        CapabilityBitmap::empty(),
        CapabilityBitmap::empty(),
    );

    let error = negotiate(&local, &remote, 1_730_000_000_000, None).expect_err("major mismatch");
    assert!(matches!(error.kind(), HandshakeErrorKind::MajorVersionMismatch { .. }));
}

#[test]
fn fail_when_remote_missing_local_requirements_records_audit() {
    let local = HandshakeOffer::new(
        Version::new(1, 1, 0),
        bitmap(&[Capability::MULTIPLEXING, Capability::COMPRESSION]),
        CapabilityBitmap::empty(),
    );
    let remote = HandshakeOffer::new(
        Version::new(1, 1, 5),
        bitmap(&[Capability::MULTIPLEXING]),
        CapabilityBitmap::empty(),
    );

    let recorder: Arc<InMemoryAuditRecorder> = Arc::new(InMemoryAuditRecorder::new());
    let actor = AuditActor { id: "system".into(), display_name: None, tenant: None };
    let mut audit = NegotiationAuditContext::new(
        Arc::clone(&recorder) as Arc<dyn AuditRecorder>,
        "transport.connection",
        "conn-failure",
        actor,
    );

    let error = negotiate(&local, &remote, 1_730_000_000_001, Some(&mut audit)).expect_err("missing capability");
    match error.kind() {
        HandshakeErrorKind::RemoteLacksLocalRequirements { missing } => {
            assert_eq!(missing.bits(), bitmap(&[Capability::COMPRESSION]).bits());
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let events = recorder.events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.action, "transport.handshake.failed");
    let reason = find_change(&event.changes.updated, "handshake.failure.reason").unwrap();
    match &reason.value {
        ConfigValue::Text(value, _) => {
            assert!(value.contains("remote lacks"));
        }
        other => panic!("unexpected value: {other:?}"),
    }
    assert_eq!(event.state_prev_hash, event.state_curr_hash);
}
