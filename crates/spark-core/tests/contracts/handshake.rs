//! 传输握手协商契约测试，覆盖版本兼容与能力降级路径。

use super::support::*;
use spark_core::transport::{
    Capability, CapabilityBitmap, HandshakeErrorKind, HandshakeOffer, Version, negotiate,
};

fn bitmap(capabilities: &[Capability]) -> CapabilityBitmap {
    let mut bits = CapabilityBitmap::empty();
    for capability in capabilities {
        bits.insert(*capability);
    }
    bits
}

#[test]
fn negotiate_success_with_downgrade() {
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

    let outcome = negotiate(&local, &remote).expect("handshake succeeds");
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

    let error = negotiate(&local, &remote).expect_err("major mismatch");
    assert!(matches!(error.kind(), HandshakeErrorKind::MajorVersionMismatch { .. }));
}

#[test]
fn fail_when_remote_missing_local_requirements() {
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

    let error = negotiate(&local, &remote).expect_err("missing capability");
    match error.kind() {
        HandshakeErrorKind::RemoteLacksLocalRequirements { missing } => {
            assert_eq!(missing.bits(), bitmap(&[Capability::COMPRESSION]).bits());
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
