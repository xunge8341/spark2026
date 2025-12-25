//! 集成测试：确保 `human()` / `hint()` 行为与文档对齐。
//!
//! ## 设计意图（Why）
//! - 框架 DoD 要求“新人 30 分钟内按 hint() 修复样例”，因此必须锁定常见错误码的提示文案。
//! - 测试覆盖核心错误、领域错误、分布式错误三类入口，防止后续重构时遗漏委托链。
//!
//! ## 测试策略（How）
//! - 针对命中表项与未命中表项分别断言返回值，验证回退逻辑。
//! - 构造域层错误验证委托关系，确保无额外分配或语义偏差。
//!
//! ## 预置条件（What）
//! - 所有错误码常量来自 `spark_core::error::codes`，确保 `'static` 生命周期。
//! - 集成测试在默认 `alloc` 特性下运行，无需启用额外 feature。

use spark_core::{CoreError, DomainError, DomainErrorKind, SparkError, error::codes};

/// 命中 Top10 表项时应返回静态文案，确保新人可直接复述原因与操作步骤。
#[test]
fn errors_human_hint_known_code() {
    // 构造核心错误：选取稳定码 `transport.timeout` 代表网络超时场景。
    let core_error = CoreError::new(codes::TRANSPORT_TIMEOUT, "low-level timeout detail");

    // human() 应返回静态摘要，hint() 返回详细操作指引。
    assert_eq!(core_error.human(), "传输层超时：请求在约定时限内未获得响应");
    assert_eq!(
        core_error.hint().as_deref(),
        Some("确认服务端是否过载或限流；若系统正常请调高超时阈值并排查链路拥塞")
    );

    // SparkError 和 DomainError 均应复用核心实现，确保跨层语义一致。
    let spark_error = SparkError::new(codes::TRANSPORT_TIMEOUT, "low-level timeout detail");
    assert_eq!(
        spark_error.human(),
        "传输层超时：请求在约定时限内未获得响应"
    );
    assert_eq!(
        spark_error.hint().as_deref(),
        Some("确认服务端是否过载或限流；若系统正常请调高超时阈值并排查链路拥塞")
    );

    let domain_error = DomainError::new(DomainErrorKind::Transport, core_error);
    assert_eq!(
        domain_error.human(),
        "传输层超时：请求在约定时限内未获得响应"
    );
    assert_eq!(
        domain_error.hint().as_deref(),
        Some("确认服务端是否过载或限流；若系统正常请调高超时阈值并排查链路拥塞")
    );
}

/// 未备案的错误码应回退到 message()，同时不提供 hint()，确保调用方感知待补充文档。
#[test]
fn errors_human_hint_unknown_code() {
    // 使用未在 Top10 表中的占位错误码。
    let core_error = CoreError::new("custom.experimental", "raw implementation detail");

    // human() 回退到原始消息，hint() 为 None。
    assert_eq!(core_error.human(), "raw implementation detail");
    assert!(core_error.hint().is_none());
}
