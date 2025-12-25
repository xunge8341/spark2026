//! 时间抽象模块，提供可注入的时钟接口以支撑 RetryAfter/超时/取消等契约在测试中实现完全确定性。
//!
//! # 模块定位（Why）
//! - 将所有依赖真实时间源的组件集中到统一入口，便于通过虚拟时钟在 CI 与模型检查中复现实验；
//! - `Clock` trait 统一 `now` 与 `sleep` 能力，调用方只需依赖 trait 即可在生产环境与测试环境之间平滑切换。
//!
//! # 结构概览（What）
//! - [`clock::Clock`]：核心时钟 trait，暴露 `now`/`sleep` 两个原语；
//! - [`clock::SystemClock`]：基于 Tokio 的生产实现；
//! - [`clock::MockClock`]：高精度虚拟时钟，提供手动推进与确定性唤醒序列。
//!
//! # 使用指引（How）
//! - 业务代码应依赖 [`Clock`] trait 注入时间源；
//! - 在测试中使用 [`MockClock`] 手动推进时间并断言唤醒顺序；
//! - 在生产环境构造 [`SystemClock`] 并通过 `Arc<dyn Clock>` 传递给需要时间能力的组件。

#[cfg(feature = "std")]
pub mod clock;

/// 在 `no_std + alloc` 环境下，`time` 模块仅在启用 `std` Feature 后可用：
/// - **意图 (Why)**：`Clock` 契约以及内建实现需要 `std::time::Instant`、线程与 `Waker` 等能力；
///   在纯 `alloc` 环境缺乏这些原语时避免暴露半功能接口；
/// - **契约 (What)**：启用 `std` Feature 时重新导出 [`clock`] 模块中的 `Clock`/`SystemClock`/
///   `MockClock` 等类型；禁用 `std` 时该导出不可用，调用方应改用 [`runtime::TimeDriver`](crate::runtime::TimeDriver)
///   提供的最小计时抽象；
/// - **风险 (Trade-offs)**：为了保持 API 的显式性，我们未尝试在 `no_std` 下提供降级实现，
///   以免引入依赖缺失却静默退化的隐患。
#[cfg(feature = "std")]
pub use clock::{Clock, MockClock, Sleep, SystemClock};
