//! 内部 sealed 模块用于控制外部扩展边界。
//!
//! # 设计背景（Why）
//! - `spark-core` 向外暴露大量可实现的 Trait，需要在 SemVer 框架下保持未来演进空间。
//! - 通过统一的 `Sealed` 标记，我们能够在不破坏公开 API 的情况下，为 Trait 增加默认方法或强化约束。
//!
//! # 逻辑解析（How）
//! - 定义私有模块级 Trait `Sealed`，并对所有类型提供 blanket 实现。
//! - 对外可实现的 Trait 通过 `: crate::sealed::Sealed` 间接依赖该标记，从而确保调用方无法绕过框架的演化控制。
//! - 若未来需要限制实现者集合，可在此处收紧 blanket 实现条件，而无需修改公开 Trait 的签名。
//!
//! # 契约说明（What）
//! - `Sealed` 无需调用方显式实现；任意类型默认满足该约束。
//! - 公共 Trait 要求的所有前置条件、返回契约仍在各自定义处描述，此模块仅负责封装“实现许可”。
//!
//! # 风险与考量（Trade-offs）
//! - Blanket 实现意味着当前不会限制实现者；这是为了兼容现有插件生态。
//! - 如果未来收紧条件，需要同步发布兼容性公告，并提供迁移指南以避免破坏性升级。
//!
//! # 维护提示（Maintenance）
//! - 修改该模块前需评估所有依赖 `crate::sealed::Sealed` 的 Trait；建议配合 `cargo semver-checks` 验证影响。
pub(crate) trait Sealed {}

impl<T: ?Sized> Sealed for T {}
