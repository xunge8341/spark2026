//! 安全分类辅助实现：在治理语境下为 [`SecurityClass`] 补充编码与摘要。
//!
//! # 模块意图（Why）
//! - 将“分类定义”与“治理侧元数据”解耦：`security.rs` 负责最小语义，本模块负责治理需要的编码与提示文案；
//! - 为错误矩阵、关闭策略等治理设施提供统一的安全分类映射，避免在上层重复硬编码。
//!
//! # 契约说明（What）
//! - 输入：来自机制层的 [`SecurityClass`] 枚举；
//! - 输出：稳定的机器可读编码（`code`）与人类可读摘要（`summary`）；
//! - 前置条件：调用方需选择正确的分类，否则治理策略会偏离实际风险。
//!
//! # 实现要点（How）
//! - 使用 `match` 返回静态字符串，保证 `no_std + alloc` 环境零分配；
//! - 方法定义在治理层，便于未来根据治理策略扩展（例如增加本地化、审计标签）。
//!
//! # 风险提示（Trade-offs & Gotchas）
//! - 扩展新分类时必须同步更新错误矩阵生成脚本与测试；
//! - `code` 和 `summary` 均为常量字符串，若需动态本地化需在调用方处理。

use crate::security::SecurityClass;

impl SecurityClass {
    /// 返回分类对应的稳定代码，供日志、指标与自动化治理使用。
    ///
    /// ## 参数与前置条件（What）
    /// - `self`：安全分类枚举值，不接受其他外部输入；
    ///
    /// ## 行为描述（How）
    /// - 按分支返回固定的命名空间字符串（`security.<area>`），确保跨系统可比对；
    /// - 采用 `const fn` 以便在编译期构建静态表或匹配字面量。
    ///
    /// ## 后置条件（What）
    /// - 返回值可安全用于指标标签、错误分类或关闭原因；
    /// - 若调用方误用（例如不匹配实际风险），治理策略将可能误触或漏报。
    pub const fn code(self) -> &'static str {
        match self {
            SecurityClass::Authentication => "security.authentication",
            SecurityClass::Authorization => "security.authorization",
            SecurityClass::Confidentiality => "security.confidentiality",
            SecurityClass::Integrity => "security.integrity",
            SecurityClass::Audit => "security.audit",
            SecurityClass::Unknown => "security.unknown",
        }
    }

    /// 返回人类可读摘要，用于关闭原因、告警或审计提示。
    ///
    /// ## 参数与前置条件（What）
    /// - `self`：安全分类枚举值，无需额外上下文；
    ///
    /// ## 行为描述（How）
    /// - 直接匹配枚举分支，输出固定中文描述，避免运行时分配；
    /// - 与 [`Display`](core::fmt::Display) 输出保持一致，方便调用方在日志与 UI 复用文案。
    ///
    /// ## 后置条件（What）
    /// - 成功返回摘要字符串；
    /// - 若后续需要本地化或多语言支持，请在调用侧包装翻译表，保持此处零依赖。
    pub const fn summary(self) -> &'static str {
        match self {
            SecurityClass::Authentication => "身份验证失败，需重新认证",
            SecurityClass::Authorization => "权限不足或策略拒绝",
            SecurityClass::Confidentiality => "检测到保密性风险，需加密或隔离",
            SecurityClass::Integrity => "数据完整性校验失败",
            SecurityClass::Audit => "审计或合规策略触发告警",
            SecurityClass::Unknown => "未归类的安全事件，建议人工复核",
        }
    }
}
