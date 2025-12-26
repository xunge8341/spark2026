//! 安全分类（SecurityClass）机制层定义：作为 Core 处理安全相关错误的最小语义入口。
//!
//! # 模块意图（Why）
//! - 将安全分类上移至机制层，确保核心错误契约不依赖治理实现细节；
//! - 仅暴露与错误语义直接相关的枚举与展示逻辑，避免引入策略、身份等治理概念。
//!
//! # 契约与职责（What）
//! - 提供 `SecurityClass` 枚举，用于描述安全事件所属的大类；
//! - 实现 `Display` 以输出面向运维/审计的可读描述；
//! - 枚举标记为 `#[non_exhaustive]`，为未来扩展（如供应链安全、运行时沙箱）预留空间。
//!
//! # 设计要点（How）
//! - 纯数据定义，不引用治理层代码；
//! - 采用 `&'static str` 字面量，确保在 `no_std + alloc` 环境下零分配；
//! - 显式实现 `Display`，便于错误分类与关闭原因直接打印。
//!
//! # 风险与权衡（Trade-offs）
//! - 仅提供展示语义，不包含策略判定或策略映射逻辑；需要策略决策时应转向治理层能力；
//! - 扩展枚举时需同步更新生成脚本与错误矩阵，否则可能出现分类缺失的风险。

use core::fmt::{self, Display, Formatter};

/// 安全事件分类枚举，聚焦描述“安全违规”的主语义。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SecurityClass {
    /// 认证失败，例如凭证过期、身份不存在。
    Authentication,
    /// 授权失败，例如策略拒绝或越权访问。
    Authorization,
    /// 保密性威胁，例如明文传输敏感数据。
    Confidentiality,
    /// 完整性校验失败，例如消息被篡改或校验和不符。
    Integrity,
    /// 审计或合规性异常，例如违规操作或缺失审计轨迹。
    Audit,
    /// 未归类的安全事件。
    Unknown,
}

impl Display for SecurityClass {
    /// 展示面向人的安全分类摘要，供日志、告警与关闭原因使用。
    ///
    /// ## 输入/前置条件（What）
    /// - `self`：当前的安全分类枚举值；无需额外上下文。
    ///
    /// ## 行为说明（How）
    /// - 按枚举分支返回固定中文描述，避免运行时分配；
    /// - 通过匹配语义覆盖认证、授权、保密、完整性与审计等常见安全场景。
    ///
    /// ## 后置条件（What）
    /// - 成功时向格式化缓冲输出对应摘要；
    /// - 若写入失败（例如底层 writer 出错），按 `fmt::Result` 约定返回错误。
    ///
    /// ## 设计考量（Trade-offs & Gotchas）
    /// - 仅提供人类可读描述，不返回机器可解析编码；
    /// - 若未来需要本地化或多语种支持，可在调用侧包装翻译表，保持此处零依赖、零分配。
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let summary = match self {
            SecurityClass::Authentication => "身份验证失败，需重新认证",
            SecurityClass::Authorization => "权限不足或策略拒绝",
            SecurityClass::Confidentiality => "检测到保密性风险，需加密或隔离",
            SecurityClass::Integrity => "数据完整性校验失败",
            SecurityClass::Audit => "审计或合规策略触发告警",
            SecurityClass::Unknown => "未归类的安全事件，建议人工复核",
        };

        f.write_str(summary)
    }
}
