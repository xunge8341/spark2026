// @generated 自动生成文件，请勿手工修改。
// 由 crates/spark-core/build.rs 根据 contracts/error_matrix.toml 生成。

use crate::{
    error::ErrorCategory,
    security::SecurityClass,
    status::{BusyReason, RetryAdvice},
    types::BudgetKind,
};
use core::time::Duration;

/// 默认错误分类矩阵的只读数据源，集中声明“错误码 → ErrorCategory → 自动响应动作”的三段映射。
///
/// # 教案式背景说明（Why）
/// - 构建脚本从 `contracts/error_matrix.toml` 读取声明式数据，统一驱动代码、文档与测试；
/// - 该模块作为框架内的单一事实来源（Single Source of Truth），确保新增错误码时无需手动同步多处文件。
///
/// # 契约定义（What）
/// - 对外暴露 [`entries`]、[`entry_for_code`]、[`default_autoresponse`] 三个只读查询接口；
/// - **输入约束**：调用方必须传入在文档中登记的稳定错误码；
/// - **返回承诺**：表中提供对应的 [`ErrorCategory`] 与默认动作（重试/背压/关闭/取消/无动作）。
///
/// # 实现思路（How）
/// - 使用 [`CategoryMatrixEntry`] 承载错误码与 [`CategoryTemplate`]；
/// - `CategoryTemplate` 负责按需构造 [`ErrorCategory`] 与 [`DefaultAutoResponse`]，避免在静态表中直接存放运行期对象；
/// - 静态常量 `MATRIX` 由构建脚本生成，确保条目顺序稳定；
/// - 通过辅助枚举 [`BusyDisposition`] 与 [`BudgetDisposition`] 描述“繁忙主语义”与“预算类型”，避免跨模块直接依赖内部细节。
///
/// # 风险与权衡（Trade-offs & Gotchas）
/// - 若未来扩展新的自动响应动作，需同步扩展 [`DefaultAutoResponse`] 及默认处理器；
/// - 表项按字母顺序维护，便于审查；测试会确保文档同步，避免遗漏更新。
pub mod matrix {
    use super::{CategoryMatrixEntry, DefaultAutoResponse};

    /// 暴露内部静态矩阵，供调用方遍历但禁止修改。
    pub const fn entries() -> &'static [CategoryMatrixEntry] {
        super::MATRIX
    }

    /// 按错误码查找矩阵条目。
    ///
    /// # 契约说明
    /// - **输入**：遵循 `<域>.<语义>` 规则的稳定错误码；
    /// - **前置条件**：错误码必须事先收录于矩阵中；
    /// - **返回值**：若存在条目返回引用，否则 `None`（表示需显式指定分类）。
    pub fn entry_for_code(code: &str) -> Option<&'static CategoryMatrixEntry> {
        entries().iter().find(|entry| entry.code == code)
    }

    /// 提供默认的自动响应动作，供 Pipeline 或测试驱动行为一致性。
    pub fn default_autoresponse(code: &str) -> Option<DefaultAutoResponse> {
        entry_for_code(code).map(CategoryMatrixEntry::default_response)
    }

    /// 内部使用：根据错误码返回默认 [`ErrorCategory`]。
    pub(crate) fn lookup_default_category(code: &str) -> Option<crate::error::ErrorCategory> {
        entry_for_code(code).map(CategoryMatrixEntry::category)
    }
}

pub use matrix::{default_autoresponse, entries, entry_for_code};

/// 描述单条“错误码 → 分类模板”的映射关系。
///
/// # 字段契约
/// - `code`：稳定错误码，匹配 `docs/error-category-matrix.md` 中的第一列；
/// - `template`：分类模板，封装 `ErrorCategory` 的构造逻辑与默认动作定义。
#[derive(Clone, Copy)]
pub struct CategoryMatrixEntry {
    code: &'static str,
    template: CategoryTemplate,
}

impl CategoryMatrixEntry {
    /// 返回错误码。
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// 根据模板构造默认分类。
    pub fn category(&self) -> ErrorCategory {
        self.template.instantiate()
    }

    /// 计算默认自动响应动作。
    pub fn default_response(&self) -> DefaultAutoResponse {
        self.template.default_response()
    }
}

/// 表达如何从静态表生成 `ErrorCategory` 及自动响应动作的模板。
#[derive(Clone, Copy)]
pub enum CategoryTemplate {
    /// 对应 `Retryable` 分类，携带等待窗口、原因描述与繁忙主语义。
    Retryable {
        wait_ms: u64,
        reason: &'static str,
        busy: Option<BusyDisposition>,
    },
    /// `Timeout` 分类。
    Timeout,
    /// 协议违规。
    ProtocolViolation { close_message: &'static str },
    /// 预算耗尽（区分预算类型）。
    ResourceExhausted { budget: BudgetDisposition },
    /// 运行时取消。
    Cancelled,
    /// 非重试错误。
    NonRetryable,
    /// 安全事件，需要携带具体安全分类以生成关闭文案。
    Security { class: SecurityClass },
}

impl CategoryTemplate {
    /// 按模板实例化 [`ErrorCategory`]。
    pub fn instantiate(&self) -> ErrorCategory {
        match self {
            CategoryTemplate::Retryable {
                wait_ms, reason, ..
            } => {
                let advice =
                    RetryAdvice::after(Duration::from_millis(*wait_ms)).with_reason(*reason);
                ErrorCategory::Retryable(advice)
            }
            CategoryTemplate::Timeout => ErrorCategory::Timeout,
            CategoryTemplate::ProtocolViolation { .. } => ErrorCategory::ProtocolViolation,
            CategoryTemplate::ResourceExhausted { budget } => {
                ErrorCategory::ResourceExhausted(budget.to_budget_kind())
            }
            CategoryTemplate::Cancelled => ErrorCategory::Cancelled,
            CategoryTemplate::NonRetryable => ErrorCategory::NonRetryable,
            CategoryTemplate::Security { class } => ErrorCategory::Security(*class),
        }
    }

    /// 计算默认自动响应动作。
    pub fn default_response(&self) -> DefaultAutoResponse {
        match self {
            CategoryTemplate::Retryable {
                wait_ms,
                reason,
                busy,
            } => DefaultAutoResponse::RetryAfter {
                wait_ms: *wait_ms,
                reason,
                busy: *busy,
            },
            CategoryTemplate::Timeout | CategoryTemplate::Cancelled => DefaultAutoResponse::Cancel,
            CategoryTemplate::ProtocolViolation { close_message } => DefaultAutoResponse::Close {
                reason_code: "protocol.violation",
                message: close_message,
            },
            CategoryTemplate::ResourceExhausted { budget } => {
                DefaultAutoResponse::BudgetExhausted { budget: *budget }
            }
            CategoryTemplate::NonRetryable => DefaultAutoResponse::None,
            CategoryTemplate::Security { class } => DefaultAutoResponse::Close {
                reason_code: "security.violation",
                message: class.summary(),
            },
        }
    }
}

/// 默认自动响应动作的精简表达，用于测试与 Pipeline 复用。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultAutoResponse {
    /// 广播 `Busy`（可选）随后发送 `RetryAfter`。
    RetryAfter {
        wait_ms: u64,
        reason: &'static str,
        busy: Option<BusyDisposition>,
    },
    /// 广播 `BudgetExhausted`。
    BudgetExhausted { budget: BudgetDisposition },
    /// 触发优雅关闭。
    Close {
        reason_code: &'static str,
        message: &'static str,
    },
    /// 标记取消令牌。
    Cancel,
    /// 默认不产生额外动作。
    None,
}

impl DefaultAutoResponse {
    /// 若为背压分支，返回对应的预算类型。
    pub const fn budget(&self) -> Option<BudgetDisposition> {
        match self {
            DefaultAutoResponse::BudgetExhausted { budget } => Some(*budget),
            _ => None,
        }
    }

    /// 若为关闭分支，返回关闭原因元组。
    pub const fn close_reason(&self) -> Option<(&'static str, &'static str)> {
        match self {
            DefaultAutoResponse::Close {
                reason_code,
                message,
            } => Some((*reason_code, *message)),
            _ => None,
        }
    }

    /// 若为重试分支，返回等待窗口与繁忙主语义。
    pub const fn retry(&self) -> Option<(u64, &'static str, Option<BusyDisposition>)> {
        match self {
            DefaultAutoResponse::RetryAfter {
                wait_ms,
                reason,
                busy,
            } => Some((*wait_ms, *reason, *busy)),
            _ => None,
        }
    }
}

/// 描述在退避信号之前广播的繁忙主语义（上游/下游）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusyDisposition {
    Upstream,
    Downstream,
}

impl BusyDisposition {
    /// 转换为 [`BusyReason`]，供 Pipeline 默认处理器复用。
    pub fn to_busy_reason(self) -> BusyReason {
        match self {
            BusyDisposition::Upstream => BusyReason::upstream(),
            BusyDisposition::Downstream => BusyReason::downstream(),
        }
    }
}

/// 描述预算耗尽场景的预算类型，避免在常量表中直接引用 `BudgetKind`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetDisposition {
    Decode,
    Flow,
}

impl BudgetDisposition {
    /// 转换为框架实际使用的 [`BudgetKind`]。
    pub fn to_budget_kind(self) -> BudgetKind {
        match self {
            BudgetDisposition::Decode => BudgetKind::Decode,
            BudgetDisposition::Flow => BudgetKind::Flow,
        }
    }
}

/// 静态矩阵：保持按错误码字典序排列，方便审查与 diff。
const MATRIX: &[CategoryMatrixEntry] = &[
    CategoryMatrixEntry {
        code: crate::error::codes::APP_BACKPRESSURE_APPLIED,
        template: CategoryTemplate::Retryable {
            wait_ms: 180,
            reason: "下游正在施加背压，遵循等待窗口",
            busy: Some(BusyDisposition::Downstream),
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::APP_ROUTING_FAILED,
        template: CategoryTemplate::NonRetryable,
    },
    CategoryMatrixEntry {
        code: crate::error::codes::APP_UNAUTHORIZED,
        template: CategoryTemplate::Security {
            class: SecurityClass::Authorization,
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::CLUSTER_LEADER_LOST,
        template: CategoryTemplate::Retryable {
            wait_ms: 250,
            reason: "集群节点暂不可用，稍后重试",
            busy: Some(BusyDisposition::Upstream),
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::CLUSTER_NETWORK_PARTITION,
        template: CategoryTemplate::Retryable {
            wait_ms: 250,
            reason: "集群节点暂不可用，稍后重试",
            busy: Some(BusyDisposition::Upstream),
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::CLUSTER_NODE_UNAVAILABLE,
        template: CategoryTemplate::Retryable {
            wait_ms: 250,
            reason: "集群节点暂不可用，稍后重试",
            busy: Some(BusyDisposition::Upstream),
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::CLUSTER_QUEUE_OVERFLOW,
        template: CategoryTemplate::ResourceExhausted {
            budget: BudgetDisposition::Flow,
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::CLUSTER_SERVICE_NOT_FOUND,
        template: CategoryTemplate::NonRetryable,
    },
    CategoryMatrixEntry {
        code: crate::error::codes::DISCOVERY_STALE_READ,
        template: CategoryTemplate::Retryable {
            wait_ms: 120,
            reason: "服务发现数据陈旧，等待刷新",
            busy: Some(BusyDisposition::Upstream),
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::PROTOCOL_BUDGET_EXCEEDED,
        template: CategoryTemplate::ResourceExhausted {
            budget: BudgetDisposition::Decode,
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::PROTOCOL_DECODE,
        template: CategoryTemplate::ProtocolViolation {
            close_message: "检测到协议契约违规，已触发优雅关闭",
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::PROTOCOL_NEGOTIATION,
        template: CategoryTemplate::ProtocolViolation {
            close_message: "检测到协议契约违规，已触发优雅关闭",
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::PROTOCOL_TYPE_MISMATCH,
        template: CategoryTemplate::ProtocolViolation {
            close_message: "检测到协议契约违规，已触发优雅关闭",
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::ROUTER_VERSION_CONFLICT,
        template: CategoryTemplate::ProtocolViolation {
            close_message: "检测到协议契约违规，已触发优雅关闭",
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::RUNTIME_SHUTDOWN,
        template: CategoryTemplate::Cancelled,
    },
    CategoryMatrixEntry {
        code: crate::error::codes::TRANSPORT_IO,
        template: CategoryTemplate::Retryable {
            wait_ms: 150,
            reason: "传输层 I/O 故障，等待链路恢复后重试",
            busy: Some(BusyDisposition::Downstream),
        },
    },
    CategoryMatrixEntry {
        code: crate::error::codes::TRANSPORT_TIMEOUT,
        template: CategoryTemplate::Timeout,
    },
];
