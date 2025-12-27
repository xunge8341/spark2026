use std::{env, fmt::Write, fs, path::PathBuf};

#[path = "../../tools/error_matrix_contract.rs"]
mod error_matrix_contract;
use error_matrix_contract::{
    BudgetDispositionSpec, BusyDispositionSpec, CategoryTemplateSpec, ExpandedEntry,
    SecurityClassSpec, expand_entries, read_error_matrix_contract,
};

/// 构建脚本入口：仅保留错误矩阵生成，切断对治理文档/代码生成的依赖。
///
/// # 教案式说明（Why）
/// - 通过生成 `category_matrix.rs` 统一错误分类与默认自动响应，避免手写表导致漂移；
/// - 去除治理侧文档生成，确保构建路径仅聚焦核心契约。
///
/// # 契约定义（What）
/// - 输入：`contracts/error_matrix.toml`；
/// - 输出：`crates/spark-core/src/error/generated/category_matrix.rs`；
/// - 前置条件：合约文件存在且字段满足解析约束；
/// - 后置条件：生成文件覆盖旧版本，保持与合约同步。
fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let contract_path = manifest_dir.join("../../contracts/error_matrix.toml");
    println!("cargo:rerun-if-changed={}", contract_path.display());

    let contract = read_error_matrix_contract(&contract_path);
    let entries = expand_entries(&contract);
    let generated = render_category_matrix(&entries);

    let generated_dir = manifest_dir.join("src/error/generated");
    fs::create_dir_all(&generated_dir).expect("创建 error/generated 目录");
    let output_path = generated_dir.join("category_matrix.rs");
    fs::write(&output_path, generated).expect("写入 generated/category_matrix.rs");
}

/// 渲染错误分类矩阵源码，保持与当前 `ErrorCategory`/`RetryAdvice` 语义一致。
fn render_category_matrix(entries: &[ExpandedEntry]) -> String {
    let mut buffer = String::new();
    buffer.push_str("// @generated 自动生成文件，请勿手工修改.\n");
    buffer.push_str("// 由 crates/spark-core/build.rs 根据 contracts/error_matrix.toml 生成。\n\n");
    buffer.push_str("use crate::{\n");
    buffer.push_str("    error::ErrorCategory,\n");
    buffer.push_str("    security::SecurityClass,\n");
    buffer.push_str("    status::{BusyReason, RetryAdvice},\n");
    buffer.push_str("    types::BudgetKind,\n");
    buffer.push_str("};\n");
    buffer.push_str("use core::time::Duration;\n\n");
    buffer.push_str(
        r#"/// 默认错误分类矩阵的只读数据源，集中声明“错误码 → ErrorCategory → 自动响应动作”的三段映射。
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
"#,
    );
    buffer.push_str("pub mod matrix {\n");
    buffer.push_str("    use super::{CategoryMatrixEntry, DefaultAutoResponse};\n\n");
    buffer.push_str(
        r#"    /// 暴露内部静态矩阵，供调用方遍历但禁止修改。
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
"#,
    );
    buffer.push_str("}\n\n");
    buffer.push_str("pub use matrix::{default_autoresponse, entries, entry_for_code};\n\n");
    buffer.push_str(
        r#"/// 描述单条“错误码 → 分类模板”的映射关系。
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
"#,
    );
    buffer.push_str(
        r#"/// 静态矩阵条目对应的模板枚举：封装 ErrorCategory 与默认动作的生成规则。
#[derive(Clone, Copy)]
pub enum CategoryTemplate {
    Retryable {
        wait_ms: u64,
        reason: &'static str,
        busy: Option<BusyDisposition>,
    },
    Timeout,
    ProtocolViolation {
        close_message: &'static str,
    },
    ResourceExhausted {
        budget: BudgetDisposition,
    },
    Cancelled,
    NonRetryable,
    Security {
        class: SecurityClass,
    },
}
"#,
    );
    buffer.push_str(
        r#"impl CategoryTemplate {
    /// 实例化错误分类。
    pub fn instantiate(&self) -> ErrorCategory {
        match self {
            CategoryTemplate::Retryable {
                wait_ms,
                reason,
                ..
            } => {
                let mut advice = RetryAdvice::after(Duration::from_millis(*wait_ms));
                if !reason.is_empty() {
                    advice = advice.with_reason(*reason);
                }
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

    /// 默认自动响应动作，供 Pipeline/Router 等模块读取。
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
"#,
    );
    buffer.push_str(
        r#"/// 默认自动响应动作，避免在热路径中重复匹配 CategoryTemplate。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultAutoResponse {
    RetryAfter {
        wait_ms: u64,
        reason: &'static str,
        busy: Option<BusyDisposition>,
    },
    BudgetExhausted { budget: BudgetDisposition },
    Close {
        reason_code: &'static str,
        message: &'static str,
    },
    Cancel,
    None,
}

impl DefaultAutoResponse {
    pub const fn budget(&self) -> Option<BudgetDisposition> {
        match self {
            DefaultAutoResponse::BudgetExhausted { budget } => Some(*budget),
            _ => None,
        }
    }

    pub const fn close_reason(&self) -> Option<(&'static str, &'static str)> {
        match self {
            DefaultAutoResponse::Close {
                reason_code,
                message,
            } => Some((*reason_code, *message)),
            _ => None,
        }
    }

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
"#,
    );
    buffer.push_str(
        r#"/// 描述“繁忙”语义的上下文，用于在重试时注入背压原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusyDisposition {
    Upstream,
    Downstream,
}
"#,
    );
    buffer.push_str(
        r#"impl BusyDisposition {
    /// 转换为 [`BusyReason`]，供 Pipeline 默认处理器复用。
    pub fn to_busy_reason(self) -> BusyReason {
        match self {
            BusyDisposition::Upstream => BusyReason::upstream(),
            BusyDisposition::Downstream => BusyReason::downstream(),
        }
    }
}
"#,
    );
    buffer.push_str(
        r#"/// 描述预算耗尽场景的预算类型，避免在常量表中直接引用 `BudgetKind`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetDisposition {
    Decode,
    Flow,
}
"#,
    );
    buffer.push_str(
        r#"impl BudgetDisposition {
    /// 转换为框架实际使用的 [`BudgetKind`]。
    pub fn to_budget_kind(self) -> BudgetKind {
        match self {
            BudgetDisposition::Decode => BudgetKind::Decode,
            BudgetDisposition::Flow => BudgetKind::Flow,
        }
    }
}
"#,
    );
    buffer.push_str("\n/// 静态矩阵：保持按错误码字典序排列，方便审查与 diff。\nconst MATRIX: &[CategoryMatrixEntry] = &[\n");
    for entry in entries {
        let const_name = code_constant(&entry.code);
        let template_expr = render_template(&entry.template);
        writeln!(
            buffer,
            "    CategoryMatrixEntry {{\n        code: crate::error::codes::{const_name},\n        template: {template_expr},\n    }},"
        )
        .expect("写入矩阵条目");
    }
    buffer.push_str("];\n");
    buffer
}

/// 将错误码转为常量名称，例如 `transport.io` → `TRANSPORT_IO`。
fn code_constant(code: &str) -> String {
    let mut name = String::with_capacity(code.len());
    for ch in code.chars() {
        match ch {
            '.' | '-' => name.push('_'),
            _ => name.push(ch),
        }
    }
    name.to_ascii_uppercase()
}

/// 渲染分类模板的 Rust 表达式。
fn render_template(template: &CategoryTemplateSpec) -> String {
    match template {
        CategoryTemplateSpec::Retryable {
            wait_ms,
            reason,
            busy,
        } => {
            let reason_literal = to_rust_string(reason);
            let busy_expr = match busy {
                Some(BusyDispositionSpec::Upstream) => {
                    "Some(BusyDisposition::Upstream)".to_string()
                }
                Some(BusyDispositionSpec::Downstream) => {
                    "Some(BusyDisposition::Downstream)".to_string()
                }
                None => "None".to_string(),
            };
            format!(
                "CategoryTemplate::Retryable {{\n            wait_ms: {wait_ms},\n            reason: {reason_literal},\n            busy: {busy_expr},\n        }}"
            )
        }
        CategoryTemplateSpec::Timeout => "CategoryTemplate::Timeout".to_string(),
        CategoryTemplateSpec::ProtocolViolation { close_message } => {
            let literal = to_rust_string(close_message);
            format!(
                "CategoryTemplate::ProtocolViolation {{\n            close_message: {literal},\n        }}"
            )
        }
        CategoryTemplateSpec::ResourceExhausted { budget } => {
            let budget_expr = match budget {
                BudgetDispositionSpec::Decode => "BudgetDisposition::Decode",
                BudgetDispositionSpec::Flow => "BudgetDisposition::Flow",
            };
            format!(
                "CategoryTemplate::ResourceExhausted {{\n            budget: {budget_expr},\n        }}"
            )
        }
        CategoryTemplateSpec::Cancelled => "CategoryTemplate::Cancelled".to_string(),
        CategoryTemplateSpec::NonRetryable => "CategoryTemplate::NonRetryable".to_string(),
        CategoryTemplateSpec::Security { class } => {
            let class_expr = match class {
                SecurityClassSpec::Authentication => "SecurityClass::Authentication",
                SecurityClassSpec::Authorization => "SecurityClass::Authorization",
                SecurityClassSpec::Confidentiality => "SecurityClass::Confidentiality",
                SecurityClassSpec::Integrity => "SecurityClass::Integrity",
                SecurityClassSpec::Audit => "SecurityClass::Audit",
                SecurityClassSpec::Unknown => "SecurityClass::Unknown",
            };
            format!("CategoryTemplate::Security {{\n            class: {class_expr},\n        }}")
        }
    }
}

/// 将任意字符串转换为合法的 Rust 字面量，处理转义字符。
fn to_rust_string(value: &str) -> String {
    let mut literal = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => literal.push_str("\\\\"),
            '"' => literal.push_str("\\\""),
            '\n' => literal.push_str("\\n"),
            '\r' => literal.push_str("\\r"),
            '\t' => literal.push_str("\\t"),
            c if c.is_control() => literal.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => literal.push(c),
        }
    }
    literal.push('"');
    literal
}
