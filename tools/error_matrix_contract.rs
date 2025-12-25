//! 错误分类矩阵契约的共享解析逻辑。
//!
//! # 教案式设计说明（Why）
//! - 构建脚本与文档生成器都需解析 `contracts/error_matrix.toml`，若各自维护结构将导致漂移；
//! - 将解析逻辑集中到单一模块，便于在新增字段时一次性扩展并覆盖所有调用方；
//! - 该模块只依赖 `std` 与 `serde`，可被 `build.rs` 与 `tools/` 下的可执行程序同时复用。
//!
//! # 契约定义（What）
//! - 暴露 [`read_error_matrix_contract`] 与 [`expand_entries`] 两个函数，分别负责读取原始合约与展开后的矩阵条目；
//! - 结构体字段与 TOML 文件一一对应，若字段发生变更，必须同步更新注释与解析函数；
//! - **前置条件**：调用方需保证文件可读且内容符合约定的枚举字符串；
//! - **后置条件**：成功返回的结构体可直接驱动代码生成、文档渲染与契约测试。
//!
//! # 风险提示（Trade-offs & Gotchas）
//! - 当前实现基于 `toml` crate 的严格反序列化，若新增可选字段需谨慎处理默认值；
//! - `expand_entries` 会复制字符串以保证排序稳定性，在大规模矩阵下可能需要进一步优化。

use serde::Deserialize;
use std::{fs, path::Path};

/// 完整的错误矩阵契约，按声明顺序保存所有条目。
///
/// # 字段说明
/// - `rows`：原始 TOML 中的条目数组，保持作者书写顺序；
///
/// # 前置条件
/// - `contracts/error_matrix.toml` 必须存在且合法；
///
/// # 后置条件
/// - 解析成功后，`rows` 将按照 TOML 中的顺序填充，供后续展开或渲染使用。
#[derive(Debug, Deserialize)]
pub struct ErrorMatrixContract {
    pub rows: Vec<ErrorMatrixRow>,
}

/// 描述共享分类模板的一组错误码。
///
/// # 字段说明
/// - `codes`：一组遵循 `<域>.<语义>` 规范的错误码；
/// - `category`：对应的分类模板；
/// - `doc`：生成文档时使用的人类可读说明。
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // 构建脚本仅消费分类模板，文档生成器才会读取 `doc` 字段。
pub struct ErrorMatrixRow {
    pub codes: Vec<String>,
    pub category: CategoryTemplateSpec,
    pub doc: DocSpec,
}

/// 文档列的结构化描述，帮助生成 Markdown 表格。
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // 构建脚本未直接使用文档列，但需保留字段以确保契约完整。
pub struct DocSpec {
    pub rationale: String,
    pub tuning: String,
}

/// 声明式的错误分类模板，反映合约中允许的分类枚举。
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CategoryTemplateSpec {
    Retryable {
        wait_ms: u64,
        reason: String,
        #[serde(default)]
        busy: Option<BusyDispositionSpec>,
    },
    Timeout,
    ProtocolViolation {
        #[allow(dead_code)] // 文档生成器未读取关闭文案，构建脚本仍依赖该字段生成矩阵。
        close_message: String,
    },
    ResourceExhausted {
        budget: BudgetDispositionSpec,
    },
    Cancelled,
    NonRetryable,
    Security {
        class: SecurityClassSpec,
    },
}

/// 可选的 Busy 主语义，指导自动响应时向上游或下游广播状态。
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum BusyDispositionSpec {
    Upstream,
    Downstream,
}

/// 预算耗尽时的预算类型，映射到运行期的 `BudgetKind` 枚举（由 `spark-core` 提供）。
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum BudgetDispositionSpec {
    Decode,
    Flow,
}

/// 安全事件分类，帮助生成统一的关闭提示与指标标签。
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SecurityClassSpec {
    Authentication,
    Authorization,
    Confidentiality,
    Integrity,
    Audit,
    Unknown,
}

/// 展开后的矩阵条目：每个错误码都会对应一条独立记录。
///
/// # 字段说明
/// - `code`：单个错误码；
/// - `template`：克隆自原始行的分类模板，保证每条记录具备完整上下文。
#[derive(Debug, Clone)]
#[allow(dead_code)] // `ExpandedEntry` 仅供构建脚本使用，其他工具不会直接构造。
pub struct ExpandedEntry {
    pub code: String,
    pub template: CategoryTemplateSpec,
}

/// 从磁盘读取并解析错误矩阵合约。
///
/// # Why
/// - 构建脚本与工具链都依赖该函数，确保解析错误时具备一致的报错语句，便于排查；
///
/// # How
/// - 通过 `fs::read_to_string` 读取文件，再使用 `toml::from_str` 反序列化；
/// - 失败时携带路径上下文 panic，提示使用者检查语法或文件权限。
///
/// # What
/// - **输入**：`path` 为合约文件路径；
/// - **返回**：结构化的 [`ErrorMatrixContract`]；
/// - **前置条件**：文件必须存在且内容合法；
/// - **后置条件**：若成功返回，调用方即可进一步展开或渲染。
pub fn read_error_matrix_contract(path: &Path) -> ErrorMatrixContract {
    let raw = fs::read_to_string(path).unwrap_or_else(|err| {
        panic!("读取 {path:?} 失败: {err}");
    });
    toml::from_str(&raw).unwrap_or_else(|err| {
        panic!("解析 {path:?} 失败: {err}");
    })
}

/// 将合约中的多错误码条目展开为逐条记录，并按字典序排序。
///
/// # Why
/// - 代码与文档的 diff 需要稳定顺序，以方便评审；
/// - 许多测试依赖 `entries()` 的遍历顺序来与文档进行比对。
///
/// # How
/// - 遍历每一行，将其中的 `codes` 与模板配对后推入结果向量；
/// - 最终按错误码字符串排序，确保结果稳定。
///
/// # What
/// - **输入**：原始合约；
/// - **输出**：排序后的 [`ExpandedEntry`] 列表；
/// - **前置条件**：合约已通过 [`read_error_matrix_contract`] 成功解析；
/// - **后置条件**：返回值可直接用于代码生成或测试断言。
#[allow(dead_code)] // 展开逻辑仅服务于代码生成，文档工具无需调用。
pub fn expand_entries(contract: &ErrorMatrixContract) -> Vec<ExpandedEntry> {
    let mut entries = Vec::new();
    for row in &contract.rows {
        for code in &row.codes {
            entries.push(ExpandedEntry {
                code: code.clone(),
                template: row.category.clone(),
            });
        }
    }
    entries.sort_by(|a, b| a.code.cmp(&b.code));
    entries
}
