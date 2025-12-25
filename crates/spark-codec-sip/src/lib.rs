#![cfg_attr(not(feature = "std"), no_std)]
#![warn(missing_docs)]

//! # spark-codec-sip
//!
//! ## 教案目的（Why）
//! - **定位**：该 crate 负责 Session Initiation Protocol (SIP) 文本报文的解析与生成，是 spark 平台信令面的入口。
//! - **架构角色**：在整体系统中，本模块向上传递业务语义（请求、响应、核心头部），向下为传输与媒体协商提供零拷贝的结构化数据。
//! - **设计演进**：此迭代完成基础类型、核心解析器与格式化器的落地，为 `spark-tck` 的协议一致性测试奠定基础。
//!
//! ## 交互契约（What）
//! - **输入前提**：调用方提供 UTF-8 编码的 SIP 文本；解析器遵循 RFC 3261 §7–§8, §19, §20 以及 RFC 3581（`rport`）。
//! - **输出能力**：
//!   - 将请求/响应起始行、指定的核心头部以零拷贝结构返回；
//!   - 支持将结构化对象重新序列化为文本，确保回写字段符合大小写与折行规则。
//! - **前置条件**：文本必须使用 CRLF 分隔行，header 部分与 body 使用空行分隔。
//! - **后置条件**：解析成功后不会持有可变引用，可在原始缓冲生命周期内安全访问字段切片。
//!
//! ## 实现策略（How）
//! - **模块划分**：
//!   1. `types`：定义零拷贝基础类型与头部模型；
//!   2. `parse`：实现请求行、响应行与核心头部解析，处理折行与大小写；
//!   3. `fmt`：提供对应的序列化能力，保证输出遵循标准格式；
//!   4. `error`：集中描述解析/格式化错误，简化调用方匹配逻辑。
//! - **关键技巧**：使用切片引用 (`&str`/`&[u8]`) 避免拷贝；保留原始 header 名称大小写，字段名比较采用 ASCII case-insensitive。
//! - **扩展考虑**：未来可在 `types` 中追加更多 header 变体，或为 `SipMessage` 引入增量解析能力。
//!
//! ## 风险提示（Trade-offs）
//! - **性能边界**：当前实现使用 `Vec` 存储 header；若需极致性能，可引入自定义 arena。现状在 `alloc` feature 下即可运行于 no_std 环境。
//! - **功能边界**：仅覆盖 RFC 中列出的核心头部，其余头部以原文形式保留在 `Header::Extension` 中。
//! - **维护建议**：新增解析能力时需同步更新注释及 `spark-tck` 的断言，保持接口语义稳定。

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod error;
pub mod fmt;
pub mod parse;
/// Registrar 辅助类型，封装 Address-of-Record 与 Contact 的所有权模型。
pub mod registrar;
/// INVITE 事务状态机模型与 CANCEL 竞态辅助工具。
pub mod transaction;
pub mod types;
pub mod ws;

pub use crate::error::{SipFormatError, SipParseError};
pub use crate::parse::{parse_request, parse_request_bytes, parse_response};
pub use crate::types::{
    CSeqHeader, ContactHeader, Header, HeaderName, MaxForwardsHeader, Method, NameAddr,
    RequestLine, SipMessage, SipUri, StatusLine, ViaHeader, ViaParamRport,
};
pub use registrar::{Aor, AorRef, ContactUri, ContactUriRef};
pub use transaction::{
    CancelOutcome, FinalResponseDisposition, InviteServerTransaction, InviteServerTransactionState,
};

/// SIP 编解码骨架类型，维持与上一迭代兼容的构造入口。
///
/// ### 角色定位（Why）
/// - 作为外部依赖的稳定锚点，避免在功能扩展期间破坏调用方。
/// - 内部委托解析/格式化逻辑给 `parse` 与 `fmt` 模块，聚焦于装配职责。
///
/// ### 契约说明（What）
/// - **输入**：无，构造函数 `new` 直接返回可复用的零尺寸实例。
/// - **输出**：提供访问本 crate 解析/格式化 API 的上下文标记；不存储状态。
/// - **前置条件**：调用环境已通过 Cargo feature 启用所需依赖（`alloc`/`std`）。
/// - **后置条件**：实例可按需复制，无需考虑 Drop 顺序。
///
/// ### 实现摘要（How）
/// - 维持零尺寸类型，避免重复状态同步；
/// - 后续若需承载配置，可在不破坏现有 API 的前提下引入字段与构建器。
///
/// ### 风险提示（Trade-offs）
/// - 若未来需要携带全局缓存，应同步更新 `Clone`/`Copy` 语义；
/// - 当新增依赖（如字典表）时，需评估 no_std 场景的可行性。
#[derive(Debug, Default, Clone, Copy)]
pub struct SipCodecScaffold;

impl SipCodecScaffold {
    /// 创建 SIP 编解码骨架实例。
    ///
    /// ### 设计动机（Why）
    /// - 为上层在构造解析器前提供统一入口，后续可扩展为具备状态的 codec。
    /// - 便于测试环境（如 `spark-tck`）快速生成标记实例，确保依赖完整。
    ///
    /// ### 调用契约（What）
    /// - **输入参数**：无；无外部配置需求。
    /// - **返回值**：`SipCodecScaffold`，可复制、可默认构造。
    /// - **前置条件**：调用方无需特殊准备，仅需满足生命周期约束。
    /// - **后置条件**：返回值可用于访问 `parse::*`/`fmt::*` 静态函数，不会持有可变状态。
    ///
    /// ### 实现说明（How）
    /// - 采用 `const fn`，允许在静态上下文中构造；
    /// - `#[must_use]` 提醒调用者实例化的目的。
    ///
    /// ### 风险提示（Trade-offs）
    /// - 当未来引入可配置项时，需考虑向后兼容策略（如构建器模式）。
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}
