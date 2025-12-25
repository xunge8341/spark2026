#![cfg_attr(not(feature = "std"), no_std)]
#![warn(missing_docs)]

//! `spark-codec-line` 提供基于换行符分帧的文本编解码扩展示例。
//!
//! # 教案背景（Why）
//! - 任务 T24 要求在**不修改核心 crate** 的前提下新增扩展，本文档示例演示如何编写遵循 `spark-codecs` 契约的外部 Codec；
//! - 通过真实的对象实现与合约测试，验证核心 API 的可扩展性并为后续伙伴提供模板；
//! - 采用行分隔协议是因为其业务语义直观，可聚焦展示缓冲池、预算控制与错误处理的交互模式。
//!
//! # 使用概览（How）
//! - 引入本 crate 后，可直接实例化 [`LineDelimitedCodec`] 并注册到运行时的 `CodecRegistry`；
//! - 编码端会自动从 `EncodeContext` 的缓冲池租借可写缓冲并附加换行符；
//! - 解码端在检测到完整的一行数据后返回业务字符串，否则返回 `DecodeOutcome::Incomplete` 提醒上游继续等待。
//!
//! # 合约说明（What）
//! - 该扩展完全依赖 `spark-codecs` 聚合后的稳定接口，不需要启用核心 crate 的 `std` 功能即可构建；
//! - 所有错误码遵循 `spark_codecs::codes` 约定，便于在指标、日志中统一聚合；
//! - 默认内容类型为 `text/plain; charset=utf-8`，调用方可据此配置跨语言互通策略。
//!
//! # 风险提示与后续（Trade-offs）
//! - 行分隔协议不包含转义策略，生产环境若需要支持二进制或多行文本，应扩展为长度前缀或引入转义；
//! - 当前实现假设底层缓冲为连续内存块，如需在分片缓冲上使用，请结合 `DecodeContext::acquire_scratch` 做增量重组。

extern crate alloc;

mod line;

#[cfg(feature = "compat_v0")]
pub mod compat;

pub use crate::line::LineDelimitedCodec;
