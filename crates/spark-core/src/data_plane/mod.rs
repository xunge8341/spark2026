//! 数据面命名空间：串联从缓冲区、编解码、协议到业务服务的完整链路。
//!
//! # 模块定位（Why）
//! - 将请求处理流程的关键阶段按数据流顺序组织，帮助读者快速了解消息如何在框架内流转；
//! - 统一暴露 `buffer/codec/protocol/transport/pipeline/service`，便于文档引用与外部集成；
//! - 抽离后可在未来拆分为独立 crate，同时共享 `kernel` 契约。
//!
//! # 阅读顺序（How）
//! 1. [`buffer`]：内存与零拷贝抽象；
//! 2. [`codec`]：编解码注册与上下文；
//! 3. [`protocol`]：协议原语与消息模型；
//! 4. [`transport`]：网络传输与监听管理；
//! 5. [`pipeline`]：中间件链、控制器与上下文；
//! 6. [`service`]：面向业务的服务 trait 与自动装配工具。
//!
//! # 维护建议（Trade-offs）
//! - 数据面大量依赖 `kernel::contract` 与治理策略，重构前请确认调用链；
//! - 若模块间出现循环引用，请通过 trait 或事件桥解耦，避免直接跨层 `use`；
//! - 添加新阶段时，务必在此文件更新顺序说明，保持“数据流向”语义清晰。

pub mod buffer;
pub mod codec;
pub mod pipeline;
pub use pipeline::initializer;
pub mod protocol;
pub mod service;
pub mod transport;
