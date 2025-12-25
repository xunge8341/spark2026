//! spark2026-fuzz 公共支持库。
//!
//! # 教案式定位
//! - **Why**：将 fuzz target 复用的缓冲池、脚本执行逻辑集中到库中，避免各 target
//!   重复实现，降低维护成本，并使得 CI 可直接调用相同逻辑进行回归验证。
//! - **What**：暴露 [`support`] 模块提供的内存缓冲封装，以及 [`protocols`] 模块的
//!   脚本解析执行入口，供 fuzz target 与常规测试共享。
//! - **How**：以 `std` 环境构建轻量模块，内部遵循 `spark-core` 的缓冲契约，
//!   并以纯函数形式暴露执行接口，便于在 CI 中直接调用。
extern crate alloc;

pub mod support;

pub mod protocols;

pub use protocols::execute_protocol_script;
