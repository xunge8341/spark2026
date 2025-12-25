#![cfg_attr(not(feature = "std"), no_std)]
#![deny(unsafe_code)]
#![allow(clippy::result_large_err)]
#![doc = "spark-examples: 协调传输监听器与 Pipeline 装配的轻量级宿主组件。"]
#![doc = ""]
#![doc = "== 设计初衷 =="]
#![doc = "- **Why**：为 `spark-core` 提供一个可以在运行期管理 Transport 生命周期的示例宿主，使控制面能力能够独立于具体协议演进；"]
#![doc = "- **What**：封装对 `DynTransportFactory` 的调度、监听器持久化与后续 Pipeline 接入的骨架逻辑；"]
#![doc = "- **How**：依赖 `no_std + alloc` 环境，所有异步/同步能力均通过 `spark-core` 提供的契约注入，无需直接依赖操作系统。"]

#[cfg(not(feature = "alloc"))]
compile_error!(
    "spark-examples 依赖堆分配能力：请启用默认特性或通过 `--features alloc` 显式打开该功能。"
);

extern crate alloc;

mod server;
mod transport;

pub use server::{PipelineFactory, ExamplesServer};
