//! Platform Runtime Mechanism
//!
//! Foundation 版本仅保留执行器/定时器/任务抽象与无害的糖（sugar）。
//!
//! 说明：
//! - 已删除 `slo` / `hotreload` 等策略模块；
//! - 已删除任何策略层 crate 的 re-export；
//! - 为保证宏与上层调用路径连续，保留 `sugar` 相关导出。

mod executor;
mod services;
pub mod sugar;
mod task;
mod timer;

pub use executor::{spawn_blocking, spawn_local, AsyncRuntime, SpawnError, TaskExecutor};
pub use services::{ServiceTask, ServiceTaskHandle};
pub use task::{JoinError, JoinHandle, TaskId};
pub use timer::{sleep, sleep_until, Instant, Interval, MissedTickBehavior, Sleep};

pub use sugar::{spawn_in, CallContext, PipelineContextCaps, RuntimeCaps};
