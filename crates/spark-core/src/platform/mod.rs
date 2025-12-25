//! 平台命名空间：抽象宿主环境、运行时调度与时间原语。
//!
//! # 模块定位（Why）
//! - 将宿主 Host、集群拓扑、运行时执行器与时间工具统一收敛，体现“platform = 控制面运行平台”；
//! - 与治理、数据面解耦，仅依赖 [`crate::kernel`] 与 [`crate::governance`] 暴露的契约；
//! - 为未来拆分平台相关 crate 打下基础。
//!
//! # 结构说明（What）
//! - [`runtime`]：任务调度、热更新栅栏、SLO 策略等运行时能力；
//! - [`host`]：宿主组件生命周期与依赖注入；
//! - [`cluster`]：服务发现与集群事件；
//! - [`time`]`*`：提供 `std` 环境下的时间工具（仅在启用 `std` feature 时可用）。
//!
//! # 维护建议（How）
//! - 平台模块可通过 `use crate::governance::...` 获取策略/配置，避免反向依赖；
//! - 若需要扩展新的宿主能力，请在此文件增加模块并同步更新文档层级；
//! - 调整运行时接口前，应检查 `spark-hosting` 等依赖 crate 的适配情况。

pub mod cluster;
pub mod host;
pub mod runtime;
#[cfg(feature = "std")]
pub mod time;
