#![cfg_attr(not(feature = "std"), no_std)]
#![deny(unsafe_code)]
#![allow(clippy::result_large_err)]
#![allow(private_bounds)]
#![doc = "spark-hosting: 为 Spark 宿主实现提供装配与运行期协调工具。"]

#[cfg(not(feature = "alloc"))]
compile_error!(
    "spark-hosting 依赖堆分配能力：请启用默认特性或通过 `--features alloc` 显式打开该功能。",
);

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod builder;
mod host;
mod pipeline;
pub mod runtime;
mod service;
pub mod shutdown;

pub use builder::{HostBuildError, HostBuilder, HostBuilderError};
pub use host::Host;
pub use pipeline::{
    MiddlewareRegistrationError, MiddlewareRegistry, factory::DefaultPipelineFactory,
};
pub use service::{ServiceEntry, ServiceFactory, ServiceRegistrationError, ServiceRegistry};
pub use shutdown::GracefulShutdownCoordinator;
