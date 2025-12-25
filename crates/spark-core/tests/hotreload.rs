//! 热更新相关集成测试入口。
//!
//! ## 设计目的（Why）
//! - 提供单一的 Cargo 集成测试目标名称 `hotreload`，便于 CI 与开发者筛选执行。
//! - 借助 `#[path]` 属性将子模块映射到子目录，保持文件结构层次化且避免模块名冲突。
//!
//! ## 契约说明（What）
//! - 当新增热更新测试时，可在此文件中追加 `mod` 声明即可生效。
//! - 本文件自身不包含测试逻辑，仅负责模块装配，不引入共享状态。
#[path = "hotreload/consistency.rs"]
mod consistency;
#[path = "hotreload/limits_timeout.rs"]
mod limits_timeout;

/// 测试名称命名空间。
///
/// - `tests::hotreload::*` 供命令行过滤使用。
pub mod tests {
    /// 热更新相关测试的统一入口，re-export 实际实现。
    pub mod hotreload {
        pub use super::super::limits_timeout::hot_reload_limits_and_timeouts_is_atomic_case;

        /// 并发热更新限流与超时配置，验证 ArcSwap 原子替换语义。
        #[test]
        fn hot_reload_limits_and_timeouts_is_atomic() {
            hot_reload_limits_and_timeouts_is_atomic_case();
        }

        pub mod consistency {
            pub use super::super::super::consistency::hot_reload_config_consistency_case;

            /// 高并发环境下验证配置热更新的无撕裂与指标打点。
            #[test]
            fn hot_reload_configuration_is_consistent() {
                hot_reload_config_consistency_case();
            }
        }
    }
}
