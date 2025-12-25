//! 时间相关集成测试入口，确保 `Clock` 抽象与重试节律在真实/虚拟时间下保持一致。
//!
//! # 模块目的（Why）
//! - 汇集所有与时间源注入相关的集成测试，便于统一运行与过滤；
//! - 对齐验收命令 `cargo test -p spark-core -- tests::time::*` 的过滤路径，确保 CI 能准确定位测试。
//!
//! # 结构概览（What）
//! - [`tests::time::deterministic_retry_after`]：验证 `RetryAfterThrottle` 在虚拟时钟下的确定性唤醒。
//!
//! # 维护提示（How）
//! - 新增时间相关集成测试时，请在此处增加相应的子模块，并在测试内部补齐“教案级”注释；
//! - 若引入额外依赖（如网络或 I/O），请说明原因与替代方案，以免影响测试稳定性。

pub mod tests {
    //! 集成测试命名空间：将所有时间相关测试归档在 `tests::time` 之下，便于过滤。
    pub mod time {
        //! 时间契约相关的单元与集成测试集合。
        include!("deterministic_retry_after.rs");
    }
}
