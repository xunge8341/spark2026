//! 自适应重试策略集成测试入口。
//!
//! # 教案式说明
//! - **目标（Why）**：围绕 `retry::adaptive` 的策略演进提供可重复的拥塞模拟，
//!   确保退避窗口在拥塞爬升与回落时保持稳定、低误触发。
//! - **结构（What）**：将测试集中暴露在 `tests::retry` 命名空间下，便于按照验收命令
//!   `cargo test -p spark-core -- tests::retry::adaptive_policy::*` 精确过滤。
//! - **维护提示（How/Trade-offs）**：新增策略测试时，请在此入口追加 `include!`，并确保
//!   每个测试文件包含“教案级”注释解释模拟场景与阈值选择。

pub mod tests {
    //! retry 策略相关测试命名空间。
    pub mod retry {
        //! 自适应策略的拥塞回放测试。
        include!("adaptive_policy.rs");
    }
}
