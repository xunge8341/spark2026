//! 管道热插拔集成测试入口。
//!
//! # 教案式说明
//! - **意图（Why）**：为 `spark-core` 包提供围绕 Pipeline Pipeline 热插拔能力的集成测试集合，
//!   通过集中入口便于 Cargo 将位于子模块中的实际测试编译为同一个测试二进制。
//! - **逻辑（How）**：该模块仅负责将 `hot_swap` 子模块纳入编译，具体测试实现位于 `hot_swap.rs`
//!   中；运行 `cargo test -p spark-core --test pipeline` 即会执行该模块中所有 `#[test]` 标记的用例。
//! - **契约（What）**：当新增 Pipeline 集成测试时，应在此处增加 `mod xxx;` 语句，从而保证测试被发现；
//!   该文件本身不定义任何测试函数。
//! - **权衡（Trade-offs）**：统一入口提高测试可发现性，但需要额外维护模块列表；相比将测试文件放在
//!   `tests/` 根目录下，采用目录结构有助于按领域分组。
mod epoch_metrics;
mod hot_swap;
