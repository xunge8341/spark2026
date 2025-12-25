//! 演示如何在第三方实现中一键运行 Spark 契约测试套件（TCK）。
//!
//! # 教案式说明
//! - **意图 (Why)**：通过极简测试模块向读者展示 `#[spark_tck]` 的使用方式，确保能在自身项目中快速复用。
//! - **逻辑 (How)**：
//!   1. 以 dev-dependency 引入 `spark-contract-tests`；
//!   2. 在测试模块上应用宏，宏将为每个主题自动生成 `#[test]`；
//!   3. 运行 `cargo test` 时，TCK 会调用默认实现验证各项契约。
//! - **契约 (What)**：
//!   - **前置条件**：需在标准库环境下构建，且 `spark-core` 依赖满足编译；
//!   - **后置条件**：测试成功意味着演示项目的传输层至少满足官方 TCK 的基本约束。

use spark_contract_tests::spark_tck;

#[spark_tck]
mod contract_tck {}
