//! Spark Core 默认实现对契约测试套件（TCK）的整体验证入口。
//!
//! # 教案式综述
//! - **意图 (Why)**：仓库提供的 `spark-core` 是基线实现，任何改动都必须通过标准 TCK 才能发布；因此在默认测试集中
//!   直接引入 `spark-contract-tests`，确保 CI 自动运行背压、取消、错误处理、状态机、热插拔、热重载与可观测性
//!   七大主题。
//! - **执行方式 (How)**：通过 `#[spark_tck]` 宏动态生成测试函数，宏会为每个主题注入一个 `#[test]`，内部调用
//!   `spark_contract_tests::run_*_suite`。借助宏避免手写样板代码，并保持与第三方集成方式一致。
//! - **契约约束 (What)**：
//!   - **前置条件**：`spark-core` 在编译期以 dev-dependency 方式依赖 `spark-contract-tests`；
//!   - **后置条件**：`cargo test --workspace` 会额外执行所有 TCK 套件，任何失败都会返回带有套件/用例上下文的 panic 信息。
//! - **设计权衡 (Trade-offs)**：测试运行时间略有增加，但换取了与第三方实现完全一致的验收门槛，避免“上游绿色、
//!   下游失败”的错判。

use spark_contract_tests::spark_tck;

#[spark_tck]
mod spark_core_contract_tck {
    //! # TCK 模块结构说明
    //!
    //! - **模块角色**：宏展开后会生成多个 `#[test]` 函数，名称形如 `backpressure_suite`，逐一运行
    //!   `spark-contract-tests` 内的主题套件。
    //! - **调用契约**：模块自身不声明任何函数或常量，保持与宏期望的一致结构，方便后续升级宏版本时无须额外调整。
    //! - **风险提示**：若未来 TCK 新增主题，只需升级依赖版本并重新运行测试，即可自动获取新的测试函数。
}
