//! Spark 契约测试套件（TCK）入口。
//!
//! # 教案式综述（Why / How / What）
//! - **为什么存在**：T34 任务要求将契约测试下沉为独立 crate，方便第三方实现按统一准绳进行自测。
//!   因此本 crate 集中维护背压、取消、错误、状态机、优雅关闭、热插拔、热重载、可观测性、慢连接防护、
//!   资源枯竭十大主题的校验用例。
//! - **如何集成**：在目标仓库的 `tests` 目录下引入 `#[spark_tck]` 宏（或直接调用 `run_*` 入口函数），即可将完整
//!   套件编译为标准的 Rust 测试；宏支持选择性启用子套件，满足增量验证需求。
//! - **测试对象**：所有用例均以 `spark-core` 暴露的稳定面为边界，既覆盖轻量级数据结构（如 `Budget`、
//!   `Cancellation`），也验证运行期状态机（如 `HotSwapPipeline` 与热重载配置），并扩展到优雅关闭协
//!   调器的 FIN/超时契约。
//!
//! # 契约说明（What）
//! - **输入要求**：调用方仅需在构建时依赖 `spark-core` 与本 crate；无额外的环境前置（热重载测试会内部创建模拟配置源）。
//! - **输出保证**：若全部测试通过，可确信实现满足框架对背压、取消、错误处理、可观测性等核心契约的显式约束。
//! - **特性组合**：`alloc` 特性会自动启用 `std`，以确保多线程/同步原语可用；当前不提供 `no_std` 模式。
//!
//! # 风险提示（Trade-offs）
//! - 套件默认使用线程与互斥锁模拟运行环境，第三方在 `no_std` 或裸机场景需要自行提供替代实现并按需跳过相关用例。
//! - 若未来 `spark-core` 的契约发生破坏性升级，务必先在此 crate 中更新测试，再在下游仓库同步宏版本，避免
//!   “红绿灯”状态错判。
//!
//! # 模块结构
//! - `case` 模块：定义测试用例与套件的元信息结构体，以及统一的执行辅助函数。
//! - 子模块 `backpressure`、`cancellation` 等分别实现十大主题的实际断言逻辑。
//! - 顶层提供 `run_*` 入口与 `#[spark_tck]` 宏 re-export，供外部直接调用。

mod backpressure;
mod cancellation;
mod errors;
mod graceful_shutdown;
mod hot_reload;
mod hot_swap;
mod observability;
mod resource_exhaustion;
mod slowloris;
mod state_machine;
mod support;

use case::{TckSuite, run_suite};
pub use spark_contract_tests_macros::spark_tck;

const ALL_SUITES: [&TckSuite; 10] = [
    backpressure::suite(),
    cancellation::suite(),
    errors::suite(),
    graceful_shutdown::suite(),
    state_machine::suite(),
    hot_swap::suite(),
    hot_reload::suite(),
    observability::suite(),
    slowloris::suite(),
    resource_exhaustion::suite(),
];

mod case {
    use super::support;
    use std::panic;

    /// 表示单个 TCK 用例的元信息。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：以结构体封装测试函数与名称，便于统一遍历、打印上下文信息，避免宏中硬编码字符串。`
    /// - **逻辑 (How)**：`name` 采用 `'static` 字符串，`test` 为零参数函数指针；通过组合使得 `const` 数组定义成为可能。
    /// - **契约 (What)**：`test` 必须在失败时 `panic`，不可返回 `Result` 后忽略；名称会用于错误提示。
    #[derive(Clone, Copy)]
    pub struct TckCase {
        /// 用例的人类可读名称。
        pub name: &'static str,
        /// 实际执行的断言逻辑。
        pub test: fn(),
    }

    /// 代表同一主题的一组 TCK 用例。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：宏需要一次性生成多个 `#[test]`，因此把每个主题的用例聚合为 `TckSuite`。
    /// - **逻辑 (How)**：包含一个名称和 `TckCase` 切片，所有数据均使用 `'static` 生命周期以支持编译期构造。
    /// - **契约 (What)**：`cases` 不允许为空，名称与 `run_*` 函数之间保持一一对应关系。
    #[derive(Clone, Copy)]
    pub struct TckSuite {
        /// 套件名称，供日志与宏展开使用。
        pub name: &'static str,
        /// 归属该套件的用例集合。
        pub cases: &'static [TckCase],
    }

    /// 在捕获 panic 的前提下执行整个套件。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：为外部入口与宏提供统一执行路径，一旦用例失败即可附加“套件/用例”上下文后重新 panic。
    /// - **逻辑 (How)**：遍历 `cases`，借助 [`panic::catch_unwind`] 捕获 panic，将 payload 交给 `support::panic_with_context` 二次抛出。
    /// - **契约 (What)**：调用前确保 `suite.cases` 非空；若所有用例均成功，函数不会返回任何值；若失败则 panic。
    pub fn run_suite(suite: &TckSuite) {
        assert!(!suite.cases.is_empty(), "TCK 套件不应为空");
        for case in suite.cases {
            let outcome = panic::catch_unwind(panic::AssertUnwindSafe(|| (case.test)()));
            if let Err(payload) = outcome {
                support::panic_with_context(suite.name, case.name, payload);
            }
        }
    }
}

/// 返回所有已注册的 TCK 套件。
///
/// # 教案式说明
/// - **意图 (Why)**：便于 CLI 或自定义脚本按需遍历执行，避免调用方自己维护套件列表。
/// - **逻辑 (How)**：以固定顺序返回静态数组，与宏默认展开顺序保持一致，便于比对日志。
/// - **契约 (What)**：返回值生命周期为 `'static`，调用方不可修改内容。
pub fn all_suites() -> &'static [&'static TckSuite] {
    &ALL_SUITES
}

/// 运行“背压”主题的全部用例，覆盖预算与 ReadyState 契约。
pub fn run_backpressure_suite() {
    run_suite(backpressure::suite());
}

/// 运行“取消”主题的全部用例，验证父子令牌传播。
pub fn run_cancellation_suite() {
    run_suite(cancellation::suite());
}

/// 运行“错误自动响应”主题的全部用例，确保 `ErrorCategory` 映射正确。
pub fn run_errors_suite() {
    run_suite(errors::suite());
}

/// 运行“优雅关闭”主题的全部用例，验证 FIN/半关闭/超时契约。
pub fn run_graceful_shutdown_suite() {
    run_suite(graceful_shutdown::suite());
}

/// 运行“状态机”主题的全部用例，聚焦 `CallContext` 与执行视图一致性。
pub fn run_state_machine_suite() {
    run_suite(state_machine::suite());
}

/// 运行“热插拔”主题的全部用例，验证 Pipeline 动态调整的顺序性与 epoch 管理。
pub fn run_hot_swap_suite() {
    run_suite(hot_swap::suite());
}

/// 运行“热重载”主题的全部用例，确认配置增量更新的原子性。
pub fn run_hot_reload_suite() {
    run_suite(hot_reload::suite());
}

/// 运行“可观测性契约”主题的全部用例，确保默认字段稳定、自定义契约零拷贝。
pub fn run_observability_suite() {
    run_suite(observability::suite());
}

/// 运行“慢连接防护”主题的全部用例，验证编解码器在 Slowloris 场景下的预算控制。
///
/// # 教案式说明
/// - **意图 (Why)**：集中执行 `slowloris` 套件下的慢速读场景，帮助调用方确认 `DecodeContext` 能在合法慢读与
///   攻击性慢读之间做出正确区分。
/// - **流程 (How)**：依序运行 `slow_reader_drains_within_budget` 与 `slow_reader_exceeding_budget_is_rejected` 两个用例，
///   由统一的 `run_suite` 提供 panic 上下文包装。
/// - **契约 (What)**：调用前无需额外前置条件；调用成功代表 Slowloris 相关的 budget 策略均被覆盖测试。
pub fn run_slowloris_suite() {
    run_suite(slowloris::suite());
}

/// 运行“资源枯竭退避”主题的全部用例，确认 Busy/RetryAfter 的传播语义。
///
/// # 教案式说明
/// - **意图 (Why)**：一次性验证队列与线程池耗尽时，`ExceptionAutoResponder` 广播的 ReadyState 序列符合契约矩阵。
/// - **流程 (How)**：顺序执行 `channel_queue_exhaustion_emits_busy_then_retry_after` 与
///   `thread_pool_starvation_emits_busy_then_retry_after`，并在断言失败时附带套件上下文信息。
/// - **契约 (What)**：调用方应确保已链接 `spark-core` 默认错误码；执行成功即表示 Busy/RetryAfter 信号可被正确感知。
pub fn run_resource_exhaustion_suite() {
    run_suite(resource_exhaustion::suite());
}
