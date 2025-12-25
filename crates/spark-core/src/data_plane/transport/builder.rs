//! 传输构建器抽象层，统一各介质的 builder 模式接口。
//!
//! # 设计目标（Why）
//! - **零耦合原则**：Codec 与 Transport 之间必须经由 `spark-core` 定义的契约互动，本模块提供“建造器”抽象，确保介质特有的
//!   配置项在各自实现内部下沉，却依旧遵循统一的调度接口；
//! - **一致的生命周期控制**：监听器/端点的初始化往往涉及异步步骤（绑定套接字、握手预热等），通过统一 trait，
//!   宿主可以在不关心具体协议的前提下，以同一模式注入 [`Context`] 并获得 Future；
//! - **可扩展性**：Builder 返回的 Future 具备 `Send` 约束，兼容多线程运行时，同时允许后续在不破坏现有实现的情况下扩展自定义介质。
//!
//! # 使用方式（How）
//! - 每个具体 Transport 实现（TCP/UDP/QUIC/TLS 等）提供自身的 `Builder` 结构体，封装介质特有的参数；
//! - 实现 [`TransportBuilder`] trait，即可被宿主或运行时统一调度：调用 `scheme()` 了解协议名，调用 `build` 即返回异步构建结果；
//! - `build` 的 Future 会继承 [`Context`]，因此实现者应在构造过程中尊重取消与截止时间。
//!
//! # 契约与约束（What）
//! - `TransportBuilder` 要求 `Send + 'static`，保证可以跨线程移动或存储在异步任务中；
//! - `Output` 须携带 `Send + 'static`，以便与运行时后续的监听/建连逻辑结合；
//! - `scheme()` 必须返回 `'static` 字符串，用于在观测与注册中心中区分协议；
//! - `build` 接收 [`Context`]：实现者应在 Future 内检查取消标记或剩余预算，必要时提前返回 [`CoreError`]。
//!
//! # 风险提示（Trade-offs & Gotchas）
//! - Builder 模式强调延迟配置：若实现者在构造 Builder 后修改内部状态，请确保线程安全；
//! - `build` 返回的 Future 不允许 `!Send`，否则在多线程 Tokio 上会 panic；
//! - 若某些介质的构建本身为同步操作，可返回 `async { Ok(..) }` 形式的 Future，保持接口一致性。

use core::future::Future;

use crate::{CoreError, context::Context};

/// 统一的传输建造器 trait。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 将 TCP/UDP/QUIC/TLS 等介质特有的 builder 抽象为统一接口，使上层调度器能够在不分支判断的前提下批量启动监听器或端点；
/// - 通过 [`Context`] 把取消、截止与预算语义延伸到构造阶段，避免慢启动或阻塞导致的资源占用。
///
/// ## 契约（What）
/// - `Output`：具体介质构造出的对象类型，例如 `TcpServerChannel`、`UdpEndpoint`；要求实现 `Send + 'static`，以便后续跨线程使用；
/// - `BuildFuture<'ctx>`：构造流程返回的 Future 类型，需满足 `Send + 'ctx`，并在完成时给出 `Output` 或 [`CoreError`]；
/// - `scheme()`：返回协议名（如 `"tcp"`），用于指标/日志标签；
/// - `build(self, ctx)`：执行实际构建逻辑，需关注 `ctx` 的取消与截止状态。
///
/// ## 解析逻辑（How）
/// - trait 采用 GAT（Generic Associated Type）定义 `BuildFuture`，允许实现方返回零分配的 `async move` Future；
/// - 接口消费 `self`，鼓励 Builder 在 `build` 后不再复用，避免状态污染；
/// - 若构建前检测到 `ctx` 已取消，可直接返回 `Err(CoreError)` 以节省资源。
///
/// ## 风险提示（Trade-offs）
/// - 构建流程若需长时间 I/O，应在内部定期检查 `ctx.cancellation()`，否则会违背取消契约；
/// - 若 `scheme()` 返回的字符串不唯一，可能导致指标或注册中心冲突；
/// - 实现者应确保 `Output` 类型在 drop 时正确释放资源，避免 builder 短期内大量失败导致资源泄露。
pub trait TransportBuilder: Send + 'static {
    /// 构造结果类型，例如监听器或端点。
    type Output: Send + 'static;

    /// 构造流程返回的 Future 类型。
    type BuildFuture<'ctx>: Future<Output = crate::Result<Self::Output, CoreError>> + Send + 'ctx
    where
        Self: 'ctx;

    /// 返回该 Builder 对应的协议标识。
    fn scheme(&self) -> &'static str;

    /// 执行构造流程。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 将调用方的执行上下文传递给构造流程，使监听器或连接端点的初始化也能响应取消与截止；
    /// - 让具体实现有机会在启动前检查预算、审计或其他跨领域约束。
    ///
    /// ## 契约（What）
    /// - `ctx`：执行上下文，生命周期需覆盖整个 Future；
    /// - 返回：`Output` 或 [`CoreError`]；若 `ctx` 已取消，建议尽早返回 `Cancelled` 错误码。
    ///
    /// ## 实现注意（Trade-offs）
    /// - Builder 应避免在 `build` 外部就启动异步任务，以免在取消时出现悬挂；
    /// - 若需要长时间同步操作，可考虑在实现内部使用 `spawn_blocking` 等手段结合 `ctx` 的预算策略。
    fn build<'ctx>(self, ctx: &'ctx Context<'ctx>) -> Self::BuildFuture<'ctx>;
}
