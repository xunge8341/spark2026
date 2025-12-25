use core::{future::Future, task::Context as TaskContext};

use crate::status::PollReady;
use crate::{Error, context::Context, contract::CallContext, sealed::Sealed};

/// `Service` 提供 Spark 数据平面“零虚分派”范式下的业务调用契约。
///
/// # 契约维度速览
/// - **语义**：`poll_ready`/`call` 双阶段驱动业务处理，配合 [`CallContext`] 传播取消、截止与预算语义。
/// - **错误**：实现返回的 `Error` 必须实现 [`crate::Error`]，常见错误码包括 `service.busy`、`service.unavailable`、`app.*`。
/// - **并发**：Trait 要求 `Send + Sync`，允许多线程并发调用 `poll_ready`/`call`；实现方应保证内部状态的线程安全。
/// - **背压**：`poll_ready` 应返回 [`ReadyState`](crate::status::ReadyState) 或 [`BusyReason`](crate::status::BusyReason) 映射的状态，帮助上层退避。
/// - **超时**：应在 `poll_ready` 与 `call` 内检查 `ctx.deadline()`；若超时应返回 `CoreError` 或业务错误提示上游终止。
/// - **取消**：`CallContext` 携带 [`Cancellation`](crate::contract::Cancellation)，长耗时操作需定期检查并及时终止。
/// - **观测标签**：建议在指标/日志中输出 `service.name`、`service.route`、`service.outcome`（成功/失败/背压）。
/// - **示例(伪码)**：
///   ```text
///   loop {
///       match service.poll_ready(ctx.view(), task_cx)? {
///           ReadyState::Ready => break,
///           ReadyState::Busy(reason) => retry_with_backoff(reason),
///           _ => handle_budget_or_retry()
///       }
///   }
///   response = await service.call(call_ctx.clone(), request)
///   ```
///
/// # 设计初衷（Why）
/// - 继承 Tower `Service`/`Layer` 生态的编排模式，允许上层通过泛型组合实现零开销内联；
/// - 在控制面统一 `CallContext`（取消/截止/预算三元组）之后，本接口成为所有 Handler、Router
///   与传输模块共享的基座；
/// - 与对象层 [`crate::service::object::DynService`] 保持语义一致，为 T05“二层 API”目标
///   提供形式化的“泛型基线”。
///
/// # 行为逻辑（How）
/// 1. `poll_ready` 读取 `Context`，基于预算/截止判断是否可继续接收请求；
/// 2. `call` 在保证 `poll_ready` 返回可用后被调用，消费一个 [`CallContext`] 并驱动异步结果；
/// 3. 所有关联类型均在编译期内联，避免动态分派与堆分配。
///
/// # 契约说明（What）
/// - **输入**：`Request` 为调用方自定义消息类型，必须满足 `Send + Sync + 'static`；
///   关联的 `Error` 实现 [`crate::Error`] 以融入统一错误链；
/// - **前置条件**：调用方必须在 `call` 前反复驱动 `poll_ready` 直至 `ReadyState::Ready`；
/// - **后置条件**：成功处理后保证在相同上下文下可继续进行下一次 `poll_ready`/`call` 循环；
/// - **返回**：`Future` 输出 `crate::Result<Response, Error>`，用于承载异步业务结果。
///
/// # 风险与取舍（Trade-offs）
/// - 若实现依赖 `async fn`，需额外包装为 `Pin<Box<...>>` 或结合 GAT 特性；
/// - `CallContext` 克隆成本为常数时间，但高频克隆仍会产生 ARC 原子操作开销，应合理缓存。
pub trait Service<Request>: Send + Sync + 'static + Sealed {
    /// 服务输出类型。
    type Response;
    /// 业务错误类型。
    type Error: Error;
    /// 代表一次调用的异步返回值。
    type Future: Future<Output = crate::Result<Self::Response, Self::Error>> + Send + 'static;

    /// 检查服务是否准备好接收下一次调用。
    ///
    /// - **输入参数**：`ctx` 为轻量视图，承载取消/截止/预算；`cx` 为运行时调度使用的 waker 上下文；
    /// - **输出语义**：返回 [`PollReady<Self::Error>`]，其内部状态应映射为
    ///   [`BackpressureSignal`](crate::contract::BackpressureSignal) 的子集，统一表达就绪、背压、
    ///   预算耗尽与关闭意图；
    /// - **边界条件**：若返回 `Poll::Pending`，实现必须在状态改变时唤醒 `cx.waker()`。
    fn poll_ready(&mut self, ctx: &Context<'_>, cx: &mut TaskContext<'_>)
    -> PollReady<Self::Error>;

    /// 发起一次业务调用。
    ///
    /// - **输入参数**：`ctx` 为可克隆的上下文，`req` 为业务请求体；
    /// - **前置条件**：最近一次 `poll_ready` 已返回 `ReadyState::Ready`；
    /// - **返回值**：异步 `Future`，完成时给出业务响应或错误。
    fn call(&mut self, ctx: CallContext, req: Request) -> Self::Future;
}

/// `Layer` 描述服务在泛型层的中间件组合方式。
///
/// # 设计初衷（Why）
/// - 将鉴权、重试、熔断等横切逻辑通过泛型“编译期装配”，避免对象层虚分派开销；
/// - 与 [`Service`] 形成双元组：任何 Layer 必须保持请求/响应类型不变，才能透明插入调用路径。
///
/// # 行为逻辑（How）
/// - `layer` 接收一个内部服务 `inner`，返回包裹后的新服务类型；
/// - 返回类型再次实现 [`Service`]，形成可叠加的泛型链路。
///
/// # 契约说明（What）
/// - **泛型参数**：`S` 表示被包裹的服务，实现必须满足 `Service<Request>`；
/// - **返回值**：`Self::Service` 与 `S` 在响应、错误类型上保持一致。
///
/// # 风险提示（Trade-offs）
/// - Layer 组合顺序可能影响语义（例如先限流再重试 vs. 先重试再限流），需要在文档中显式声明；
/// - 若 Layer 内部需要共享状态，推荐通过 `Arc` 等结构注入，避免生命周期复杂度。
pub trait Layer<S, Request>: Sealed
where
    S: Service<Request>,
{
    /// 包裹后的服务类型。
    type Service: Service<Request, Response = S::Response, Error = S::Error>;

    /// 应用中间件并返回新服务实例。
    fn layer(&self, inner: S) -> Self::Service;
}
