//! 运行时语法糖模块，聚焦在不破坏显式契约的前提下降低调用样板。
//!
//! # 模块定位（Why）
//! - 框架要求在跨组件调用时显式传递 [`CallContext`](crate::contract::CallContext) 以确保取消/截止/预算三元组不丢失。
//! - 业务代码在实际操作中仍需频繁地将上下文与运行时能力组合，容易出现重复样板或遗漏克隆。
//! - 本模块提供轻量包装与宏辅助，统一上下文与运行时的绑定方式，同时保留原有契约接口。
//!
//! # 提供能力（How）
//! - [`CallContext`]：以零拷贝方式将 `CallContext` 与运行时能力视图绑在一起；
//! - [`spawn_in`]：基于语法糖上下文调用 [`TaskExecutor::spawn`]，自动回填 Join 句柄类型；
//! - [`crate::with_ctx!`] 宏：在事件驱动上下文中快速获得语法糖上下文，便于直接调用 [`spawn_in`];
//! - [`RuntimeCaps`]：统一抽象“可提交任务的运行时能力集合”，支持 [`CoreServices`](crate::runtime::CoreServices)、自定义执行器或 Pipeline [`pipeline::Context`]
//!   对象。
//!
//! # 使用契约（What）
//! - 模块仅提供包装，不会隐式缓存全局状态；调用方仍需根据业务语义显式克隆或派生子上下文；
//! - `RuntimeCaps` 实现者应保证底层执行器在任务提交期间有效，且返回的 Join 句柄遵循 [`TaskHandle`](crate::runtime::TaskHandle) 契约；
//! - `with_ctx!` 只在宏作用域内重绑定变量名，不会泄露到外层，避免破坏原始上下文所有权。
//!
//! # 风险提示（Trade-offs & Gotchas）
//! - 若运行时实现返回自定义 Join 句柄，可在 [`RuntimeCaps::spawn_with`] 中自行转换；默认返回 [`crate::runtime::JoinHandle`]；
//! - 语法糖上下文在 `async move` 闭包中使用时，需要显式调用 [`CallContext::clone_call`] 将上下文克隆为拥有所有权的版本；
//! - `with_ctx!` 假定传入对象实现 [`crate::pipeline::Context`]，若用于其它类型请手动构造 [`CallContext`]。

use alloc::{borrow::Cow, boxed::Box};
use core::{any::Any, future::Future};

use crate::{
    contract, pipeline,
    runtime::{self, JoinHandle, TaskError, TaskExecutor},
};

/// `RuntimeCaps` 描述“可以提交任务”的运行时能力集合。
///
/// # 设计动机（Why）
/// - 运行时代码可能来自宿主提供的 `AsyncRuntime`、Pipeline `Context` 或测试桩实现；
///   统一的 trait 能避免在语法糖中重复编写样板。
/// - `spawn_with` 将“取出执行器 + 提交任务 + 获得句柄”这组操作抽象为一次调用，
///   让上层 API 专注于业务逻辑。
///
/// # 契约说明（What）
/// - **前置条件**：实现者需保证在 `spawn_with` 调用期间运行时处于可用状态；
/// - **返回值**：`Join<T>` 应满足 `Send + 'static`，并遵循 [`TaskHandle`](runtime::TaskHandle) 契约。
///
/// # 风险提示（Trade-offs）
/// - Trait 不会自动克隆 [`CallContext`](contract::CallContext)；若 Future 需要所有权请显式调用
///   [`CallContext::clone_call`](CallContext::clone_call)；
/// - 若运行时返回自定义句柄，请在实现文档中说明与 `JoinHandle` 的差异，避免误用。
pub trait RuntimeCaps {
    /// 运行时返回的 Join 句柄类型。
    type Join<T: Send + 'static>: Send + 'static;

    /// 在给定上下文与 Future 的情况下提交任务。
    fn spawn_with<F>(&self, ctx: &contract::CallContext, fut: F) -> Self::Join<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}

/// 对任意实现 [`TaskExecutor`] 且满足 `Sized` 的类型提供默认实现。
impl<T> RuntimeCaps for T
where
    T: TaskExecutor,
{
    type Join<U: Send + 'static> = JoinHandle<U>;

    fn spawn_with<F>(&self, ctx: &contract::CallContext, fut: F) -> Self::Join<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        TaskExecutor::spawn(self, ctx, fut)
    }
}

/// `PipelineContextCaps` 将 Pipeline [`Context`](pipeline::Context) 适配为 [`RuntimeCaps`]。
#[derive(Clone, Copy)]
pub struct PipelineContextCaps<'a, C: pipeline::Context + ?Sized> {
    context: &'a C,
}

impl<'a, C: pipeline::Context + ?Sized> PipelineContextCaps<'a, C> {
    /// 构造基于 Pipeline `Context` 的运行时能力视图。
    pub fn new(context: &'a C) -> Self {
        Self { context }
    }

    /// 返回底层 Pipeline `Context` 引用，便于访问其它控制面能力。
    pub fn context(&self) -> &'a C {
        self.context
    }
}

impl<'a, C> RuntimeCaps for PipelineContextCaps<'a, C>
where
    C: pipeline::Context + ?Sized,
{
    type Join<T: Send + 'static> = JoinHandle<T>;

    fn spawn_with<F>(&self, ctx: &contract::CallContext, fut: F) -> Self::Join<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let erased = async move {
            let value = fut.await;
            Ok::<Box<dyn Any + Send>, TaskError>(Box::new(value))
        };
        let handle = self.context.executor().spawn_dyn(ctx, Box::pin(erased));
        map_join(handle)
    }
}

/// `BorrowedRuntimeCaps` 兼容层：保留旧版 API，帮助现有代码在迁移到“直接使用引用”模式前保持可编译。
///
/// # 设计背景（Why）
/// - 历史上语法糖模块通过显式包装类型暴露借用运行时能力；
/// - 为照顾仍依赖 `BorrowedRuntimeCaps::new` 的调用方，本结构体继续存在，但内部实现仅简单转发到引用实现。
///
/// # 契约说明（What）
/// - **输入**：`new` 接收实现 [`RuntimeCaps`] 的对象引用；
/// - **输出**：实现 [`RuntimeCaps`] 的轻量包装，调用行为等价于直接使用原始引用；
/// - **前置条件**：引用生命周期需覆盖任务提交过程；
/// - **后置条件**：`spawn_with` 保证仍以显式 [`CallContext`] 参入运行时。
///
/// # 权衡与风险（Trade-offs & Gotchas）
/// - 推荐新代码直接依赖 `&R`，减少一层类型包装；
/// - 若未来确认无调用方依赖，可在大版本中移除此兼容结构。
#[derive(Clone, Copy)]
pub struct BorrowedRuntimeCaps<'a, R: RuntimeCaps + ?Sized> {
    inner: &'a R,
}

impl<'a, R: RuntimeCaps + ?Sized> BorrowedRuntimeCaps<'a, R> {
    /// 构造引用包装，兼容旧版 API。
    pub fn new(inner: &'a R) -> Self {
        Self { inner }
    }

    /// 返回底层运行时能力引用，便于访问宿主能力。
    pub fn inner(&self) -> &'a R {
        self.inner
    }
}

impl<'a, R: RuntimeCaps + ?Sized> RuntimeCaps for BorrowedRuntimeCaps<'a, R> {
    type Join<T: Send + 'static> = R::Join<T>;

    fn spawn_with<F>(&self, ctx: &contract::CallContext, fut: F) -> Self::Join<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.spawn_with(ctx, fut)
    }
}

/// `CallContext` 语法糖视图，将 [`contract::CallContext`] 与 [`RuntimeCaps`] 绑定在一起。
pub struct CallContext<'a, R: RuntimeCaps> {
    call: &'a contract::CallContext,
    runtime_caps: R,
}

impl<'a, R: RuntimeCaps> CallContext<'a, R> {
    /// 构造语法糖上下文视图。
    pub fn new(call: &'a contract::CallContext, runtime_caps: R) -> Self {
        Self { call, runtime_caps }
    }

    /// 返回底层调用上下文引用。
    pub fn call(&self) -> &'a contract::CallContext {
        self.call
    }

    /// 克隆底层上下文，获取拥有所有权的副本。
    pub fn clone_call(&self) -> contract::CallContext {
        self.call.clone()
    }

    /// 访问运行时能力对象。
    pub fn runtime_caps(&self) -> &R {
        &self.runtime_caps
    }

    /// 拆分结构，返回底层引用与运行时能力。
    pub fn into_parts(self) -> (&'a contract::CallContext, R) {
        (self.call, self.runtime_caps)
    }

    /// 基于引用构造语法糖上下文。
    pub fn borrowed<RC>(
        call: &'a contract::CallContext,
        runtime_caps: &'a RC,
    ) -> CallContext<'a, BorrowedRuntimeCaps<'a, RC>>
    where
        RC: RuntimeCaps + ?Sized,
    {
        CallContext::new(call, BorrowedRuntimeCaps::new(runtime_caps))
    }
}

/// 基于语法糖上下文提交任务，返回运行时对应的 Join 句柄。
pub fn spawn_in<'a, R, F>(ctx: &CallContext<'a, R>, fut: F) -> R::Join<F::Output>
where
    R: RuntimeCaps,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    ctx.runtime_caps.spawn_with(ctx.call, fut)
}

/// 让 [`runtime::CoreServices`] 直接具备 `RuntimeCaps` 能力，方便在依赖注入容器上调用语法糖。
impl RuntimeCaps for runtime::CoreServices {
    type Join<T: Send + 'static> = JoinHandle<T>;

    fn spawn_with<F>(&self, ctx: &contract::CallContext, fut: F) -> Self::Join<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let erased = async move {
            let value = fut.await;
            Ok::<Box<dyn Any + Send>, TaskError>(Box::new(value))
        };
        let handle = self.runtime().spawn_dyn(ctx, Box::pin(erased));
        map_join(handle)
    }
}

fn map_join<T>(handle: JoinHandle<Box<dyn Any + Send>>) -> JoinHandle<T>
where
    T: Send + 'static,
{
    handle.map(|result| {
        result.and_then(|boxed| {
            boxed
                .downcast::<T>()
                .map(|value| *value)
                .map_err(|_| TaskError::Failed(Cow::from("join handle type mismatch")))
        })
    })
}
