//! AutoDyn 桥接工具：将实现泛型 [`Service`] 的类型自动转换为对象层 [`BoxService`]。
//!
//! # 设计动机（Why）
//! - 在常见“请求/响应均具备编解码能力”的场景，开发者往往需要手写 `PipelineMessage`
//!   到领域模型的解码闭包，并在返回路径上重复构造编码闭包；
//! - 这些样板代码不仅冗长，且容易在错误分支遗漏错误码或类型提示，降低维护效率；
//! - `AutoDynBridge` 通过统一的 Trait 将泛型层服务桥接为对象层句柄，确保双层 API
//!   之间的语义保持一致，同时复用框架内置的 [`ServiceObject`]。
//!
//! # 模块角色（How）
//! - [`Decode`] 与 [`Encode`] 定义了类型与 [`PipelineMessage`] 之间的往返契约，
//!   通过 blanket 实现与手工实现兼容多种转换策略；
//! - [`AutoDynBridge`] 提供 `into_dyn` 默认方法，一旦请求类型满足 [`Decode`]、响应类型满足
//!   [`Encode`]，即可无感生成 [`BoxService`]；
//! - 桥接过程复用 [`ServiceObject`] 生成虚表，并以 `Arc` 封装，保持线程安全与生命周期要求。
//!
//! # 契约说明（What）
//! - **输入条件**：`Service` 的错误类型必须为 [`SparkError`]，以便对象层统一返回域错误；
//! - **前置条件**：请求类型实现 [`Decode`]，响应类型实现 [`Encode`]，并满足 `Send + Sync + 'static`；
//! - **后置条件**：`into_dyn` 返回的 [`BoxService`] 可直接交由路由、Pipeline 等对象层组件复用；
//! - **风险提示**：若 `Decode`/`Encode` 的实现未覆盖所有消息分支，将在运行时返回结构化错误，
//!   调用方需结合观测系统捕获并修复对应契约。
//!
//! # 教案提示（Trade-offs & Gotchas）
//! - `AutoDynBridge` 生成的闭包为函数指针，避免在热路径捕获多余环境；
//! - `BoxService` 内部持有 `Arc`，若需获得可变引用，可在未克隆前调用
//!   [`BoxService::into_arc`](crate::service::BoxService::into_arc) 并通过 `Arc::get_mut` 获取；
//! - 若请求/响应类型本身就是 [`PipelineMessage`]，可以使用模块提供的 blanket 实现直接桥接。

use alloc::{format, sync::Arc};
use core::{convert::TryFrom, marker::PhantomData, task::Context as TaskContext};

use crate::SparkError;
use crate::buffer::PipelineMessage;
use crate::context::Context;
use crate::contract::CallContext;
use crate::error::codes;
use crate::service::{BoxService, Service, ServiceObject};
use crate::status::PollReady;

/// 描述“从 [`PipelineMessage`] 解码为具体业务类型”的契约。
///
/// # 教案式注释
/// - **意图 (Why)**：抽象请求类型的解码逻辑，避免在 `AutoDynBridge` 中硬编码具体协议；
/// - **位置 (Where)**：位于 `service::auto_dyn`，专供泛型 Service 自动桥接使用；
/// - **实现逻辑 (How)**：调用者可直接实现该 Trait，或复用 blanket 实现（如 `TryFrom<PipelineMessage>`）。
/// - **契约 (What)**：输入为 `PipelineMessage`，返回成功的具体类型或带 `SparkError` 的错误；
/// - **风险与权衡**：实现需确保错误码稳定且语义准确，避免使用裸 `panic!` 造成调用栈崩溃。
pub trait Decode: Sized {
    /// 将 [`PipelineMessage`] 转换为业务类型。
    fn decode(message: PipelineMessage) -> crate::Result<Self, SparkError>;
}

/// 描述“将业务类型编码为 [`PipelineMessage`]”的契约。
///
/// # 教案式注释
/// - **意图 (Why)**：统一响应编码出口，保障所有对象层桥接遵循相同的消息包装语义；
/// - **位置 (Where)**：`service::auto_dyn` 模块，供 `AutoDynBridge` 闭包复用；
/// - **实现逻辑 (How)**：实现者可直接返回 `PipelineMessage::User`/`::Buffer` 等形式；
/// - **契约 (What)**：消费业务对象并生成 `PipelineMessage`；
/// - **风险提示**：若编码结果与路由/传输层预期不符，将在运行时触发协议错误，应在实现中提供明确文案。
pub trait Encode {
    /// 将业务响应转换为 `PipelineMessage`。
    fn encode(self) -> PipelineMessage;
}

/// `AutoDynBridge` 为实现泛型 [`Service`] 的类型提供一键转换到对象层的能力。
///
/// # 教案式注释
/// - **意图 (Why)**：消除在每个 Service 实现旁手写 `ServiceObject::new` 的重复劳动；
/// - **位置 (Where)**：`spark-core::service::auto_dyn` 模块，由宏 `#[spark::service]` 与手写实现共用；
/// - **执行逻辑 (How)**：
///   1. 调用方需在实现中委托给 [`bridge_to_box_service`] 或等价逻辑，完成泛型层到对象层的桥接；
///   2. 利用泛型 `Request` 参数约束 [`Decode`] 与 [`Encode`]，在调用 `into_dyn` 时确保契约满足；
/// - **契约 (What)**：关联类型 [`AutoDynBridge::DynOut`] 指定桥接后的对象层句柄形态；
/// - **前置条件**：仅当请求实现 [`Decode`]、响应实现 [`Encode`]、错误类型等于 [`SparkError`] 时，桥接才可执行；
/// - **后置条件**：成功桥接后获得的 `DynOut` 可直接复用在对象层路由与 Pipeline；
/// - **风险与权衡**：若类型未实现编解码契约，调用 `into_dyn` 将在编译期报错，提醒开发者补齐实现。
pub trait AutoDynBridge: Sized + Send + Sync + 'static {
    /// 桥接后对象层服务的具体类型（常见为 [`BoxService`]）。
    type DynOut;

    /// 将泛型 Service 转换为对象层句柄。
    fn into_dyn<Request>(self) -> Self::DynOut
    where
        Self: Service<Request, Error = SparkError>,
        Request: Decode + Send + Sync + 'static,
        <Self as Service<Request>>::Response: Encode + Send + Sync + 'static;
}

/// DynBridge：为任意泛型 [`Service`] 提供“保持原语义 + 自动桥接”的轻量包装。
///
/// # 教案式注释
/// - **意图 (Why)**：通过包装器集中封装对象层桥接逻辑，使过程宏与手写 Service 可复用同一实现；
/// - **位置 (Where)**：`service::auto_dyn` 模块内部，仅在需要显式声明请求类型时使用；
/// - **执行逻辑 (How)**：
///   1. `DynBridge::new` 以值语义保存原始服务，实现 [`Service`] 时直接转发 `poll_ready` 与 `call`；
///   2. `AutoDynBridge` 实现于 `DynBridge` 上，在 `into_dyn` 调用时复用 [`bridge_to_box_service`]；
/// - **契约 (What)**：包装后仍实现 [`Service`]，关联类型沿用内部服务；
/// - **前置条件**：内部服务需满足 `Send + Sync + 'static`，并在 `into_dyn` 时额外要求 `Decode`/`Encode`；
/// - **后置条件**：调用 `into_dyn` 后获得的 [`BoxService`] 可安全用于对象层 API；
/// - **风险与权衡**：包装器本身不复制内部状态，若需多次消费需在调用方显式克隆或重新构造服务。
pub struct DynBridge<S, Request> {
    /// 实际的泛型层 Service 实现。
    ///
    /// - **职责**：承载真实业务逻辑，`DynBridge` 在 `Service` 实现中直接委托给它；
    /// - **约束**：必须满足 `Send + Sync + 'static`，以便包装器也能安全跨线程移动。
    inner: S,
    /// 编译期标记：记录泛型服务所处理的请求类型。
    ///
    /// - **作用**：使 `DynBridge` 在类型层面携带 `Request` 信息，便于 [`AutoDynBridge`] 实现绑定解码器；
    /// - **实现**：使用零尺寸的 [`PhantomData`]，不会在运行时产生额外存储成本。
    _marker: PhantomData<fn() -> Request>,
}

impl<S, Request> DynBridge<S, Request>
where
    S: Service<Request>,
    Request: Send + Sync + 'static,
{
    /// 创建新的桥接包装器。
    ///
    /// - **输入参数**：`inner` 为原始泛型服务；
    /// - **行为说明**：纯值语义封装，不触发额外分配或初始化逻辑；
    /// - **后置条件**：返回的 `DynBridge` 可立即参与 `Service` 调度或对象层桥接。
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// 取回内部服务。
    ///
    /// - **使用场景**：当调用方希望在桥接前执行额外装饰，或在单元测试中直接断言内部状态。
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, Request> Service<Request> for DynBridge<S, Request>
where
    S: Service<Request>,
    Request: Send + Sync + 'static,
{
    type Response = <S as Service<Request>>::Response;
    type Error = <S as Service<Request>>::Error;
    type Future = <S as Service<Request>>::Future;

    fn poll_ready(
        &mut self,
        ctx: &Context<'_>,
        cx: &mut TaskContext<'_>,
    ) -> PollReady<Self::Error> {
        self.inner.poll_ready(ctx, cx)
    }

    fn call(&mut self, ctx: CallContext, req: Request) -> Self::Future {
        self.inner.call(ctx, req)
    }
}

impl<S, Request> AutoDynBridge for DynBridge<S, Request>
where
    S: Service<Request> + Send + Sync + 'static,
    Request: Send + Sync + 'static,
    <S as Service<Request>>::Response: Send + Sync + 'static,
{
    type DynOut = BoxService;

    fn into_dyn<R>(self) -> Self::DynOut
    where
        Self: Service<R, Error = SparkError>,
        R: Decode + Send + Sync + 'static,
        <Self as Service<R>>::Response: Encode + Send + Sync + 'static,
    {
        bridge_to_box_service::<Self, R>(self)
    }
}

impl AutoDynBridge for BoxService {
    type DynOut = BoxService;

    fn into_dyn<Request>(self) -> Self::DynOut
    where
        Self: Service<Request, Error = SparkError>,
        Request: Decode + Send + Sync + 'static,
        <Self as Service<Request>>::Response: Encode + Send + Sync + 'static,
    {
        self
    }
}

/// 公共桥接函数：复用在默认实现与宏生成代码中，集中封装类型擦除逻辑。
///
/// # 教案式注释
/// - **意图 (Why)**：为手写 Service 提供低样板的桥接入口，避免重复实现 `AutoDynBridge`；
/// - **位置 (Where)**：`service::auto_dyn` 模块对外公开的辅助函数；
/// - **执行逻辑 (How)**：构造 [`ServiceObject`] 并用 [`BoxService::new`] 包装，保持线程安全；
/// - **契约 (What)**：输入泛型 Service，输出对象层 `BoxService`；
/// - **前置条件**：满足 `Decode`/`Encode`/`SparkError` 契约；
/// - **后置条件**：返回的 `BoxService` 可直接注入路由或 Pipeline；
/// - **风险与权衡**：若解码闭包 panic，将导致整个调用栈终止，应在实现中优先返回结构化错误。
pub fn bridge_to_box_service<S, Request>(service: S) -> BoxService
where
    S: Service<Request, Error = SparkError> + Send + Sync + 'static,
    Request: Decode + Send + Sync + 'static,
    <S as Service<Request>>::Response: Encode + Send + Sync + 'static,
{
    let object = ServiceObject::new(
        service,
        Request::decode,
        <<S as Service<Request>>::Response as Encode>::encode,
    );
    BoxService::new(Arc::new(object))
}

impl Decode for PipelineMessage {
    fn decode(message: PipelineMessage) -> crate::Result<Self, SparkError> {
        Ok(message)
    }
}

impl<T> Decode for T
where
    T: TryFrom<PipelineMessage, Error = SparkError>,
{
    fn decode(message: PipelineMessage) -> crate::Result<Self, SparkError> {
        T::try_from(message)
    }
}

impl<T> Encode for T
where
    T: Into<PipelineMessage>,
{
    fn encode(self) -> PipelineMessage {
        self.into()
    }
}

/// 为 `TryFrom<PipelineMessage>` 实现提供统一的类型错误帮助函数。
///
/// - **Why**：简化自定义 `Decode` 实现的错误码书写，保持错误语义一致；
/// - **How**：返回标准的 `protocol.type_mismatch` 域错误；
/// - **What**：供实现者在 `Decode`/`TryFrom` 失败时调用。
pub fn type_mismatch_error(expected: &'static str, actual: &'static str) -> SparkError {
    SparkError::new(
        codes::PROTOCOL_TYPE_MISMATCH,
        format!("expect `{expected}` but received `{actual}` while bridging PipelineMessage"),
    )
}
