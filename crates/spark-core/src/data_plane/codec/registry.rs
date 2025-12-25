use alloc::{boxed::Box, vec::Vec};
use core::marker::PhantomData;

use crate::{CoreError, sealed::Sealed};

use super::contract::Codec;
use super::dyn_codec::{DynCodec, TypedCodecAdapter};
use super::metadata::{CodecDescriptor, ContentEncoding, ContentType};

/// `NegotiatedCodec` 描述一次内容协商的结果。
///
/// # 设计背景（Why）
/// - 参考 HTTP `Accept`/`Content-Type` 与 gRPC `content-subtype` 的握手语义，需记录最终选择及备选方案；
/// - 作为泛型/对象双层的共享数据结构，为 `CodecRegistry` 与插件系统提供统一快照。
///
/// # 行为逻辑（How）
/// - `new` 以最终描述符初始化；
/// - `with_fallback_encodings` 记录按优先级排列的备选压缩算法；
/// - `descriptor`/`fallback_encodings` 分别提供读取接口。
///
/// # 契约说明（What）
/// - **前置条件**：备选列表需按照客户端优先级排序；
/// - **后置条件**：结构体可安全克隆、跨线程传递。
#[derive(Clone, Debug)]
pub struct NegotiatedCodec {
    descriptor: CodecDescriptor,
    fallback_encodings: Vec<ContentEncoding>,
}

impl NegotiatedCodec {
    /// 创建新的协商结果。
    pub fn new(descriptor: CodecDescriptor) -> Self {
        Self {
            descriptor,
            fallback_encodings: Vec::new(),
        }
    }

    /// 指定备选编码列表。
    pub fn with_fallback_encodings(mut self, fallback: Vec<ContentEncoding>) -> Self {
        self.fallback_encodings = fallback;
        self
    }

    /// 获取最终描述符。
    pub fn descriptor(&self) -> &CodecDescriptor {
        &self.descriptor
    }

    /// 访问备选编码列表。
    pub fn fallback_encodings(&self) -> &[ContentEncoding] {
        &self.fallback_encodings
    }
}

/// `DynCodecFactory` 定义对象层的编解码实例构建契约。
///
/// # 设计初衷（Why）
/// - 适配运行时注册中心，通过 trait 对象动态生成编解码实例；
/// - 使内容协商结果能够被直接用于插件化场景，无需了解泛型类型。
///
/// # 契约说明（What）
/// - **前置条件**：`instantiate` 接收的 [`NegotiatedCodec`] 必须与工厂描述符兼容，否则返回带错误码的 [`CoreError`]；
/// - **后置条件**：成功返回的 [`DynCodec`] 可立即投入使用，并满足线程安全要求。
pub trait DynCodecFactory: Send + Sync + 'static + Sealed {
    /// 获取工厂支持的描述符。
    fn descriptor(&self) -> &CodecDescriptor;

    /// 基于协商结果创建对象层编解码实例。
    fn instantiate(
        &self,
        negotiated: &NegotiatedCodec,
    ) -> crate::Result<Box<dyn DynCodec>, CoreError>;
}

/// `TypedCodecFactory` 将返回泛型 [`Codec`] 的构造器包装为 [`DynCodecFactory`]。
///
/// # 设计初衷（Why）
/// - 支持在泛型层实现具体逻辑，同时通过适配器复用到对象层；
/// - 保持 T05“互转/适配器”要求的清晰边界：泛型→对象通过 [`TypedCodecAdapter`]，对象层仅依赖最小集合。
///
/// # 行为逻辑（How）
/// - 保存描述符与构造闭包；
/// - `instantiate` 调用闭包生成泛型实现，再包装为 [`DynCodec`]。
///
/// # 风险提示（Trade-offs）
/// - 若闭包捕获状态，请确保满足 `Send + Sync + 'static` 要求，避免破坏线程安全。
pub struct TypedCodecFactory<C, F>
where
    C: Codec,
    F: Fn(&NegotiatedCodec) -> C + Send + Sync + 'static,
{
    descriptor: CodecDescriptor,
    constructor: F,
    _marker: PhantomData<C>,
}

impl<C, F> TypedCodecFactory<C, F>
where
    C: Codec,
    F: Fn(&NegotiatedCodec) -> C + Send + Sync + 'static,
{
    /// 基于描述符与构造闭包创建工厂。
    pub fn new(descriptor: CodecDescriptor, constructor: F) -> Self {
        Self {
            descriptor,
            constructor,
            _marker: PhantomData,
        }
    }
}

impl<C, F> DynCodecFactory for TypedCodecFactory<C, F>
where
    C: Codec,
    F: Fn(&NegotiatedCodec) -> C + Send + Sync + 'static,
{
    fn descriptor(&self) -> &CodecDescriptor {
        &self.descriptor
    }

    fn instantiate(
        &self,
        negotiated: &NegotiatedCodec,
    ) -> crate::Result<Box<dyn DynCodec>, CoreError> {
        let codec = (self.constructor)(negotiated);
        Ok(Box::new(TypedCodecAdapter::new(codec)))
    }
}

/// `CodecRegistry` 约束编解码注册中心的核心能力。
///
/// # 设计初衷（Why）
/// - 抽象出注册、协商、实例化三项职责，便于内存实现与分布式实现共享；
/// - 输出 [`NegotiatedCodec`] 快照，使泛型层与对象层调用者都能重用相同结构。
///
/// # 线程安全与生命周期约束
/// - Trait 继承 `Send + Sync + 'static`，以支撑运行时在多线程/多实例间共享注册中心；
/// - `register`/`register_static` 提供拥有型与借用型两种入口：
///   - 拥有型用于动态加载插件时直接转移工厂所有权；
///   - 借用型适合复用进程级单例，避免重复装箱并明确声明 `'static` 生命周期要求。
///
/// # 契约说明（What）
/// - **前置条件**：`preferred` 与 `accepted_encodings` 应按客户端优先级排序；
/// - **后置条件**：协商成功返回的快照可跨线程共享；
/// - **错误语义**：若协商失败或缺失实现，必须返回携带标准错误码的 [`CoreError`]。
pub trait CodecRegistry: Send + Sync + 'static + Sealed {
    /// 注册新的对象层编解码工厂。
    fn register(&self, factory: Box<dyn DynCodecFactory>) -> crate::Result<(), CoreError>;

    /// 注册 `'static` 生命周期的对象层编解码工厂。
    fn register_static(
        &self,
        factory: &'static dyn DynCodecFactory,
    ) -> crate::Result<(), CoreError> {
        self.register(box_dyn_codec_factory_from_static(factory))
    }

    /// 根据客户端偏好协商最终方案。
    fn negotiate(
        &self,
        preferred: &[ContentType],
        accepted_encodings: &[ContentEncoding],
    ) -> crate::Result<NegotiatedCodec, CoreError>;

    /// 基于协商结果实例化编解码器。
    fn instantiate(
        &self,
        negotiated: &NegotiatedCodec,
    ) -> crate::Result<Box<dyn DynCodec>, CoreError>;
}

/// 将 `'static` DynCodecFactory 引用适配为拥有型 Box，便于在借用入口与原有 API 之间复用。
fn box_dyn_codec_factory_from_static(
    factory: &'static dyn DynCodecFactory,
) -> Box<dyn DynCodecFactory> {
    Box::new(BorrowedDynCodecFactory { inner: factory })
}

struct BorrowedDynCodecFactory {
    inner: &'static dyn DynCodecFactory,
}

impl DynCodecFactory for BorrowedDynCodecFactory {
    fn descriptor(&self) -> &CodecDescriptor {
        self.inner.descriptor()
    }

    fn instantiate(
        &self,
        negotiated: &NegotiatedCodec,
    ) -> crate::Result<Box<dyn DynCodec>, CoreError> {
        self.inner.instantiate(negotiated)
    }
}
