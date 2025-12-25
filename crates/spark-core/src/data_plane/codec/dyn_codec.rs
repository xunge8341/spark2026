use alloc::{boxed::Box, format};
use core::any::Any;

use crate::buffer::ErasedSparkBuf;
use crate::codec::contract::Codec;
use crate::codec::decoder::{DecodeContext, DecodeOutcome};
use crate::codec::encoder::{EncodeContext, EncodedPayload};
use crate::codec::metadata::CodecDescriptor;
use crate::error::codes;
use crate::{CoreError, sealed::Sealed};

/// `DynCodec` 为对象层提供编解码能力的对象安全接口。
///
/// # 设计初衷（Why）
/// - 适配运行时编解码注册中心，需要存放多种实现的 trait 对象；
/// - 支撑插件系统与脚本语言在不知道具体泛型的情况下完成编解码；
/// - 与泛型 [`Codec`] 在功能上保持等价，差异仅在于类型擦除与运行时检查。
///
/// # 行为逻辑（How）
/// - `encode_dyn` 接收 `Any` 引用，尝试下转型为具体的 `Outgoing`；
/// - `decode_dyn` 将解码结果打包为 `Box<dyn Any + Send + Sync>`，供上层再做类型还原；
/// - 所有方法返回 [`CoreError`]，用于表达类型不匹配或协议错误。
///
/// # 契约说明（What）
/// - **前置条件**：调用方必须保证传入的 `Any` 与目标类型一致，否则将得到类型不匹配错误；
/// - **后置条件**：成功解码后，上层需按照双方约定的类型信息进行 `downcast`；
/// - **性能权衡**：相较泛型层，额外引入一次虚表跳转与一次堆分配。
pub trait DynCodec: Send + Sync + 'static + Sealed {
    /// 返回编解码器描述符。
    fn descriptor(&self) -> &CodecDescriptor;

    /// 对象安全的编码入口。
    #[allow(unused_parens)]
    fn encode_dyn(
        &self,
        item: &(dyn Any + Send + Sync),
        ctx: &mut EncodeContext<'_>,
    ) -> crate::Result<EncodedPayload, CoreError>;

    /// 对象安全的解码入口。
    fn decode_dyn(
        &self,
        src: &mut ErasedSparkBuf,
        ctx: &mut DecodeContext<'_>,
    ) -> crate::Result<DecodeOutcome<Box<dyn Any + Send + Sync>>, CoreError>;
}

/// `TypedCodecAdapter` 将泛型 [`Codec`] 装箱为对象安全的 [`DynCodec`]。
///
/// # 设计初衷（Why）
/// - 延续 Netty `MessageToMessageCodec` / Tower `Layer` 在泛型与对象层之间的桥接模式；
/// - 让编解码注册中心可以同时管理泛型实现与对象实现。
///
/// # 行为逻辑（How）
/// - 内部持有具体的泛型实现；
/// - `encode_dyn` 使用 `Any::downcast_ref` 做类型还原；
/// - `decode_dyn` 将泛型结果重新装箱，供调用方恢复为原始类型。
///
/// # 契约说明（What）
/// - **前置条件**：调用方需传入正确的 `Outgoing` 类型；
/// - **后置条件**：解码成功后返回的 `Any` 必须由调用方再度下转型；
/// - 类型不匹配时返回 `CoreError::new(codes::PROTOCOL_TYPE_MISMATCH, ..)`。
///
/// # 风险提示（Trade-offs）
/// - 每次调用都涉及一次 `downcast` 检查；若处于热路径，推荐直接使用泛型 [`Codec`]。
pub struct TypedCodecAdapter<C>
where
    C: Codec,
{
    inner: C,
}

impl<C> TypedCodecAdapter<C>
where
    C: Codec,
{
    /// 使用给定的泛型实现构造适配器。
    pub fn new(inner: C) -> Self {
        Self { inner }
    }

    /// 取回内部泛型实现。
    pub fn into_inner(self) -> C {
        self.inner
    }
}

impl<C> DynCodec for TypedCodecAdapter<C>
where
    C: Codec,
{
    fn descriptor(&self) -> &CodecDescriptor {
        self.inner.descriptor()
    }

    #[allow(unused_parens)]
    fn encode_dyn(
        &self,
        item: &(dyn Any + Send + Sync),
        ctx: &mut EncodeContext<'_>,
    ) -> crate::Result<EncodedPayload, CoreError> {
        match item.downcast_ref::<C::Outgoing>() {
            Some(typed) => self.inner.encode(typed, ctx),
            None => Err(CoreError::new(
                codes::PROTOCOL_TYPE_MISMATCH,
                format!(
                    "期待类型 `{}`，实际收到不兼容类型",
                    core::any::type_name::<C::Outgoing>(),
                ),
            )),
        }
    }

    fn decode_dyn(
        &self,
        src: &mut ErasedSparkBuf,
        ctx: &mut DecodeContext<'_>,
    ) -> crate::Result<DecodeOutcome<Box<dyn Any + Send + Sync>>, CoreError> {
        match self.inner.decode(src, ctx)? {
            DecodeOutcome::Complete(item) => Ok(DecodeOutcome::Complete(Box::new(item))),
            DecodeOutcome::Incomplete => Ok(DecodeOutcome::Incomplete),
            DecodeOutcome::Skipped => Ok(DecodeOutcome::Skipped),
        }
    }
}
