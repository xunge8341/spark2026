use crate::buffer::ErasedSparkBuf;
use crate::codec::decoder::{DecodeContext, DecodeOutcome, Decoder as DecoderTrait};
use crate::codec::encoder::{EncodeContext, EncodedPayload, Encoder};
use crate::codec::metadata::CodecDescriptor;
use crate::{CoreError, sealed::Sealed};

/// `Codec` 统一封装编码与解码逻辑，是泛型层的零成本编解码契约。
///
/// # 契约维度速览
/// - **语义**：以单一 Trait 同时描述 `encode`/`decode`，配合 [`CodecDescriptor`] 揭示内容类型、压缩、schema。
/// - **错误**：所有失败返回 [`CoreError`]；常见码值包含 `protocol.decode`、`protocol.encode`、`protocol.type_mismatch`。
/// - **并发**：实现需 `Send + Sync`，允许多个协程并发调用；若内部缓存状态，请使用原子或锁保护。
/// - **背压**：编码/解码上下文暴露预算接口；当 `ctx.budget(BudgetKind::Flow)` 耗尽时，应返回 `CoreError` 并附带 `BusyReason`。
/// - **超时**：上下文中的 `deadline()` 可用于限时编解码；实现应在可能阻塞的流程（压缩/解压）中检查并尽早失败。
/// - **取消**：`DecodeContext`/`EncodeContext` 持有 [`Cancellation`](crate::contract::Cancellation)；长时间操作需定期轮询 `is_cancelled()` 并提前结束。
/// - **观测标签**：统一打点 `codec.name`、`codec.phase`（`encode`/`decode`）、`codec.content_type`，便于跨实现聚合指标。
/// - **示例(伪码)**：
///   ```text
///   ctx = EncodeContext::from(call_ctx)
///   payload = codec.encode(&request, &mut ctx)?
///   transport.write(payload)
///   ```
///
/// # 设计初衷（Why）
/// - 借鉴 `tokio-util::codec::Decoder + Encoder` 的组合模式，以单一 trait 同时表达双向能力；
/// - 通过关联类型区分入站/出站业务对象，保证静态类型安全；
/// - 作为对象层 [`crate::codec::DynCodec`] 的泛型基线，支撑 T05“二层 API”在编解码域的等价实现。
///
/// # 行为逻辑（How）
/// 1. `descriptor` 返回实现所支持的内容类型与压缩信息；
/// 2. `encode` 将业务对象序列化为字节缓冲；
/// 3. `decode` 从缓冲中解析业务对象，并返回统一的 [`DecodeOutcome`]。
///
/// # 契约说明（What）
/// - **关联类型**：`Incoming`/`Outgoing` 均需满足 `Send + Sync + 'static`，以支持跨线程传输；
/// - **输入**：`EncodeContext`/`DecodeContext` 携带预算、观测等约束，上层会在超限时触发熔断；
/// - **前置条件**：调用方必须确保入站缓冲遵循协议约定；
/// - **后置条件**：若返回错误，必须给出语义化 [`CoreError`] 以供上层记录。
///
/// # 风险提示（Trade-offs）
/// - `Codec` 自身不维持状态同步；若实现需要缓存上下文，请在内部保证并发安全；
/// - 在极端性能场景，可结合 Arena/零拷贝缓冲减少复制，仍保持与对象层兼容。
pub trait Codec: Send + Sync + 'static + Sealed {
    /// 解码后的业务类型。
    type Incoming: Send + Sync + 'static;
    /// 编码时的业务类型。
    type Outgoing: Send + Sync + 'static;

    /// 返回编解码器描述符。
    fn descriptor(&self) -> &CodecDescriptor;

    /// 编码业务对象。
    fn encode(
        &self,
        item: &Self::Outgoing,
        ctx: &mut EncodeContext<'_>,
    ) -> crate::Result<EncodedPayload, CoreError>;

    /// 解码字节流。
    fn decode(
        &self,
        src: &mut ErasedSparkBuf,
        ctx: &mut DecodeContext<'_>,
    ) -> crate::Result<DecodeOutcome<Self::Incoming>, CoreError>;
}

impl<T> Encoder for T
where
    T: Codec,
{
    type Item = T::Outgoing;

    fn descriptor(&self) -> &CodecDescriptor {
        Codec::descriptor(self)
    }

    fn encode(
        &self,
        item: &Self::Item,
        ctx: &mut EncodeContext<'_>,
    ) -> crate::Result<EncodedPayload, CoreError> {
        Codec::encode(self, item, ctx)
    }
}

impl<T> DecoderTrait for T
where
    T: Codec,
{
    type Item = T::Incoming;

    fn descriptor(&self) -> &CodecDescriptor {
        Codec::descriptor(self)
    }

    fn decode(
        &self,
        src: &mut ErasedSparkBuf,
        ctx: &mut DecodeContext<'_>,
    ) -> crate::Result<DecodeOutcome<Self::Item>, CoreError> {
        Codec::decode(self, src, ctx)
    }
}
