use super::frame_constraints::{FrameConstraints, FrameGuard, FrameRole};
use super::metadata::CodecDescriptor;
use crate::buffer::{BufferAllocator, ErasedSparkBuf, ErasedSparkBufMut};
use crate::contract::Budget;
use crate::{CoreError, sealed::Sealed};
use alloc::boxed::Box;
use core::{fmt, num::NonZeroU16};

/// `EncodeContext` 为编码过程提供共享资源视图。
///
/// # 设计背景（Why）
/// - 借鉴 Tokio `Codec` 与 Netty `ChannelHandlerContext` 思路，将缓冲租借、预算控制等能力外置，使编码实现聚焦于序列化细节。
/// - 在无标准库场景下，统一从 `BufferAllocator` 租借可写缓冲，避免直接依赖 `Vec<u8>` 导致内存碎片。
///
/// # 逻辑解析（How）
/// - `new` 仅要求传入分配器；`with_max_frame_size` 允许额外携带帧大小预算，用于限制极端输入。
/// - `acquire_buffer` 是访问分配器的便捷方法，便于实现者撰写无 `unwrap` 的安全代码。
/// - `max_frame_size` 暴露预算，编码器可据此切片或启用分块传输。
/// - 内部借助共享的 `FrameConstraints` 统一处理帧预算、递归深度与预算退款逻辑，确保与解码端行为一致。
///
/// # 契约说明（What）
/// - **前置条件**：分配器必须实现线程安全，且返回的缓冲区满足 `Send + Sync` 要求。
/// - **后置条件**：成功租借的缓冲区由调用方负责归还或冻结为只读缓冲；上下文自身不持有状态。
///
/// # 风险提示（Trade-offs）
/// - 未内置重试逻辑；若租借失败（例如内存池耗尽），编码器需向上返回 `CoreError`，由调用者决定降级或背压。
pub struct EncodeContext<'a> {
    allocator: &'a dyn BufferAllocator,
    constraints: FrameConstraints<'a>,
}

impl<'a> EncodeContext<'a> {
    /// 使用分配器构建上下文。
    pub fn new(allocator: &'a dyn BufferAllocator) -> Self {
        Self {
            allocator,
            constraints: FrameConstraints::empty(),
        }
    }

    /// 附带可选的最大帧尺寸约束。
    pub fn with_max_frame_size(
        allocator: &'a dyn BufferAllocator,
        max_frame_size: Option<usize>,
    ) -> Self {
        Self {
            allocator,
            constraints: FrameConstraints::with_frame_size(max_frame_size),
        }
    }

    /// 构造同时携带预算与深度限制的上下文。
    ///
    /// # 教案式说明
    /// - **目标 (Why)**：编解码通常需要在“长度预算”“嵌套深度”之间协同防御畸形输入。本方法统一注入三类约束，确保上层无需自行拼装。
    /// - **执行 (How)**：
    ///   1. `allocator` 负责后续缓冲租借；
    ///   2. `budget` 若存在，则在 [`Self::check_frame_constraints`] 中按帧长度扣减；
    ///   3. `max_frame_size` 为单帧硬上限；
    ///   4. `max_recursion_depth` 控制 `enter_frame` 可嵌套的层级；
    ///   5. 内部 `current_depth` 自增/自减以实现 RAII 管理。
    /// - **契约 (What)**：
    ///   - `budget`：引用需在上下文生命周期内有效；建议来自 `CallContext` 的 `BudgetKind::Flow` 等限额；
    ///   - `max_frame_size`：单位字节；`None` 表示不设限；
    ///   - `max_recursion_depth`：`None` 表示无限制；`Some` 必须非零；
    ///   - **前置条件**：调用前无需持有任何深度锁；
    ///   - **后置条件**：构造完成后深度计数归零，预算尚未消费。
    /// - **设计考量 (Trade-offs)**：
    ///   - 统一入口避免调用方遗漏其中任一限制；
    ///   - 深度使用 `NonZeroU16` 存储上限，可在 `no_std` 场景保持轻量；
    ///   - 若未来需要多级预算，可在此扩展额外字段，不破坏现有 API。
    pub fn with_limits(
        allocator: &'a dyn BufferAllocator,
        budget: Option<&'a Budget>,
        max_frame_size: Option<usize>,
        max_recursion_depth: Option<NonZeroU16>,
    ) -> Self {
        Self {
            allocator,
            constraints: FrameConstraints::with_limits(budget, max_frame_size, max_recursion_depth),
        }
    }

    /// 租借一个满足最小容量的可写缓冲区。
    pub fn acquire_buffer(
        &self,
        min_capacity: usize,
    ) -> crate::Result<Box<ErasedSparkBufMut>, CoreError> {
        self.allocator.acquire(min_capacity)
    }

    /// 返回最大帧尺寸预算。
    pub fn max_frame_size(&self) -> Option<usize> {
        self.constraints.max_frame_size()
    }

    /// 返回可选的剩余额度引用，方便上层记录指标或自定义扣减逻辑。
    pub fn budget(&self) -> Option<&'a Budget> {
        self.constraints.budget()
    }

    /// 返回当前允许的最大递归深度。
    pub fn max_recursion_depth(&self) -> Option<NonZeroU16> {
        self.constraints.max_recursion_depth()
    }

    /// 查询当前已经进入的嵌套层级。
    pub fn current_depth(&self) -> u16 {
        self.constraints.current_depth()
    }

    /// 校验给定帧长度是否满足帧长与预算限制，并在成功时消费预算。
    ///
    /// # 教案式分解
    /// - **意图 (Why)**：
    ///   - 防御攻击者通过巨型帧或高频帧耗尽内存；
    ///   - 与 `BudgetKind::Flow` 等限额协同，实现精细化背压。
    /// - **逻辑步骤 (How)**：
    ///   1. 若设置了 `max_frame_size`，先比较长度，超过立即返回 `protocol.budget_exceeded`；
    ///   2. 若绑定预算，则将长度安全转换为 `u64`，调用 [`Budget::try_consume`]；
    ///   3. 成功消费返回 `Ok(())`；若预算不足，结合快照生成具备剩余额度信息的错误消息。
    /// - **契约 (What)**：
    ///   - **输入**：`frame_len` 必须是即将写入的字节数；
    ///   - **输出**：成功则表示预算已扣减，调用方无需再次消费；失败保持原始预算；
    ///   - **前置条件**：调用方应避免在同一帧上重复调用，否则会重复扣减；
    ///   - **后置条件**：若返回错误，内部状态（深度、预算）不发生变化。
    /// - **风险提示 (Trade-offs & Gotchas)**：
    ///   - 若调用方需要在失败后继续处理同一帧，需显式调用 [`Self::refund_budget`] 回滚此前的消费。
    pub fn check_frame_constraints(&self, frame_len: usize) -> crate::Result<(), CoreError> {
        self.constraints.check_frame(frame_len, FrameRole::Encoder)
    }

    /// 在帧处理期间尝试进入新的递归层级，返回 RAII 守卫以在离开时自动回退计数。
    ///
    /// # 教案式讲解
    /// - **意图 (Why)**：避免 JSON/Proto 等嵌套结构在极端情况下递归过深，引发栈溢出或 CPU DoS。
    /// - **实现 (How)**：
    ///   1. 若设置上限并已达最大深度，立即返回 `protocol.budget_exceeded`；
    ///   2. 使用 `checked_add` 防止 `u16` 溢出；
    ///   3. 返回 [`EncodeFrameGuard`]，其 `Drop` 会在离开作用域时自动减一。
    /// - **契约 (What)**：
    ///   - **前置条件**：调用方在进入下一层逻辑前必须持有守卫；
    ///   - **后置条件**：无论函数正常返回或 panic，守卫析构都会恢复深度计数；
    ///   - **输入**：无需显式传参，当前上下文自身携带上限信息。
    /// - **注意事项 (Trade-offs)**：
    ///   - `EncodeFrameGuard` 标记为 `#[must_use]`，若立即丢弃则无法真正进入下一层（相当于空操作）。
    pub fn enter_frame(&mut self) -> crate::Result<EncodeFrameGuard<'_, 'a>, CoreError> {
        Ok(EncodeFrameGuard(
            self.constraints.enter_frame(FrameRole::Encoder)?,
        ))
    }

    /// 在发生回滚时归还部分预算。
    ///
    /// # 教案式注解
    /// - **场景 (Why)**：编码器可能在写入部分数据后因业务校验失败而中止，需要返还已扣减的预算避免“泄漏”。
    /// - **做法 (How)**：
    ///   - 尝试将 `amount` 转换为 `u64`；若超出表示数值异常，直接忽略，留待上层处理；
    ///   - 存在预算时调用 [`Budget::refund`]，否则为幂等空操作。
    /// - **契约 (What)**：
    ///   - 仅在之前调用过 [`Self::check_frame_constraints`] 且成功扣减时调用；
    ///   - 方法不返回错误，确保回滚路径不会遮蔽真正的业务错误。
    /// - **风险提示 (Trade-offs)**：超大 `amount` 会被静默忽略，以避免在 fuzz 场景下产生未定义行为。
    pub fn refund_budget(&self, amount: usize) {
        self.constraints.refund_budget(amount);
    }
}

impl fmt::Debug for EncodeContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncodeContext")
            .field("max_frame_size", &self.constraints.max_frame_size())
            .field(
                "budget_kind",
                &self
                    .constraints
                    .budget()
                    .map(|budget| budget.kind().clone()),
            )
            .field(
                "max_recursion_depth",
                &self.constraints.max_recursion_depth(),
            )
            .field("current_depth", &self.constraints.current_depth())
            .finish()
    }
}

/// 编码阶段的递归深度守卫，离开作用域时自动回退深度计数。
///
/// # 教案级注释
/// - **目的 (Why)**：RAII 方式保证无论函数如何返回都能恢复深度，杜绝“借入未归还”导致的深度泄漏。
/// - **工作原理 (How)**：内部封装共享的 `FrameGuard`，`Drop` 时由其自动将 `current_depth` 减 1。
/// - **契约 (What)**：
///   - 仅能通过 [`EncodeContext::enter_frame`] 获取；
///   - 释放顺序与作用域一致，适合嵌套结构；
///   - 不公开额外方法，避免误用。
/// - **边界情况 (Trade-offs)**：若在逻辑中手动调用 `core::mem::forget`，需要确保自行维护计数，否则会造成深度泄漏。
#[must_use = "guard drops to release encoder recursion depth"]
pub struct EncodeFrameGuard<'ctx, 'a>(FrameGuard<'ctx, 'a>);

impl Drop for EncodeFrameGuard<'_, '_> {
    fn drop(&mut self) {
        // 访问内部字段以满足 `dead_code` 检查，同时保持由 `FrameGuard` 管理的 RAII 行为。
        let _ = &self.0;
    }
}

/// `EncodedPayload` 表示编码完成、可沿管道传播的字节缓冲。
///
/// # 设计背景（Why）
/// - 类似 gRPC、NATS 中的帧抽象，编码器需要返回只读视图以便下游传输层直接写出。
/// - 通过持有 `Box<ErasedSparkBuf>`，允许实现者提供自定义引用计数缓冲或零拷贝切片。
///
/// # 逻辑解析（How）
/// - `from_buffer` 接收任何对象安全的缓冲实现。
/// - `into_buffer` 释放所有权，供调用方写入传输层或复用。
///
/// # 契约说明（What）
/// - **前置条件**：传入缓冲必须遵循 `ErasedSparkBuf` 的语义，读指针指向有效负载开头。
/// - **后置条件**：结构体不对缓冲执行额外处理，确保零拷贝传播。
///
/// # 风险提示（Trade-offs）
/// - 不提供部分写入标记；如需分片发送，应在更高层通过 `PipelineMessage::Buf` 的多帧组合实现。
pub struct EncodedPayload {
    buffer: Box<ErasedSparkBuf>,
}

impl EncodedPayload {
    /// 使用已经冻结的只读缓冲创建负载。
    pub fn from_buffer(buffer: Box<ErasedSparkBuf>) -> Self {
        Self { buffer }
    }

    /// 取回底层缓冲。
    ///
    /// # 使用指引
    /// - 常见用法是结合 [`crate::buffer::PipelineMessage::from_encoded_payload`]，确保编码结果以零拷贝方式进入 Pipeline；
    /// - 若需直接交付传输层，也可手动调用此方法再传入 `Channel::write`。
    pub fn into_buffer(self) -> Box<ErasedSparkBuf> {
        self.buffer
    }
}

impl fmt::Debug for EncodedPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncodedPayload")
            .field("remaining", &self.buffer.remaining())
            .finish()
    }
}

/// `Encoder` 定义将高层对象转换为字节流的契约。
///
/// # 设计背景（Why）
/// - 吸收 Netty `MessageToByteEncoder`、Tokio `Encoder` 与 gRPC 拓展点经验，统一了内容描述与上下文依赖。
/// - 通过关联类型 `Item` 表达输出对象，保证在静态类型系统中保持语义清晰。
///
/// # 逻辑解析（How）
/// - `descriptor` 返回编解码描述符，供注册中心或握手阶段使用。
/// - `encode` 负责将业务对象序列化为 `EncodedPayload`，并可根据上下文申请缓冲或检查帧预算。
///
/// # 契约说明（What）
/// - **前置条件**：调用方需确保输入 `item` 满足业务协议约束，例如字段完整性、schema 兼容性。
/// - **后置条件**：返回的 `EncodedPayload` 必须完全代表 `item` 的序列化结果；若发生错误，应通过 `CoreError` 携带稳定错误码。
///
/// # 风险提示（Trade-offs）
/// - 接口未强制 `&mut self`，方便实现者以无状态单例形式注册，但若内部需要缓存，应自行处理并发安全。
pub trait Encoder: Send + Sync + 'static + Sealed {
    /// 编码输入的具体业务类型。
    type Item: Send + Sync + 'static;

    /// 返回与该编码器绑定的描述符。
    fn descriptor(&self) -> &CodecDescriptor;

    /// 将业务对象编码为字节负载。
    fn encode(
        &self,
        item: &Self::Item,
        ctx: &mut EncodeContext<'_>,
    ) -> crate::Result<EncodedPayload, CoreError>;
}
