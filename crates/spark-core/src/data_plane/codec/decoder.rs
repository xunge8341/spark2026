use super::frame_constraints::{FrameConstraints, FrameGuard, FrameRole};
use super::metadata::CodecDescriptor;
use crate::buffer::{BufferAllocator, ErasedSparkBuf, ErasedSparkBufMut};
use crate::contract::Budget;
use crate::{CoreError, sealed::Sealed};
use alloc::boxed::Box;
use core::{fmt, num::NonZeroU16};

/// `DecodeContext` 为增量解码提供辅助资源。
///
/// # 设计背景（Why）
/// - 结合 gRPC streaming、QUIC 帧解析与 Kafka Fetch API 设计，解码过程往往需要临时缓冲或预算信息，统一上下文便于适配多协议。
/// - 在 `no_std` 环境下，避免直接依赖全局分配，保障在嵌入式或多租户场景稳定运行。
///
/// # 逻辑解析（How）
/// - `new` 接收缓冲分配器；`with_max_frame_size` 提供帧预算，防止畸形包攻击。
/// - `acquire_scratch` 允许解码器申请可写缓冲，常用于重组分片或执行临时拷贝。
/// - `max_frame_size` 帮助实现根据预算提前拒绝或调整策略。
/// - 内部通过共享的 `FrameConstraints` 统一帧预算、递归深度与预算退款逻辑，和编码端保持一套实现，减少双写风险。
///
/// # 契约说明（What）
/// - **前置条件**：分配器返回的缓冲区必须满足 `Send + Sync`，以保证解码器在多线程环境安全复用。
/// - **后置条件**：上下文本身不保存状态，调用方可在多次解码之间重复使用同一实例。
///
/// # 风险提示（Trade-offs）
/// - 仅提供最小约束；若需要统计信息（如包计数、速率），应在更高层扩展。
pub struct DecodeContext<'a> {
    allocator: &'a dyn BufferAllocator,
    constraints: FrameConstraints<'a>,
}

impl<'a> DecodeContext<'a> {
    /// 使用分配器构建解码上下文。
    pub fn new(allocator: &'a dyn BufferAllocator) -> Self {
        Self {
            allocator,
            constraints: FrameConstraints::empty(),
        }
    }

    /// 附加可选帧预算信息。
    pub fn with_max_frame_size(
        allocator: &'a dyn BufferAllocator,
        max_frame_size: Option<usize>,
    ) -> Self {
        Self {
            allocator,
            constraints: FrameConstraints::with_frame_size(max_frame_size),
        }
    }

    /// 构造同时包含预算与嵌套深度上限的解码上下文。
    ///
    /// # 教案式说明
    /// - **动机 (Why)**：
    ///   - 入站攻击往往通过畸形帧长度或深度递归消耗 CPU/内存；
    ///   - 将所有限制统一交给上下文，减轻具体解码器的防御负担。
    /// - **流程 (How)**：
    ///   1. 保存分配器引用；
    ///   2. 记录可选预算 `budget`，供 [`Self::check_frame_constraints`] 扣减；
    ///   3. 配置最大帧长与最大递归层数；
    ///   4. `current_depth` 从零开始，配合 [`Self::enter_frame`] 自动递增/递减。
    /// - **契约 (What)**：
    ///   - `budget` 引用必须在上下文生命周期内有效；
    ///   - `max_frame_size` 为字节单位；
    ///   - `max_recursion_depth` 使用 `NonZeroU16` 表示硬上限；
    ///   - **前置条件**：无特别约束；
    ///   - **后置条件**：构造后上下文尚未消费任何预算，也未进入任何层级。
    /// - **权衡 (Trade-offs)**：
    ///   - 通过单一入口降低调用方出错概率；
    ///   - 若未来需要多预算支持，可在此扩展额外字段。
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

    /// 租借一块临时可写缓冲，用于重组片段或复制数据。
    pub fn acquire_scratch(
        &self,
        min_capacity: usize,
    ) -> crate::Result<Box<ErasedSparkBufMut>, CoreError> {
        self.allocator.acquire(min_capacity)
    }

    /// 返回最大帧预算。
    pub fn max_frame_size(&self) -> Option<usize> {
        self.constraints.max_frame_size()
    }

    /// 返回绑定的预算引用，便于上层进行观测或手动扣减。
    pub fn budget(&self) -> Option<&'a Budget> {
        self.constraints.budget()
    }

    /// 返回可选的最大递归深度限制。
    pub fn max_recursion_depth(&self) -> Option<NonZeroU16> {
        self.constraints.max_recursion_depth()
    }

    /// 查询当前已进入的嵌套层级。
    pub fn current_depth(&self) -> u16 {
        self.constraints.current_depth()
    }

    /// 检查输入帧长度是否满足帧长与预算约束，并在成功时扣减预算。
    ///
    /// # 教案式拆解
    /// - **意图 (Why)**：防止解码阶段被超长帧、极端频率的输入耗尽资源。
    /// - **操作步骤 (How)**：
    ///   1. 比较 `max_frame_size`；
    ///   2. 若存在预算，将长度转换为 `u64` 并尝试消费；
    ///   3. 返回 `Ok(())` 表示预算已扣减；`Err` 则包含剩余额度快照。
    /// - **契约 (What)**：
    ///   - **输入**：`frame_len` 为即将解析的字节数；
    ///   - **输出**：成功时预算减少，失败时预算保持原值；
    ///   - **前置条件**：每个帧仅调用一次，避免重复扣减；
    ///   - **后置条件**：错误返回附带 `protocol.budget_exceeded`，供上层快速拒绝连接。
    /// - **风险提示 (Trade-offs)**：
    ///   - 在自定义解码器需要多次读取同一帧时，建议结合 [`Self::refund_budget`] 做差量扣减。
    pub fn check_frame_constraints(&self, frame_len: usize) -> crate::Result<(), CoreError> {
        self.constraints.check_frame(frame_len, FrameRole::Decoder)
    }

    /// 进入一层新的解码递归，返回 RAII 守卫用于自动回退深度计数。
    ///
    /// # 教案式讲解
    /// - **目的 (Why)**：阻断恶意构造的深层嵌套结构导致的堆栈/CPU 攻击。
    /// - **步骤 (How)**：
    ///   1. 若配置了最大深度且即将超过，返回 `protocol.budget_exceeded`；
    ///   2. 使用 `checked_add` 防止计数溢出；
    ///   3. 返回 [`DecodeFrameGuard`]，其析构负责 `current_depth -= 1`。
    /// - **契约 (What)**：
    ///   - 守卫必须在离开当前层级后自动析构；
    ///   - 不提供手动 `drop` 之外的 API，避免错用；
    ///   - **前置条件**：无；**后置条件**：成功进入后 `current_depth` 增加 1。
    /// - **注意事项 (Trade-offs)**：
    ///   - 若调用方使用 `mem::forget` 刻意泄漏守卫，会导致深度永远不回退，应避免此类操作。
    pub fn enter_frame(&mut self) -> crate::Result<DecodeFrameGuard<'_, 'a>, CoreError> {
        Ok(DecodeFrameGuard(
            self.constraints.enter_frame(FrameRole::Decoder)?,
        ))
    }

    /// 在解码失败或需要回滚时归还此前扣减的预算。
    ///
    /// # 教案式提示
    /// - **场景 (Why)**：部分协议需要先读取长度字段再验证校验和，若校验失败需退还预算以便继续处理其他流量。
    /// - **实现 (How)**：若存在预算且 `amount` 能安全转为 `u64`，调用 [`Budget::refund`]；否则忽略。
    /// - **契约 (What)**：方法无返回值，确保回滚路径不会因错误阻塞；调用频次不限，退款累加至预算上限。
    /// - **风险 (Trade-offs)**：超大 `amount` 被静默忽略，需要上层结合日志观察异常模式。
    pub fn refund_budget(&self, amount: usize) {
        self.constraints.refund_budget(amount);
    }
}

impl fmt::Debug for DecodeContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DecodeContext")
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

/// 解码阶段的递归守卫，保证离开作用域时深度计数自动回退。
///
/// # 教案级注释
/// - **Why**：防止实现者忘记手动回退深度导致后续帧被错误拒绝。
/// - **How**：封装共享的 `FrameGuard` 并持有 [`DecodeContext`] 的可变借用，`Drop` 时执行 `saturating_sub(1)`。
/// - **What**：只能通过 [`DecodeContext::enter_frame`] 获取；守卫自身不暴露额外状态。
/// - **Trade-offs**：若调用者刻意 `mem::forget`，将造成深度泄漏——这是故意允许的“逃生舱”，但必须谨慎使用。
#[must_use = "guard drops to release decoder recursion depth"]
pub struct DecodeFrameGuard<'ctx, 'a>(FrameGuard<'ctx, 'a>);

impl Drop for DecodeFrameGuard<'_, '_> {
    fn drop(&mut self) {
        // 访问内部字段以满足 `dead_code` 检查，同时保留共享 `FrameGuard` 的 RAII 行为。
        let _ = &self.0;
    }
}

/// `DecodeOutcome` 表示一次尝试解码的结果状态。
///
/// # 设计背景（Why）
/// - 借鉴 Tokio `Decoder` 的 `Some/None` 语义与 Kafka streaming 的“拉取更多数据”模式，通过枚举明确区分三种常见状态。
/// - 额外提供 `Skipped`，以适配多路复用时对无关帧的快速丢弃需求。
///
/// # 逻辑解析（How）
/// - `Complete(T)`：成功生成一个业务对象；
/// - `Incomplete`：输入不足，调用方应在收到更多字节后重试；
/// - `Skipped`：帧被忽略，通常用于探测包或遥测数据。
///
/// # 契约说明（What）
/// - **前置条件**：解码器必须在消费字节后确保缓冲读指针正确推进；若返回 `Incomplete`，不得提前消费超出需求的字节。
/// - **后置条件**：`Complete` 分支返回的值必须满足业务协议定义。
///
/// # 风险提示（Trade-offs）
/// - 状态枚举未区分可恢复与不可恢复错误；若遇不可解析数据，应直接返回 `CoreError`。
#[derive(Debug)]
#[non_exhaustive]
pub enum DecodeOutcome<T> {
    /// 成功解析出完整对象。
    Complete(T),
    /// 数据不足，等待更多输入。
    Incomplete,
    /// 解码器主动跳过此帧（通常依据业务约定）。
    Skipped,
}

/// `Decoder` 定义将字节流还原为高层对象的契约。
///
/// # 设计背景（Why）
/// - 与 Kafka `Deserializer`、gRPC `Codec` 解码侧保持一致，强调幂等与流式处理能力。
/// - 通过关联类型 `Item` 表示输出对象，确保静态类型安全。
///
/// # 逻辑解析（How）
/// - `descriptor` 与编码端一致，便于校验双方契约。
/// - `decode` 接收可变引用缓冲区，可直接调用 `split_to` 或 `advance` 控制读指针。
///
/// # 契约说明（What）
/// - **前置条件**：调用前应确保缓冲区包含连续字节；若消息跨帧，需要在上层聚合后再调用。
/// - **后置条件**：返回 `Complete` 时，解码器必须已经消费对应字节，避免重复解析；返回 `Incomplete` 时，应保持缓冲状态可回滚。
///
/// # 风险提示（Trade-offs）
/// - 若实现内部维护状态（如分片缓存），需自行处理并发安全，契约未强制 `&mut self`。
pub trait Decoder: Send + Sync + 'static + Sealed {
    /// 解码输出的业务类型。
    type Item: Send + Sync + 'static;

    /// 返回绑定的描述符。
    fn descriptor(&self) -> &CodecDescriptor;

    /// 尝试将字节缓冲解码为高层对象。
    fn decode(
        &self,
        src: &mut ErasedSparkBuf,
        ctx: &mut DecodeContext<'_>,
    ) -> crate::Result<DecodeOutcome<Self::Item>, CoreError>;
}
