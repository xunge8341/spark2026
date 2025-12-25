use alloc::{boxed::Box, format, sync::Arc, vec, vec::Vec};
use core::{
    mem,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use spark_core::{
    CoreError, Result,
    buffer::{ReadableBuffer, WritableBuffer},
    error::codes,
};
use spin::Mutex;

/// `BufferRecycler` 描述缓冲池在租借结束时的回收入口。
///
/// # 设计初衷（Why）
/// - `spark-core` 的 [`BufferPool`](spark_core::buffer::BufferPool) trait 仅负责“租借”侧的抽象，
///   不约束具体实现如何在缓冲生命周期结束时归还容量。
/// - 通过补充该回收接口，我们可以在 `PooledBuffer` 的 `Drop` 阶段统一通知池，
///   避免在上层组件中散落自定义的回收逻辑，保持内存使用的可预测性。
///
/// # 使用方式（How）
/// - 缓冲池实现应当将自身或内部的回收句柄封装为 `Arc<dyn BufferRecycler>`，
///   在构造 [`PooledBuffer`] 时一并注入。
/// - 当所有引用同一租约的缓冲全部被释放后，`reclaim` 会被调用一次，
///   实现者可在其中更新统计、归还 slab、或触发后续的异步回收流程。
///
/// # 契约定义（What）
/// - `capacity`：表示此次归还的字节容量，应等于池在租借时实际分配的上限。
/// - **前置条件**：实现必须线程安全，且调用过程中不得 panic，
///   否则 `Drop` 路径上的 panic 将导致进程异常终止。
/// - **后置条件**：成功执行后，池应当已记录该容量的可用性；若归还失败，
///   实现者需自行决定是否重试或将统计信息标记为不一致。
pub trait BufferRecycler: Send + Sync + 'static {
    /// 通知池释放指定容量。
    fn reclaim(&self, reclaimed: ReclaimedBuffer);
}

/// 表示一次回收动作所携带的上下文。
///
/// # 设计动机（Why）
/// - 在零拷贝流水线中，我们不仅需要统计回收容量，还希望尽可能回收原始 `BytesMut`，
///   以避免再次向系统申请内存导致的抖动。
/// - 传统回收接口仅返回 `usize` 容量，难以区分“实际回收到的内存块”与“统计信息更新”，
///   无法支撑复用自由链表（Free List）的需求。
///
/// # 数据结构解析（How）
/// - `capacity`：本次租约的最终容量，保证池侧统计的一致性；
/// - `buffer`：若成功夺回底层 `BytesMut` 所有权，则携带 `Some(BytesMut)`；
///   若因冻结后仍有别名或引用计数未归零，则只能返回 `None`，由池端自行决定是否重新分配。
///
/// # 契约说明（What）
/// - **前置条件**：调用者必须确保 `buffer` 与 `capacity` 对应同一块内存；
/// - **后置条件**：池实现可以据此选择复用内存块或仅更新统计值。
///
/// # 风险提示（Trade-offs）
/// - 若实现始终收到 `None`，说明上层存在持久化 `Bytes` 切片的场景，此时应结合监控
///   调整内存策略或通过压测验证是否需要更激进的回收机制。
#[derive(Debug)]
pub struct ReclaimedBuffer {
    capacity: usize,
    buffer: Option<BytesMut>,
}

impl ReclaimedBuffer {
    /// 创建携带完整上下文的回收结果。
    pub fn new(capacity: usize, buffer: Option<BytesMut>) -> Self {
        Self { capacity, buffer }
    }

    /// 返回本次回收的容量，用于更新统计或回收失败时的降级决策。
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// 消耗结构并返回可复用的 `BytesMut`，若不存在则为 `None`。
    pub fn into_buffer(self) -> Option<BytesMut> {
        self.buffer
    }
}

/// `Lease` 追踪缓冲租借的元数据，并在生命周期结束时触发回收。
///
/// # 角色定位（Why）
/// - 在零拷贝场景下，同一块内存可能被拆分为多个视图（例如 `split_to` 产生的前缀），
///   只有最后一个视图被销毁时才能真正归还容量。
/// - 将回收逻辑放入 `Lease` 的 `Drop` 中，可利用 `Arc` 的引用计数，
///   自动判定“最后一个持有者”并触发池级回收。
///
/// # 结构设计（How）
/// - `recycler`：持有池级回收句柄；
/// - `capacity`：使用原子整数记录当前租约的容量上限，支持动态扩容后更新；
/// - `Drop`：调用 `recycler.reclaim`，确保只执行一次。
struct Lease {
    recycler: Arc<dyn BufferRecycler>,
    capacity: AtomicUsize,
    buffer: Mutex<Option<BytesMut>>,
}

impl Lease {
    /// 构造新的租约元数据。
    fn new(initial_capacity: usize, recycler: Arc<dyn BufferRecycler>) -> Self {
        Self {
            recycler,
            capacity: AtomicUsize::new(initial_capacity),
            buffer: Mutex::new(None),
        }
    }

    /// 更新租约记录的容量，配合动态扩容保持统计一致。
    fn update_capacity(&self, new_capacity: usize) {
        self.capacity.store(new_capacity, Ordering::Relaxed);
    }

    /// 存储尚未释放的 `BytesMut`，供最终的 `Drop` 钩子回收。
    fn store_buffer(&self, buffer: Option<BytesMut>) {
        if let Some(buf) = buffer {
            let mut slot = self.buffer.lock();
            if slot.is_none() {
                *slot = Some(buf);
            }
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let capacity = self.capacity.load(Ordering::Relaxed);
        let buffer = self.buffer.lock().take();
        self.recycler
            .reclaim(ReclaimedBuffer::new(capacity, buffer));
    }
}

/// 表征缓冲当前所处的读写状态。
///
/// - `Writable`：持有 `BytesMut`，可被追加写入并支持冻结为只读视图；
/// - `ReadOnly`：持有 `Bytes`，仅支持读取和拆分，避免误写导致数据竞争。
#[derive(Debug)]
enum BufferState {
    Writable(BytesMut),
    ReadOnly(Bytes),
}

impl BufferState {
    /// 返回当前视图的长度。
    fn len(&self) -> usize {
        match self {
            BufferState::Writable(buf) => buf.len(),
            BufferState::ReadOnly(bytes) => bytes.len(),
        }
    }

    /// 返回当前可直接读取的切片。
    fn chunk(&self) -> &[u8] {
        match self {
            BufferState::Writable(buf) => buf.as_ref(),
            BufferState::ReadOnly(bytes) => bytes.as_ref(),
        }
    }

    /// 将前 `len` 字节拆分为新的只读状态。
    fn split_to(&mut self, len: usize) -> BufferState {
        match self {
            BufferState::Writable(buf) => {
                let split = buf.split_to(len);
                BufferState::ReadOnly(split.freeze())
            }
            BufferState::ReadOnly(bytes) => BufferState::ReadOnly(bytes.split_to(len)),
        }
    }

    /// 丢弃前 `len` 字节。
    fn advance(&mut self, len: usize) {
        match self {
            BufferState::Writable(buf) => buf.advance(len),
            BufferState::ReadOnly(bytes) => bytes.advance(len),
        }
    }

    /// 将内容复制到目标切片。
    fn copy_into_slice(&mut self, dst: &mut [u8]) {
        dst.copy_from_slice(&self.chunk()[..dst.len()]);
        self.advance(dst.len());
    }

    /// 将视图内容扁平化为 Vec。
    /// 复制当前内容为 `Vec<u8>`，不获取所有权。
    fn to_vec(&self) -> Vec<u8> {
        match self {
            BufferState::Writable(buf) => buf.to_vec(),
            BufferState::ReadOnly(bytes) => bytes.to_vec(),
        }
    }

    /// 返回当前剩余可写区域的裸视图。
    ///
    /// # 安全约束说明（Why & Gotchas）
    /// - 仅当状态为 `Writable` 时才会暴露实际切片；调用前必须通过上层的
    ///   `PooledBuffer::is_writable` 或 `require_writable` 完成校验。
    /// - 我们在调试模式下依赖 [`debug_assert!`] 捕捉误用，防止在冻结后仍走写路径导致未定义行为。
    ///
    /// # 逻辑概览（How）
    /// 1. 读取 `BytesMut::chunk_mut` 的 `UninitSlice`；
    /// 2. 通过指针转换为 `&mut [MaybeUninit<u8>]`，保持“尚未初始化”语义；
    /// 3. 该转换不修改所有权，也不延长借用生命周期，符合 `bytes` 文档约束。
    ///
    /// # 契约定义（What）
    /// - **前置条件**：必须持有写态独占访问；若存在其它引用，`bytes` 将返回长度为 0 的切片。
    /// - **后置条件**：返回切片的生命周期与 `&mut self` 一致，调用方需在写入后立刻调用 `advance_mut`。
    fn chunk_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        match self {
            BufferState::Writable(buf) => {
                // bytes::BufMut::chunk_mut 暴露的是 `UninitSlice`，其安全 API 不提供直接
                // 访问 `MaybeUninit<u8>` 的方法。为了与 WritableBuffer 的返回类型对齐，
                // 我们通过指针还原底层切片：
                // 1. 取得切片长度与原始指针；
                // 2. 使用 `from_raw_parts_mut` 将其视作 `MaybeUninit<u8>`；
                // 3. 该转换不越界且保持未初始化区间，满足安全约束。
                let chunk = buf.chunk_mut();
                let len = chunk.len();
                let ptr = chunk.as_mut_ptr().cast::<MaybeUninit<u8>>();
                unsafe { core::slice::from_raw_parts_mut(ptr, len) }
            }
            BufferState::ReadOnly(_) => empty_chunk_mut(),
        }
    }

    /// 推进写指针，告知内部有多少字节被初始化。
    fn advance_mut(&mut self, len: usize) -> Result<(), CoreError> {
        match self {
            BufferState::Writable(buf) => {
                let available = buf.chunk_mut().len();
                if len > available {
                    return Err(CoreError::new(
                        codes::APP_INVALID_ARGUMENT,
                        format!(
                            "PooledBuffer::advance_mut 超出剩余可写空间：请求 {len}，实际 {available}",
                        ),
                    ));
                }
                unsafe {
                    buf.advance_mut(len);
                }
                Ok(())
            }
            BufferState::ReadOnly(_) => Err(CoreError::new(
                codes::APP_INVALID_ARGUMENT,
                "PooledBuffer 已冻结，advance_mut 无效",
            )),
        }
    }
}

fn empty_chunk_mut() -> &'static mut [MaybeUninit<u8>] {
    static mut EMPTY: [MaybeUninit<u8>; 0] = [];
    unsafe { &mut EMPTY[..] }
}

/// `PooledBuffer` 是面向池化场景的零拷贝缓冲。
///
/// # 设计动机（Why）
/// - 框架需要一个同时满足 `ReadableBuffer` 与 `WritableBuffer` 契约的具体类型，
///   以便流水线在租借后无需再做类型擦除或额外包装即可完成读写转换。
/// - 借助 `BytesMut` 的引用计数与高效分片能力，可以在不复制数据的情况下提供 `split_to`、`advance` 等操作，
///   满足协议编解码与网络传输的高频访问模式。
/// - 通过内部 `Lease` 与 `BufferRecycler` 的组合，自动在 `Drop` 时归还容量，
///   避免调用端遗忘释放导致池统计不一致。
///
/// # 架构关系（How）
/// - `state` 记录当前读写态：写态使用 `BytesMut`，冻结后转换为 `Bytes`；
/// - `lease` 通过 `Arc` 与其它视图共享，当所有视图销毁时回收容量；
/// - 所有读写操作会根据状态分别委派给 `BytesMut`/`Bytes`，并在扩容后即时刷新租约统计。
///
/// # 契约说明（What）
/// - **构造前置条件**：池实现需提供有效的 `BufferRecycler`，并保证其在整个生命周期内有效；
///   `BytesMut` 初始容量即为租约容量基准。
/// - **调用后置条件**：`remaining` 始终反映当前可读字节数；写操作遵守顺序追加语义；
///   `freeze` 后返回的对象仅支持读取；`Drop` 时池必然收到一次 `reclaim` 调用。
///
/// # 风险与取舍（Trade-offs）
/// - 为兼容 `ReadableBuffer` 对象安全要求，本类型实现了 `unsafe impl Sync`，
///   依赖 Rust 借用规则保证不存在并发写入；实现者在维护时需注意不要暴露新的 `&self` 可变访问路径。
/// - 拆分 (`split_to`) 返回的前缀与剩余缓冲共享租约，回收将以整体容量为单位，
///   因此池侧统计应以“租约”而非“视图”粒度衡量。
pub struct PooledBuffer {
    state: BufferState,
    lease: Arc<Lease>,
}

impl PooledBuffer {
    /// 使用给定的 `BytesMut` 与回收句柄创建缓冲。
    ///
    /// # 参数
    /// - `inner`：池分配的可写缓冲，要求容量足以满足调用者初始写入需求；
    /// - `recycler`：池级回收句柄，通常为 `Arc<Pool>` 或其轻量代理。
    ///
    /// # 前置条件
    /// - `inner` 尚未被其它视图共享（新分配或独占所有权），以避免初始时出现多重回收；
    /// - `recycler` 与池的生命周期至少与缓冲等长。
    ///
    /// # 后置条件
    /// - 返回的缓冲处于可写状态；其 `lease` 记录的容量等于 `inner.capacity()`；
    /// - 一旦所有引用被释放，`reclaim` 将收到该容量的回收通知。
    pub fn new(inner: BytesMut, recycler: Arc<dyn BufferRecycler>) -> Self {
        let lease = Arc::new(Lease::new(inner.capacity(), recycler));
        Self {
            state: BufferState::Writable(inner),
            lease,
        }
    }

    /// 通用构造器，便于在拆分时复用现有租约。
    fn from_state(state: BufferState, lease: Arc<Lease>) -> Self {
        Self { state, lease }
    }

    /// 返回当前状态是否仍可写入。
    fn is_writable(&self) -> bool {
        matches!(self.state, BufferState::Writable(_))
    }

    /// 确保缓冲处于可写态，否则返回错误。
    fn require_writable(&self, op: &'static str) -> Result<(), CoreError> {
        if self.is_writable() {
            Ok(())
        } else {
            Err(CoreError::new(
                codes::APP_INVALID_ARGUMENT,
                format!("PooledBuffer 已冻结，无法执行 {op}"),
            ))
        }
    }

    /// 获取可写状态的可变引用；调用前需确认仍处于写态。
    fn writable_mut(&mut self) -> &mut BytesMut {
        match &mut self.state {
            BufferState::Writable(buf) => buf,
            BufferState::ReadOnly(_) => unreachable!("require_writable 已保证状态"),
        }
    }

    /// 在容量发生变化时刷新租约统计。
    fn refresh_capacity(&self, capacity: usize) {
        self.lease.update_capacity(capacity);
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        //=== 教案式说明 ===//
        // 1. **目标 (Why)**：在缓冲被释放时，尽量归还底层 `BytesMut`，确保池能够复用内存；
        //    若无法获取所有权，仍需通过 `Lease` 报告最终容量，保持统计准确。
        // 2. **策略 (How)**：
        //    - 将当前状态替换为一个空壳，取得原有的 `BufferState` 所有权；
        //    - 针对写态与只读态分别尝试提取 `BytesMut`：
        //      - 写态直接取得内部 `BytesMut`；
        //      - 只读态调用 `Bytes::try_mut`，在引用计数归一的情况下夺回可写视图；
        //    - 将成功回收的缓冲交由 `Lease` 存储，等待最后一次 `Arc` 释放时统一归还池；
        //      若仍存在其它别名则返回 `None`，表示本次仅能回收容量统计。
        // 3. **契约 (What)**：
        //    - 前置条件：`self.state` 与 `self.lease` 均有效；
        //    - 后置条件：`Lease::store_buffer` 至多存储一次非空缓冲；
        //      之后 `Lease::drop` 会以 `ReclaimedBuffer` 形式通知池。
        // 4. **风险 (Trade-offs)**：
        //    - 当上层长期持有 `Bytes` 切片时，`try_mut` 将失败，导致我们无法复用原内存；
        //      池会自动感知到 `None` 并选择重新分配，保持语义正确性。
        let state = mem::replace(&mut self.state, BufferState::ReadOnly(Bytes::new()));
        let reclaimed = match state {
            BufferState::Writable(mut buf) => {
                buf.clear();
                Some(buf)
            }
            BufferState::ReadOnly(bytes) => match bytes.try_into_mut() {
                Ok(mut writable) => {
                    writable.clear();
                    Some(writable)
                }
                Err(_) => None,
            },
        };
        self.lease.store_buffer(reclaimed);
    }
}

impl ReadableBuffer for PooledBuffer {
    fn remaining(&self) -> usize {
        self.state.len()
    }

    fn chunk(&self) -> &[u8] {
        self.state.chunk()
    }

    fn split_to(&mut self, len: usize) -> Result<Box<dyn ReadableBuffer>, CoreError> {
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "PooledBuffer::split_to 超出剩余可读字节",
            ));
        }
        let split_state = self.state.split_to(len);
        debug_assert!(matches!(&split_state, BufferState::ReadOnly(_)));
        Ok(Box::new(Self::from_state(
            split_state,
            Arc::clone(&self.lease),
        )))
    }

    fn advance(&mut self, len: usize) -> Result<(), CoreError> {
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "PooledBuffer::advance 超出剩余可读字节",
            ));
        }
        self.state.advance(len);
        Ok(())
    }

    fn copy_into_slice(&mut self, dst: &mut [u8]) -> Result<(), CoreError> {
        if dst.len() > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "PooledBuffer::copy_into_slice 数据不足",
            ));
        }
        self.state.copy_into_slice(dst);
        Ok(())
    }

    fn try_into_vec(self: Box<Self>) -> Result<Vec<u8>, CoreError> {
        Ok(self.state.to_vec())
    }
}

impl WritableBuffer for PooledBuffer {
    fn capacity(&self) -> usize {
        match &self.state {
            BufferState::Writable(buf) => buf.capacity(),
            BufferState::ReadOnly(bytes) => bytes.len(),
        }
    }

    fn remaining_mut(&self) -> usize {
        match &self.state {
            BufferState::Writable(buf) => buf.capacity().saturating_sub(buf.len()),
            BufferState::ReadOnly(_) => 0,
        }
    }

    fn written(&self) -> usize {
        self.state.len()
    }

    fn reserve(&mut self, additional: usize) -> Result<(), CoreError> {
        self.require_writable("reserve")?;
        if additional == 0 {
            return Ok(());
        }
        let buf = self.writable_mut();
        let before = buf.capacity();
        buf.reserve(additional);
        let after = buf.capacity();
        if after != before {
            self.refresh_capacity(after);
        }
        Ok(())
    }

    fn put_slice(&mut self, src: &[u8]) -> Result<(), CoreError> {
        self.require_writable("put_slice")?;
        if src.is_empty() {
            return Ok(());
        }
        let buf = self.writable_mut();
        let remaining = buf.capacity().saturating_sub(buf.len());
        if remaining < src.len() {
            buf.reserve(src.len() - remaining);
        }
        let before = buf.capacity();
        buf.put_slice(src);
        let after = buf.capacity();
        if after != before {
            self.refresh_capacity(after);
        }
        Ok(())
    }

    fn write_from(&mut self, src: &mut dyn ReadableBuffer, len: usize) -> Result<(), CoreError> {
        self.require_writable("write_from")?;
        if len > src.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "PooledBuffer::write_from 源缓冲数据不足",
            ));
        }
        let mut tmp = vec![0u8; len];
        src.copy_into_slice(&mut tmp)?;
        self.put_slice(&tmp)
    }

    fn chunk_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        debug_assert!(
            self.is_writable(),
            "PooledBuffer::chunk_mut 只应在可写态调用；若已冻结请重新租借缓冲"
        );
        if self.is_writable() {
            self.state.chunk_mut()
        } else {
            empty_chunk_mut()
        }
    }

    fn advance_mut(&mut self, len: usize) -> Result<(), CoreError> {
        self.require_writable("advance_mut")?;
        let before = self.state.len();
        let result = self.state.advance_mut(len);
        if result.is_ok() {
            debug_assert_eq!(
                self.state.len(),
                before + len,
                "PooledBuffer::advance_mut 后写入指针应推进 len 字节"
            );
        }
        result
    }

    fn clear(&mut self) {
        if self.is_writable() {
            self.writable_mut().clear();
        }
    }

    fn freeze(mut self: Box<Self>) -> Result<Box<dyn ReadableBuffer>, CoreError> {
        if let BufferState::Writable(buf) = &mut self.state {
            let frozen = buf.split().freeze();
            self.refresh_capacity(frozen.len());
            self.state = BufferState::ReadOnly(frozen);
        }
        Ok(self)
    }
}

/// `PooledBuffer` 的并发安全性说明：
///
/// - 写操作均要求 `&mut self`，遵循 Rust 借用规则，避免与其它读取并发发生数据竞争；
/// - 只读方法仅访问不可变切片，不会触发内部可变状态；
/// - `BufferState` 的切换仅发生在独占持有时（`&mut self` 或 `Box<Self>`）。
unsafe impl Send for PooledBuffer {}

/// 参见 [`Send`] 的说明，同步访问只暴露不可变视图或受限于独占借用，
/// 因此可安全实现 `Sync` 以满足对象安全要求。
unsafe impl Sync for PooledBuffer {}
