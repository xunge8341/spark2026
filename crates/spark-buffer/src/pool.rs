use alloc::{borrow::Cow, boxed::Box, sync::Arc, vec, vec::Vec};
use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::BytesMut;
use spin::Mutex;

use spark_core::{
    CoreError, Result,
    buffer::{
        BufferPool, ErasedSparkBuf, ErasedSparkBufMut, PoolStatDimension, PoolStats, WritableBuffer,
    },
};

use crate::pooled_buffer::{BufferRecycler, PooledBuffer, ReclaimedBuffer};

/// `SlabBufferPool` 提供基于自由链表（Free List）的缓冲池实现，
/// 专注在**高并发、低延迟**场景下复用 `BytesMut`，以减少堆分配次数。
///
/// # 模块角色（Why）
/// - 作为 `spark-core::buffer::BufferPool` 的默认实现，为运行时、协议栈提供统一的缓冲来源；
/// - 借助 `PooledBuffer` 的生命周期钩子，在 `Drop` 阶段自动回收 `BytesMut`，避免调用方关注回收细节；
/// - 对外暴露 `alloc_readable`/`alloc_writable` 工厂方法，便于直接生成对象安全的缓冲引用。
///
/// # 核心机制（How）
/// - 内部维护 `spin::Mutex<Vec<BytesMut>>` 作为自由链表，租借时优先复用足够大的块，减少重新分配；
/// - `PoolMetrics` 通过原子计数跟踪 `allocated_bytes`、`available_bytes`、`active_leases` 等指标，
///   支撑 `statistics` 快照以及后续的监控集成；
/// - `BufferRecycler` 实现中使用 `ReclaimedBuffer` 获取回收上下文，既更新统计也将 `BytesMut` 放回链表。
///
/// # 契约说明（What）
/// - **线程安全**：所有共享状态均通过 `spin::Mutex` 与原子计数保护，满足 `Send + Sync + 'static` 约束；
/// - **前置条件**：调用方需保证 `min_capacity` 表示真实需求；若为 0，将返回最小容量的可写缓冲；
/// - **后置条件**：`alloc_writable` 返回的缓冲满足 `remaining_mut() >= min_capacity`，
///   `alloc_readable` 会将输入字节完整写入并冻结为只读视图。
///
/// # 设计权衡（Trade-offs）
/// - 使用自旋锁（`spin::Mutex`）而非 `parking_lot::Mutex`，以便在 `no_std`/线程数量有限的环境中仍能工作；
/// - 回收失败（无法重新获得 `BytesMut`）时，仅更新统计并在下次租借时重新分配，
///   牺牲部分性能换取语义稳定性；
/// - `shrink_to_fit` 采取“清空自由链表”的简单策略，便于在压测后快速归还峰值内存。
#[derive(Clone)]
pub struct SlabBufferPool {
    inner: Arc<PoolInner>,
}

impl Default for SlabBufferPool {
    fn default() -> Self {
        Self {
            inner: Arc::new(PoolInner::new()),
        }
    }
}

impl SlabBufferPool {
    /// 创建空池实例，供运行时注入或测试场景直接使用。
    pub fn new() -> Self {
        Self::default()
    }

    /// 分配并填充一个只读缓冲。
    ///
    /// # 参数与契约
    /// - `data`：待写入的原始字节切片，允许为空；
    /// - **前置条件**：调用方无需持有其它租借；方法内部会根据长度计算最小容量；
    /// - **后置条件**：返回的 `ErasedSparkBuf` 中 `remaining()` 等于 `data.len()`，
    ///   且可被安全地拆分、拷贝。
    ///
    /// # 实现策略
    /// 1. 复用 `alloc_writable` 的核心逻辑，确保统计与回收路径一致；
    /// 2. 将输入数据写入 `PooledBuffer` 后立即 `freeze`，生成只读视图；
    /// 3. 若 `put_slice` 过程中触发扩容，将自动刷新租约容量，保证回收时统计正确。
    pub fn alloc_readable(&self, data: &[u8]) -> Result<Box<ErasedSparkBuf>, CoreError> {
        let mut writable: Box<PooledBuffer> = Box::new(self.allocate_pooled(data.len())?);
        if !data.is_empty() {
            writable.put_slice(data)?;
        }
        writable.freeze()
    }

    /// 分配一个可写缓冲，满足最小容量约束。
    ///
    /// # 参数与契约
    /// - `min_capacity`：调用方期望的最小可写空间；
    /// - **后置条件**：返回对象满足 `WritableBuffer` 契约，容量至少等于 `min_capacity`；
    /// - **异常处理**：当前实现不会返回错误，但保留 `Result` 以对齐 trait 约束。
    pub fn alloc_writable(&self, min_capacity: usize) -> Result<Box<ErasedSparkBufMut>, CoreError> {
        let buffer: Box<PooledBuffer> = Box::new(self.allocate_pooled(min_capacity)?);
        Ok(buffer)
    }

    /// 实际的缓冲构建逻辑，供公开 API 与 trait 方法复用。
    fn allocate_pooled(&self, min_capacity: usize) -> Result<PooledBuffer, CoreError> {
        let raw = self.inner.acquire_buffer(min_capacity);
        let recycler: Arc<dyn BufferRecycler> = self.inner.clone();
        Ok(PooledBuffer::new(raw, recycler))
    }

    /// 返回池化缓冲的实时统计快照。
    ///
    /// # 教案式解读
    /// - **目标 (Why)**：调用方在运行时需要低成本地观测缓冲池的健康状况，例如活跃租借数、
    ///   缓冲回收次数以及池命中率，本方法提供统一的查询入口。
    /// - **实现策略 (How)**：
    ///   1. 直接复用 `PoolInner::snapshot` 的原子读操作，避免额外锁与拷贝；
    ///   2. 快照中同时包含核心字段与 `custom_dimensions`，保证与 `BufferPool::statistics`
    ///      的约定一致；
    ///   3. 由于内部依赖 `Ordering::Relaxed` 读取，整段逻辑仅产生单次 `Arc` 克隆和若干原子读，
    ///      满足低开销要求。
    /// - **契约 (What)**：
    ///   - **输入**：无额外参数，调用前无需持锁。
    ///   - **返回值**：[`PoolStats`]——代表调用瞬间的统计快照。
    ///   - **前置条件**：`SlabBufferPool` 已初始化；
    ///   - **后置条件**：返回的结构体不持有内部可变引用，可安全在调用方线程使用或克隆。
    /// - **设计权衡 (Trade-offs)**：
    ///   - 使用惰性统计（即刻读原子计数）而非累积快照缓存，牺牲部分跨调用一致性换取极低延迟；
    ///   - 若未来需要更精细的统计（如直方图），可以在 `custom_dimensions` 中扩展而无需修改接口。
    pub fn stats(&self) -> PoolStats {
        self.inner.snapshot()
    }
}

impl BufferPool for SlabBufferPool {
    fn acquire(&self, min_capacity: usize) -> Result<Box<dyn WritableBuffer>, CoreError> {
        let buffer: Box<PooledBuffer> = Box::new(self.allocate_pooled(min_capacity)?);
        Ok(buffer)
    }

    fn shrink_to_fit(&self) -> Result<usize, CoreError> {
        Ok(self.inner.shrink_free_list())
    }

    fn statistics(&self) -> Result<PoolStats, CoreError> {
        Ok(self.stats())
    }
}

struct PoolInner {
    free_list: Mutex<Vec<BytesMut>>,
    metrics: PoolMetrics,
}

impl PoolInner {
    fn new() -> Self {
        Self {
            free_list: Mutex::new(Vec::new()),
            metrics: PoolMetrics::default(),
        }
    }

    /// 从自由链表或堆上获取一个满足容量的 `BytesMut`。
    fn acquire_buffer(&self, min_capacity: usize) -> BytesMut {
        self.metrics.record_allocation();
        let reused = {
            let mut list = self.free_list.lock();
            if let Some(index) = list.iter().position(|buf| buf.capacity() >= min_capacity) {
                let mut buf = list.swap_remove(index);
                let capacity = buf.capacity();
                buf.clear();
                self.metrics.decrease_available(capacity);
                Some(buf)
            } else {
                None
            }
        };

        let mut buffer = match reused {
            Some(buf) => buf,
            None => {
                self.metrics.record_pool_miss();
                let buf = BytesMut::with_capacity(min_capacity);
                let capacity = buf.capacity();
                self.metrics.increase_on_new_allocation(capacity);
                buf
            }
        };
        buffer.clear();
        self.metrics.increase_active_buffers();
        buffer
    }

    fn shrink_free_list(&self) -> usize {
        let mut list = self.free_list.lock();
        let reclaimed: usize = list.iter().map(BytesMut::capacity).sum();
        list.clear();
        self.metrics.decrease_on_shrink(reclaimed);
        reclaimed
    }

    fn snapshot(&self) -> PoolStats {
        let free_slots = self.free_list.lock().len();
        let active_buffers = self.metrics.active_buffers.load(Ordering::Relaxed);
        let total_allocated = self.metrics.total_allocated.load(Ordering::Relaxed);
        let total_recycled = self.metrics.total_recycled.load(Ordering::Relaxed);
        let pool_misses = self.metrics.pool_misses.load(Ordering::Relaxed);
        let total_bytes = self.metrics.total_bytes.load(Ordering::Relaxed);

        // 教案级补充：指标集合构建策略
        //
        // - **意图（Why）**：一次性初始化所有 `PoolStatDimension`，避免 `Vec::with_capacity + push`
        //   模式在 Clippy 的 `vec-init-then-push` 检查下触发告警，同时保证读者理解指标顺序的稳定性。
        // - **逻辑（How）**：直接使用 `vec![]` 宏按最终顺序填充条目，既减轻错误处理，也让代码
        //   对“指标数量固定”为前置条件的假设更加直观。
        // - **契约（What）**：生成的 `custom_dimensions` 始终包含 6 个元素，对应自由槽位、活跃缓冲等指标。
        // - **权衡（Trade-offs）**：失去显式 `with_capacity` 的微观优化，但换来静态分析友好性与可读性。
        let custom_dimensions = vec![
            PoolStatDimension {
                key: Cow::Borrowed("slab_free_slots"),
                value: free_slots,
            },
            PoolStatDimension {
                key: Cow::Borrowed("active_buffers"),
                value: active_buffers,
            },
            PoolStatDimension {
                key: Cow::Borrowed("total_allocated"),
                value: total_allocated,
            },
            PoolStatDimension {
                key: Cow::Borrowed("total_recycled"),
                value: total_recycled,
            },
            PoolStatDimension {
                key: Cow::Borrowed("pool_misses"),
                value: pool_misses,
            },
            PoolStatDimension {
                key: Cow::Borrowed("total_bytes"),
                value: total_bytes,
            },
        ];

        PoolStats {
            allocated_bytes: self.metrics.allocated_bytes.load(Ordering::Relaxed),
            resident_bytes: self.metrics.resident_bytes.load(Ordering::Relaxed),
            active_leases: active_buffers,
            available_bytes: self.metrics.available_bytes.load(Ordering::Relaxed),
            pending_lease_requests: 0,
            failed_acquisitions: self.metrics.failed_acquisitions.load(Ordering::Relaxed),
            custom_dimensions,
        }
    }
}

impl BufferRecycler for PoolInner {
    fn reclaim(&self, reclaimed: ReclaimedBuffer) {
        self.metrics.record_recycle();
        self.metrics.decrease_active_buffers();
        let capacity = reclaimed.capacity();
        match reclaimed.into_buffer() {
            Some(mut buf) => {
                buf.clear();
                self.metrics.increase_available(capacity);
                self.free_list.lock().push(buf);
            }
            None => {
                self.metrics.decrease_on_loss(capacity);
            }
        }
    }
}

#[derive(Default)]
struct PoolMetrics {
    allocated_bytes: AtomicUsize,
    resident_bytes: AtomicUsize,
    available_bytes: AtomicUsize,
    active_buffers: AtomicUsize,
    failed_acquisitions: AtomicU64,
    total_allocated: AtomicUsize,
    total_recycled: AtomicUsize,
    pool_misses: AtomicUsize,
    total_bytes: AtomicUsize,
}

impl PoolMetrics {
    fn increase_on_new_allocation(&self, capacity: usize) {
        self.allocated_bytes.fetch_add(capacity, Ordering::Relaxed);
        self.resident_bytes.fetch_add(capacity, Ordering::Relaxed);
        self.total_bytes.fetch_add(capacity, Ordering::Relaxed);
    }

    fn increase_available(&self, capacity: usize) {
        self.available_bytes.fetch_add(capacity, Ordering::Relaxed);
    }

    fn decrease_available(&self, capacity: usize) {
        saturating_sub(&self.available_bytes, capacity);
    }

    fn decrease_on_loss(&self, capacity: usize) {
        saturating_sub(&self.allocated_bytes, capacity);
        saturating_sub(&self.resident_bytes, capacity);
        saturating_sub(&self.total_bytes, capacity);
    }

    fn decrease_on_shrink(&self, capacity: usize) {
        self.decrease_available(capacity);
        self.decrease_on_loss(capacity);
    }

    fn increase_active_buffers(&self) {
        self.active_buffers.fetch_add(1, Ordering::Relaxed);
    }

    fn decrease_active_buffers(&self) {
        let _ = self
            .active_buffers
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
                Some(prev.saturating_sub(1))
            });
    }

    fn record_allocation(&self) {
        saturating_inc(&self.total_allocated);
    }

    fn record_pool_miss(&self) {
        saturating_inc(&self.pool_misses);
    }

    fn record_recycle(&self) {
        saturating_inc(&self.total_recycled);
    }
}

fn saturating_sub(target: &AtomicUsize, value: usize) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(value))
    });
}

fn saturating_inc(target: &AtomicUsize) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}
