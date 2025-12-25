use crate::{CoreError, sealed::Sealed};
use alloc::{borrow::Cow, boxed::Box, vec::Vec};

use super::{WritableBuffer, writable::ErasedSparkBufMut};

/// `BufferPool` 规定缓冲区租借与回收的统一接口。
///
/// # 契约维度速览
/// - **语义**：统一“租借 → 写入 → 冻结/归还”的生命周期，支撑传输、编解码与 Pipeline 在零拷贝路径下复用缓冲。
/// - **错误**：所有失败均返回 [`CoreError`]，建议使用 `resource_exhausted.pool_unavailable`、`protocol.decode` 等稳定错误码。
/// - **并发**：Trait 要求 `Send + Sync`；实现内部若采用锁或原子，需要保证在高并发下无活锁/饥饿。
/// - **背压**：当池容量不足时可通过错误返回或在 [`PoolStats`] 中暴露水位，提示上层切换降级或退避策略。
/// - **超时**：契约未定义内置超时；调用方若需限时租借，应结合 `CallContext::deadline()` 或外部定时器主动终止等待。
/// - **取消**：当上层取消调用时，正在等待的租借操作应尽快退出并释放已持有资源；实现若包含自旋，应定期查询取消信号。
/// - **观测标签**：推荐在指标/日志中统一记录 `pool.name`、`pool.tenant`、`pool.event`（`acquire`、`shrink`、`stats`）。
/// - **示例(伪码)**：
///   ```text
///   buf = pool.acquire(min_capacity)
///   try encode(buf)
///       send(buf.freeze())
///   catch err => log(err.code)
///   finally metrics.record(pool.statistics())
///   ```
///
/// # 设计背景（Why）
/// - 综合 Netty `ByteBufAllocator`、Envoy `WatermarkBufferFactory`、Aerospike `BufferPool`、ClickHouse `Arena`、Tokio `BytesMut` 共享池实践，确保在高并发场景稳定控制内存峰值。
/// - 运行时、传输层、协议层需要共享统一的租借来源，以支持跨线程协程协作和背压策略。
///
/// # 逻辑解析（How）
/// - `acquire` 负责租借指定最小容量的可写缓冲，底层可采用 slab、分级自由链表或 jemalloc arena。
/// - `shrink_to_fit` 允许主动归还多余容量，吸收 Rust `Vec::shrink_to_fit` 与 Netty `trimUnreleasedBytes` 的思路。
/// - `statistics` 提供池化观测指标，便于运维与自适应扩容。
///
/// # 契约说明（What）
/// - **输入参数**：`min_capacity` 表示调用方当前写入批次最少需要的字节数。
/// - **返回值**：
///   - `acquire` 返回实现了 [`WritableBuffer`] 的缓冲实例，调用方拥有唯一所有权。
///   - `shrink_to_fit` 返回实际回收的字节数，便于调用方做指标打点。
///   - `statistics` 返回 [`PoolStats`] 快照，可为空实现但建议填充核心字段。
/// - **前置条件**：池实现必须是线程安全的；若依赖外部内存池，需确保生命周期覆盖整个运行时。
/// - **后置条件**：`acquire` 成功后必须保证返回缓冲的 `remaining_mut() >= min_capacity`。
///
/// # 设计考量（Trade-offs & Gotchas）
/// - **分配策略**：允许实现根据场景选择固定大小、指数级或 TCMalloc 风格的分配策略，契约仅关注语义。
/// - **背压处理**：当池容量不足时可返回 `CoreError`，或在错误中携带降级建议（如切换到 on-heap）。
/// - **观测性**：`statistics` 默认返回静态数据，鼓励实现者提供诸如“池使用率”“活跃租借数”等核心指标。
///
/// # 示例（Examples）
/// ```rust
/// use spark_core::buffer::{BufferPool, PoolStats, ReadableBuffer, WritableBuffer};
/// use spark_core::error::codes;
/// use spark_core::CoreError;
/// use std::sync::{Arc, Mutex};
///
/// //=== 可读缓冲最小实现 ===//
/// struct FrozenBuffer {
///     data: Vec<u8>,
///     cursor: usize,
/// }
///
/// impl ReadableBuffer for FrozenBuffer {
///     fn remaining(&self) -> usize {
///         self.data.len().saturating_sub(self.cursor)
///     }
///
///     fn chunk(&self) -> &[u8] {
///         &self.data[self.cursor..]
///     }
///
///     fn split_to(&mut self, len: usize) -> crate::Result<Box<dyn ReadableBuffer>, CoreError> {
///         if len > self.remaining() {
///             return Err(CoreError::new(codes::PROTOCOL_DECODE, "split exceeds remaining bytes"));
///         }
///         let end = self.cursor + len;
///         let slice = self.data[self.cursor..end].to_vec();
///         self.cursor = end;
///         Ok(Box::new(FrozenBuffer { data: slice, cursor: 0 }))
///     }
///
///     fn advance(&mut self, len: usize) -> crate::Result<(), CoreError> {
///         if len > self.remaining() {
///             return Err(CoreError::new(codes::PROTOCOL_DECODE, "advance exceeds remaining bytes"));
///         }
///         self.cursor += len;
///         Ok(())
///     }
///
///     fn copy_into_slice(&mut self, dst: &mut [u8]) -> crate::Result<(), CoreError> {
///         if dst.len() > self.remaining() {
///             return Err(CoreError::new(codes::PROTOCOL_DECODE, "insufficient data for copy"));
///         }
///         let end = self.cursor + dst.len();
///         dst.copy_from_slice(&self.data[self.cursor..end]);
///         self.cursor = end;
///         Ok(())
///     }
///
///     fn try_into_vec(self: Box<Self>) -> crate::Result<Vec<u8>, CoreError> {
///         Ok(self.data)
///     }
/// }
///
/// //=== 可写缓冲最小实现 ===//
/// struct SimpleBuffer {
///     data: Vec<u8>,
/// }
///
/// impl SimpleBuffer {
///     fn with_capacity(capacity: usize) -> Self {
///         Self { data: Vec::with_capacity(capacity) }
///     }
/// }
///
/// impl WritableBuffer for SimpleBuffer {
///     fn capacity(&self) -> usize {
///         self.data.capacity()
///     }
///
///     fn remaining_mut(&self) -> usize {
///         self.data.capacity().saturating_sub(self.data.len())
///     }
///
///     fn written(&self) -> usize {
///         self.data.len()
///     }
///
///     fn reserve(&mut self, additional: usize) -> crate::Result<(), CoreError> {
///         self.data.reserve(additional);
///         Ok(())
///     }
///
///     fn put_slice(&mut self, src: &[u8]) -> crate::Result<(), CoreError> {
///         self.data.extend_from_slice(src);
///         Ok(())
///     }
///
///     fn write_from(&mut self, src: &mut dyn ReadableBuffer, len: usize) -> crate::Result<(), CoreError> {
///         if len > src.remaining() {
///             return Err(CoreError::new(codes::PROTOCOL_DECODE, "source buffer exhausted"));
///         }
///         let mut tmp = vec![0u8; len];
///         src.copy_into_slice(&mut tmp)?;
///         self.data.extend_from_slice(&tmp);
///         Ok(())
///     }
///
///     fn clear(&mut self) {
///         self.data.clear();
///     }
///
///     fn freeze(self: Box<Self>) -> crate::Result<Box<dyn ReadableBuffer>, CoreError> {
///         Ok(Box::new(FrozenBuffer { data: self.data, cursor: 0 }))
///     }
/// }
///
/// //=== 池实现：通过互斥锁维护快照 ===//
/// #[derive(Default)]
/// struct InMemoryPool {
///     snapshot: Arc<Mutex<PoolStats>>,
/// }
///
/// impl BufferPool for InMemoryPool {
///     fn acquire(&self, min_capacity: usize) -> crate::Result<Box<dyn WritableBuffer>, CoreError> {
///         let mut stats = self.snapshot.lock().expect("mutex poisoned");
///         stats.allocated_bytes = stats.allocated_bytes.saturating_add(min_capacity);
///         stats.available_bytes = stats.available_bytes.saturating_add(min_capacity);
///         stats.active_leases = stats.active_leases.saturating_add(1);
///         Ok(Box::new(SimpleBuffer::with_capacity(min_capacity)))
///     }
///
///     fn shrink_to_fit(&self) -> crate::Result<usize, CoreError> {
///         Ok(0)
///     }
///
///     fn statistics(&self) -> crate::Result<PoolStats, CoreError> {
///         Ok(self.snapshot.lock().expect("mutex poisoned").clone())
///     }
/// }
///
/// let pool = InMemoryPool::default();
/// let mut writable = pool.acquire(4).expect("租借缓冲不应失败");
/// writable.put_slice(&[0xAA, 0xBB]).expect("写入示例数据");
/// let frozen = writable.freeze().expect("freeze 应产生只读缓冲");
/// assert_eq!(frozen.remaining(), 2, "冻结后的剩余字节应与写入量一致");
/// let stats = pool.statistics().expect("应能读取快照");
/// assert!(stats.allocated_bytes >= stats.active_leases, "池快照应保持语义约束");
/// ```
pub trait BufferPool: Send + Sync + 'static + Sealed {
    /// 租借一个最少具备 `min_capacity` 可写空间的缓冲区。
    fn acquire(&self, min_capacity: usize) -> crate::Result<Box<dyn WritableBuffer>, CoreError>;

    /// 主动收缩池内冗余内存，返回实际回收的字节数。
    fn shrink_to_fit(&self) -> crate::Result<usize, CoreError>;

    /// 返回池当前的核心统计指标（例如 `usage_bytes`、`lease_count`）。
    fn statistics(&self) -> crate::Result<PoolStats, CoreError>;
}

/// 高阶缓冲租借契约，面向运行时统一分配入口。
///
/// ## 教案式说明
/// - **意图 (Why)**：
///   - 充当 `buffer` 模块对外暴露的“工厂接口”，让执行计划、网络栈、存储日志等子系统能够以统一入口租借 [`WritableBuffer`]，避免直接依赖具体池实现带来的耦合。
///   - 将 Netty `ByteBufAllocator`、Envoy `Buffer::Factory` 等行业实践迁移到 Spark Core，以支撑 NUMA 感知、零拷贝或自适应池策略的扩展。
/// - **逻辑 (How)**：
///   - trait 仅定义单一方法 [`BufferAllocator::acquire`]，内部委派给具体的 [`BufferPool`] 实现；
///   - 通过对象安全（`Box<ErasedSparkBufMut>`）返回写端缓冲，上层在完成写入后可调用 `freeze()` 转换为只读视图。
/// - **契约 (What)**：
///   - **输入/前置**：`min_capacity` 指明调用方至少需要的可写字节数；实现需保证线程安全并遵循池的生命周期管理（通常为 `'static`）。
///   - **输出/后置**：成功时返回的缓冲需满足 `remaining_mut() >= min_capacity`，且具备 [`WritableBuffer`] 的全部语义；失败时返回 [`CoreError`]，建议带有资源类错误码。
/// - **权衡与注意事项 (Trade-offs & Gotchas)**：
///   - 保持接口最小化可让不同池实现（预分配、按需增长、分级分配）自由选择内部策略；
///   - trait 本身不提供归还接口，约定通过 `Drop`、引用计数或池内部回收完成资源释放，上层在热路径中需谨慎处理泄漏风险。
pub trait BufferAllocator: Send + Sync + Sealed {
    /// 租借满足最小容量的缓冲。
    fn acquire(&self, min_capacity: usize) -> crate::Result<Box<ErasedSparkBufMut>, CoreError>;
}

impl<T> BufferAllocator for T
where
    T: BufferPool + ?Sized,
{
    fn acquire(&self, min_capacity: usize) -> crate::Result<Box<ErasedSparkBufMut>, CoreError> {
        BufferPool::acquire(self, min_capacity)
    }
}

/// 将 [`BufferPool`] 引用包装为 [`BufferAllocator`] 视图的适配器。
pub struct BufferPoolAllocatorAdapter<'a> {
    pool: &'a dyn BufferPool,
}

impl<'a> BufferPoolAllocatorAdapter<'a> {
    /// 通过池引用构造适配器。
    pub fn new(pool: &'a dyn BufferPool) -> Self {
        Self { pool }
    }
}

impl BufferAllocator for BufferPoolAllocatorAdapter<'_> {
    fn acquire(&self, min_capacity: usize) -> crate::Result<Box<ErasedSparkBufMut>, CoreError> {
        self.pool.acquire(min_capacity)
    }
}

impl dyn BufferPool {
    /// 获取针对当前池的 [`BufferAllocator`] 视图。
    pub fn as_allocator(&self) -> BufferPoolAllocatorAdapter<'_> {
        BufferPoolAllocatorAdapter::new(self)
    }
}

/// 池统计快照，帮助调用方观测内存行为并执行自适应调度。
///
/// # 设计背景（Why）
/// - 旧版接口通过 `&dyn PoolStatisticsView` + `as_pairs` 返回键值对数组，
///   对于常见的容量、租借数等核心指标缺乏静态类型，调用方只能通过字符串匹配取值，
///   难以编译期校验字段是否存在，且在 Prometheus/OpenTelemetry 等后端中无法直接做强类型映射。
/// - 新结构体以明确字段呈现核心指标，并保留 `custom_dimensions` 承载实现特有的数据，
///   在保证可扩展性的同时提升类型安全与 IDE 可发现性。
///
/// # 逻辑解析（How）
/// - `PoolStats` 使用值语义（`Clone` + `Default`），实现者可在内部生成快照后返回，
///   调用方无需担心生命周期绑定，实现上常见做法是读取原子计数或持锁收集数据。
/// - `custom_dimensions` 采用 `Vec<PoolStatDimension>`，鼓励实现者使用稳定的蛇形命名键，
///   并可通过该列表扩展如分级池使用率等信息。
///
/// # 契约说明（What）
/// - **字段含义**：
///   - `allocated_bytes`：池向系统请求的总字节数，包含已借出与待命容量。
///   - `resident_bytes`：当前常驻内存（通常等于或小于 `allocated_bytes`），可辅助判断碎片。
///   - `active_leases`：正在被调用方持有的缓冲数量。
///   - `available_bytes`：无需再分配即可提供的剩余容量，用于驱动背压或预扩容逻辑。
///   - `pending_lease_requests`：处于等待/排队状态的租借请求数量，帮助识别热点。
///   - `failed_acquisitions`：累计租借失败次数，方便实现者暴露降级建议。
///   - `custom_dimensions`：实现自定义指标的有序列表，键建议使用 `snake_case` 并保持稳定。
/// - **前置条件**：实现者在构造快照时需保证字段语义一致（例如 `available_bytes <= allocated_bytes`）。
/// - **后置条件**：返回的结构体代表调用瞬间的快照，不应长期引用内部可变状态。
///
/// # 设计取舍与风险（Trade-offs）
/// - 采用 `usize`/`u64` 而非 `NonZero` 类型，以支持尚未初始化或暂不可用时返回 0；
///   调用方若需要严格不为 0，可自行断言。
/// - `custom_dimensions` 仍使用字符串键，允许实验性指标快速落地；
///   若需进一步约束，可在上层定义白名单或枚举映射。
///
/// # 示例（Examples）
/// ```rust
/// use spark_core::buffer::{PoolStatDimension, PoolStats};
/// use std::borrow::Cow;
///
/// let stats = PoolStats {
///     allocated_bytes: 1024,
///     resident_bytes: 768,
///     active_leases: 2,
///     available_bytes: 256,
///     pending_lease_requests: 0,
///     failed_acquisitions: 0,
///     custom_dimensions: vec![PoolStatDimension {
///         key: Cow::Borrowed("slab_spans"),
///         value: 3,
///     }],
/// };
///
/// assert!(stats.allocated_bytes >= stats.resident_bytes);
/// assert_eq!(stats.custom_dimensions[0].key, "slab_spans");
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// **意图 (Why)**：记录池自创建以来主动向系统（操作系统或上游 Arena）申请的总字节数，
    /// 便于观察整体内存占用趋势，并与 `resident_bytes` 对比判断是否存在尚未归还的“虚高”。
    ///
    /// **逻辑 (How)**：通常由实现读取内部计数器或分配器统计值；若底层按层级统计，可在此字段聚合总和。
    ///
    /// **契约 (What)**：必须为非负整数；允许在池尚未初始化时返回 `0`。前置条件是实现维护申请总量计数；
    /// 后置条件是快照反映调用瞬时值，调用方可据此推导池的峰值承压能力。
    ///
    /// **考量 (Trade-offs)**：若实现存在“软上限”或延迟释放机制，数值可能短暂高于真实常驻值，
    /// 调用方需要结合 `resident_bytes` 做最终判断。
    pub allocated_bytes: usize,
    /// **意图**：刻画当前仍由池持有（含借出与待借）的常驻内存，帮助识别碎片或惰性归还问题。
    ///
    /// **逻辑**：多采用 `allocated_bytes - freed_bytes` 或直接读取后端分配器的 `in_use` 指标；
    /// 若无法区分，可与 `allocated_bytes` 保持一致并在 `custom_dimensions` 中补充注释。
    ///
    /// **契约**：应满足 `resident_bytes <= allocated_bytes`；若实现暂不支持该指标，可安全返回 `0`。
    ///
    /// **考量**：对于依赖后台 GC/Arena 的实现，需在文档中说明刷新周期，避免调用方误判瞬时波动。
    pub resident_bytes: usize,
    /// **意图**：统计当前正由业务方租借且尚未归还的缓冲数量，支撑背压与容量规划决策。
    ///
    /// **逻辑**：常通过原子计数器在 `acquire`/`release` 时增减；
    /// 若存在分级池，需要累加所有等级的租借数。
    ///
    /// **契约**：返回值必须大于等于等待队列长度；调用前提是实现能追踪租借生命周期。
    ///
    /// **考量**：高并发下降低锁争用时可能使用近似计数，
    /// 调用方应容忍短暂的不精确并结合 `failed_acquisitions` 评估风险。
    pub active_leases: usize,
    /// **意图**：反映无需额外分配即可提供的空闲字节数，帮助调用方判断是否需要提前扩容或限流。
    ///
    /// **逻辑**：通常等于内部空闲块容量之和；若池采用 slab/segmented 设计，可将剩余空间折算为字节后汇总。
    ///
    /// **契约**：应满足 `available_bytes + resident_bytes >= allocated_bytes`，允许在无精确信息时返回 0。
    ///
    /// **考量**：当实现存在后台回收线程时，该值可能滞后，请在 `custom_dimensions` 中暴露刷新节奏以便上层调优。
    pub available_bytes: usize,
    /// **意图**：揭示当前因资源不足而排队的租借请求数量，便于观察吞吐瓶颈。
    ///
    /// **逻辑**：在调用 `acquire` 被阻塞或排队时递增，完成分配后递减；
    /// 对于无阻塞实现，可固定返回 `0`。
    ///
    /// **契约**：需要保证统计与 `active_leases` 保持一致性，避免出现排队但活跃租借为 0 的矛盾数据。
    ///
    /// **考量**：若实现采用指数退避或拒绝策略，请在 `custom_dimensions` 中添加命中率指标辅助定位。
    pub pending_lease_requests: usize,
    /// **意图**：记录历史上租借失败的累计次数（如达到容量上限、发生 OOM 等），为 AIOps 提供异常趋势参考。
    ///
    /// **逻辑**：在 `acquire` 返回错误路径增加计数；若错误类型可细分，可在 `custom_dimensions` 中按类别拆分。
    ///
    /// **契约**：该值必须单调递增；允许溢出到 `u64::MAX` 后保持最大值。
    ///
    /// **考量**：若实现提供周期性重置功能，需在文档中说明重置策略及调用约束。
    pub failed_acquisitions: u64,
    /// **意图**：承载实现特有或实验性指标，扩展核心字段之外的观测能力。
    ///
    /// **逻辑**：使用有序向量携带 `key/value` 对，调用方可遍历并将其映射到监控系统（Prometheus/OpenTelemetry 等）。
    ///
    /// **契约**：键必须稳定且使用 `snake_case`，值为非负整数；前置条件是调用方能够理解自定义语义，
    /// 后置条件是返回的列表不会持有内部可变引用，可安全移动或克隆。
    ///
    /// **考量**：若指标数量较多，请权衡分配与序列化开销；必要时可通过 feature flag 控制。
    pub custom_dimensions: Vec<PoolStatDimension>,
}

/// 扩展指标维度，用于承载实现者的定制数据。
///
/// # 契约说明（What）
/// - `key`：稳定的蛇形命名字符串，建议使用模块前缀（例如 `slab_spans`）。
/// - `value`：非负整数值，可表示计数或容量；若需记录浮点，请在上层转换为基准单位。
/// - **前置条件**：键必须对同一实现保持稳定，避免调用方缓存后发生语义变化。
/// - **后置条件**：维度序列可为空；如需表示比率等小数，可通过扩展字段或编码为百分比整数。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolStatDimension {
    pub key: Cow<'static, str>,
    pub value: usize,
}
