use alloc::vec::Vec;
use core::sync::atomic::{AtomicUsize, Ordering};

use spark_codecs::buffer::{BufView, Chunks, PoolStats, ReadableBuffer, WritableBuffer, BufferPool};
use spark_codecs::CoreError;
use spark_codecs::codes;

/// Fuzz 专用缓冲池：以最小实现满足 `BufferPool` 契约。
///
/// - **Why**：在 fuzz 运行时复现 `EncodeContext`/`DecodeContext` 对缓冲池的依赖，
///   但避免引入真实网络池化实现的复杂性与并发噪声。
/// - **How**：每次租借都分配新的 [`LinearWritable`]，丢弃复用策略，仅用一个原子计数
///   记录租借次数，便于调试时观测资源使用频率。
/// - **What**：实现 [`BufferPool`] 所需的 `acquire`/`shrink_to_fit`/`statistics`，供 fuzz target
///   构建上下文；任何分配失败都会转换为 `protocol.decode` 错误，保持契约一致。
/// - **Trade-offs**：牺牲真实池化的性能，换取确定性与易于追踪的状态。
#[derive(Default)]
pub struct FuzzBufferPool {
    leases: AtomicUsize,
}

impl FuzzBufferPool {
    /// 返回当前累积租借次数，便于在测试中断言资源使用是否符合预期。
    #[must_use]
    pub fn lease_count(&self) -> usize {
        self.leases.load(Ordering::Relaxed)
    }
}

impl BufferPool for FuzzBufferPool {
    fn acquire(&self, min_capacity: usize) -> Result<Box<dyn WritableBuffer>, CoreError> {
        self.leases.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(LinearWritable::with_capacity(min_capacity)))
    }

    fn shrink_to_fit(&self) -> Result<usize, CoreError> {
        // 教案提示：Fuzz 环境下直接返回 0，表明无可回收内存。
        Ok(0)
    }

    fn statistics(&self) -> Result<PoolStats, CoreError> {
        // 提供默认统计结果，避免调用方因 `None` 触发 unwrap panic。
        Ok(PoolStats::default())
    }
}

/// 线性写缓冲，以 `Vec<u8>` 记录写入内容。
///
/// - **Why**：`EncodeContext` 需要 `WritableBuffer` 来聚合帧字节，线性结构足以模拟绝大多数
///   场景并具备可预测的性能行为。
/// - **How**：内部维护 `Vec<u8>`，按需扩容并提供 `reserve`/`put_slice` 等接口；冻结时转换为
///   [`LinearReadable`] 以交还只读视图。
/// - **What**：暴露容量、剩余空间等查询函数，契合 `WritableBuffer` 的语义定义。
/// - **Trade-offs**：忽略零拷贝优化，例如 scatter/gather；在 fuzz 环境下优先保证稳定性。
pub struct LinearWritable {
    data: Vec<u8>,
}

impl LinearWritable {
    /// 以至少 `min_capacity` 的容量构造写缓冲，内部自动纠正为 `>=1`。
    pub fn with_capacity(min_capacity: usize) -> Self {
        Self { data: Vec::with_capacity(min_capacity.max(1)) }
    }
}

impl WritableBuffer for LinearWritable {
    fn capacity(&self) -> usize {
        self.data.capacity()
    }

    fn remaining_mut(&self) -> usize {
        self.capacity().saturating_sub(self.data.len())
    }

    fn written(&self) -> usize {
        self.data.len()
    }

    fn reserve(&mut self, additional: usize) -> Result<(), CoreError> {
        self.data.reserve(additional);
        Ok(())
    }

    fn put_slice(&mut self, src: &[u8]) -> Result<(), CoreError> {
        self.data.extend_from_slice(src);
        Ok(())
    }

    fn write_from(&mut self, src: &mut dyn ReadableBuffer, len: usize) -> Result<(), CoreError> {
        ensure(len <= src.remaining(), "source buffer exhausted")?;
        let mut tmp = vec![0u8; len];
        src.copy_into_slice(&mut tmp)?;
        self.data.extend_from_slice(&tmp);
        Ok(())
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn freeze(self: Box<Self>) -> Result<Box<dyn ReadableBuffer>, CoreError> {
        Ok(Box::new(LinearReadable::from(self.data)))
    }
}

/// 线性只读缓冲：支持顺序读取、分片拆分与复制。
///
/// - **Why**：`DecodeContext` 期望可按需拆分字节序列，线性缓冲通过游标模拟切片移动即可。
/// - **How**：维护 `cursor` 指向下一个未消费字节，`split_to`/`advance` 操作更新游标并返回新视图。
/// - **What**：实现 [`ReadableBuffer`]，在 fuzz 场景下提供稳定、可控的读取行为。
/// - **Trade-offs**：始终持有底层 `Vec<u8>` 所有权，无法零拷贝共享；但这与 fuzz 追求确定性一致。
pub struct LinearReadable {
    data: Vec<u8>,
    cursor: usize,
}

impl From<Vec<u8>> for LinearReadable {
    fn from(data: Vec<u8>) -> Self {
        Self { data, cursor: 0 }
    }
}

impl LinearReadable {
    /// 将自身装箱为 trait 对象，便于传递给 `Codec::decode`。
    pub fn into_box(self) -> Box<dyn ReadableBuffer> {
        Box::new(self)
    }
}

impl ReadableBuffer for LinearReadable {
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.cursor)
    }

    fn chunk(&self) -> &[u8] {
        &self.data[self.cursor..]
    }

    fn split_to(&mut self, len: usize) -> Result<Box<dyn ReadableBuffer>, CoreError> {
        ensure(len <= self.remaining(), "split exceeds remaining bytes")?;
        let end = self.cursor + len;
        let slice = self.data[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(Box::new(LinearReadable::from(slice)))
    }

    fn advance(&mut self, len: usize) -> Result<(), CoreError> {
        ensure(len <= self.remaining(), "advance exceeds remaining bytes")?;
        self.cursor += len;
        Ok(())
    }

    fn copy_into_slice(&mut self, dst: &mut [u8]) -> Result<(), CoreError> {
        ensure(dst.len() <= self.remaining(), "insufficient bytes for copy")?;
        let end = self.cursor + dst.len();
        dst.copy_from_slice(&self.data[self.cursor..end]);
        self.cursor = end;
        Ok(())
    }

    fn try_into_vec(self: Box<Self>) -> Result<Vec<u8>, CoreError> {
        Ok(self.data)
    }
}

/// 模拟分片网络缓冲的视图，实现 `BufView` 以测试碎片化报文。
///
/// - **Why**：RTP/SDP 等解析器需要适配 scatter/gather 缓冲，Fuzz 场景通过显式分片
///   可以覆盖历史上“跨分片越界读取”的缺陷。
/// - **How**：存储 `Vec<Vec<u8>>` 作为每个分片，`as_chunks` 将其转换为 `&[u8]` 切片向量
///   以喂给解码器；`len` 聚合总字节数供边界检查。
/// - **What**：实现 [`BufView`]，可直接传入 `parse_rtp` 等 API；额外提供
///   [`FragmentedView::from_splits`] 用于根据分片尺寸快速构造视图。
/// - **Trade-offs**：调用 `as_chunks` 时复制切片引用，会产生轻微分配开销；但换取
///   与生产环境相符的分片语义。
pub struct FragmentedView {
    fragments: Vec<Vec<u8>>,
    total_len: usize,
}

impl FragmentedView {
    /// 根据分片数组构造视图，忽略空分片以保持逻辑一致性。
    pub fn from_fragments(fragments: Vec<Vec<u8>>) -> Self {
        let mut total = 0usize;
        let filtered: Vec<Vec<u8>> = fragments
            .into_iter()
            .filter(|chunk| !chunk.is_empty())
            .inspect(|chunk| total += chunk.len())
            .collect();
        Self {
            fragments: filtered,
            total_len: total,
        }
    }

    /// 将单片字节流按给定切分点拆分，常用于模拟 MTU 分片。
    pub fn from_splits(bytes: Vec<u8>, splits: &[usize]) -> Self {
        if splits.is_empty() {
            return Self::from_fragments(vec![bytes]);
        }
        let mut offset = 0usize;
        let mut out = Vec::new();
        for &size in splits {
            if offset >= bytes.len() {
                break;
            }
            let end = (offset + size).min(bytes.len());
            out.push(bytes[offset..end].to_vec());
            offset = end;
        }
        if offset < bytes.len() {
            out.push(bytes[offset..].to_vec());
        }
        Self::from_fragments(out)
    }

    /// 返回逻辑长度，供断言或调试使用。
    #[must_use]
    pub fn len(&self) -> usize {
        self.total_len
    }
}

impl BufView for FragmentedView {
    fn as_chunks(&self) -> Chunks<'_> {
        let refs: Vec<&[u8]> = self.fragments.iter().map(|chunk| chunk.as_slice()).collect();
        Chunks::from_vec(refs)
    }

    fn len(&self) -> usize {
        self.total_len
    }
}

/// 统一的错误构造器：违背缓冲契约时返回 `protocol.decode`。
fn ensure(predicate: bool, message: &str) -> Result<(), CoreError> {
    if predicate {
        Ok(())
    } else {
        Err(CoreError::new(codes::PROTOCOL_DECODE, message.to_owned()))
    }
}
