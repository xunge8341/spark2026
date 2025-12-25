//! 最小可运行示例：使用自定义缓冲池跑通 `LineDelimitedCodec` 的一次编码/解码往返。
//!
//! # 设计目的 (Why)
//! - 向新同学展示“最少概念”即可运行的端到端流程：缓冲池 → 上下文 → Codec。
//! - 解释 `spark-core` 关键契约的输入/输出要求，降低实现自定义组件的心智负担。
//! - 为 `docs/getting-started.md` 的步骤提供可验证的样例，确保文档和代码同步。
//!
//! # 使用方式 (How)
//! ```bash
//! cargo run -p spark-codec-line --example minimal
//! ```
//! 输出示例：`[spark-codec-line/minimal] encoded=12 bytes, decoded="hello spark"`
//!
//! # 契约说明 (What)
//! - 依赖 `spark-core` 暴露的公开 trait，不需要访问任何内部模块。
//! - 示例通过 `SimpleBufferPool` 提供最小化的 `BufferPool`、`WritableBuffer`、`ReadableBuffer` 实现。
//! - 输入字符串为 `hello spark`，成功执行后将打印编码字节数与解码结果。
//!
//! # 注意事项 (Trade-offs & Gotchas)
//! - Mock 缓冲池采用堆分配，未做零拷贝优化；真实系统应替换为池化实现。
//! - 错误路径统一返回 `CoreError`，便于与框架其他组件协同；示例中遇到 `DecodeOutcome::Incomplete` 会转换为错误退出。

use core::mem::MaybeUninit;

use spark_codec_line::LineDelimitedCodec;
use spark_codecs::buffer::{BufferPool, PoolStats, ReadableBuffer, WritableBuffer};
use spark_codecs::{Codec, CoreError, DecodeContext, DecodeOutcome, EncodeContext, codes};
use std::mem;

/// `SimpleBufferPool` 提供最简 Mock，实现 `BufferPool` 契约以支撑示例运行。
///
/// # 设计动机 (Why)
/// - 将“缓冲池”概念具体化，帮助理解 `EncodeContext`/`DecodeContext` 所需的输入资源。
/// - 使用无状态结构体，降低首次实现时的复杂度。
///
/// # 行为概览 (How)
/// - `acquire` 返回基于 `Vec<u8>` 的可写缓冲；
/// - `shrink_to_fit`、`statistics` 返回默认值，保持接口完整；
/// - 通过泛型实现复用 `BufferPool` 与 `BufferAllocator` 的 blanket 实现。
///
/// # 契约说明 (What)
/// - `min_capacity` 会转化为缓冲的初始容量；
/// - 返回的缓冲满足 `Send + Sync + 'static` 要求，可被上下文持久使用。
///
/// # 权衡提示 (Trade-offs)
/// - 无内存复用策略，适合教学/测试；实际部署需换成锁自由池或分级分配器。
#[derive(Default, Clone, Copy)]
struct SimpleBufferPool;

impl BufferPool for SimpleBufferPool {
    fn acquire(
        &self,
        min_capacity: usize,
    ) -> spark_core::Result<Box<dyn WritableBuffer>, CoreError> {
        // 教案级注释：
        // Why: `EncodeContext`/`DecodeContext` 需要通过该方法租借缓冲，否则无法写入/读取帧。
        // How: 直接构造 `VecWritableBuffer`，并确保容量至少覆盖调用方要求的最小值。
        // What: 返回 `Box<dyn WritableBuffer>`，所有权交给调用方；错误分支在真实实现中用于 OOM/背压提示。
        // Trade-offs: 采用 `Vec::with_capacity`，舍弃池化换取最小实现；真实场景应加上租借统计与重用。
        Ok(Box::new(VecWritableBuffer::with_capacity(min_capacity)))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
        // Why: 契约要求提供主动收缩接口；示例中无状态，因此直接返回 0。
        // How: 返回 `Ok(0)` 表示未执行任何回收逻辑。
        // What: 调用方可忽略返回值；在真实实现中应记录回收字节数。
        Ok(0)
    }

    fn statistics(&self) -> spark_core::Result<PoolStats, CoreError> {
        // Why: 观测性接口要求返回池快照，示例侧用默认值提供“结构化字段”示例。
        // How: 直接使用 `PoolStats::default()`，其中所有字段为 0。
        // What: 真实实现应填充 `allocated_bytes` 等核心指标。
        Ok(PoolStats::default())
    }
}

/// 可写缓冲的最小实现，内部使用 `Vec<u8>` 存储已写入数据。
///
/// # 设计动机 (Why)
/// - 提供一个一目了然的参考，说明 `WritableBuffer` 需要维护容量、已写长度与冻结语义。
///
/// # 行为概览 (How)
/// - `put_slice` 直接向 `Vec` 追加字节；
/// - `write_from` 借助 `ReadableBuffer::copy_into_slice` 完成跨缓冲复制；
/// - `freeze` 将内部 `Vec` 转换为只读缓冲。
///
/// # 契约说明 (What)
/// - `with_capacity` 确保初始容量满足上下文需求；
/// - 所有写入操作在成功后保证 `written()` 立刻可见。
///
/// # 权衡提示 (Trade-offs)
/// - 未实现任何零拷贝优化；`freeze` 始终移动所有权。
struct VecWritableBuffer {
    buf: Vec<u8>,
}

impl VecWritableBuffer {
    /// 依据最小容量创建缓冲。
    fn with_capacity(min_capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(min_capacity),
        }
    }
}

impl WritableBuffer for VecWritableBuffer {
    fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    fn remaining_mut(&self) -> usize {
        self.buf.capacity().saturating_sub(self.buf.len())
    }

    fn written(&self) -> usize {
        self.buf.len()
    }

    fn reserve(&mut self, additional: usize) -> spark_core::Result<(), CoreError> {
        // Why: 确保后续写入不会因容量不足失败。
        // How: 调用 `Vec::reserve` 扩充容量；`Vec` 会在必要时重新分配并保留已有数据。
        // What: 约定返回 `Ok(())` 代表容量满足需求。
        self.buf.reserve(additional);
        Ok(())
    }

    fn put_slice(&mut self, src: &[u8]) -> spark_core::Result<(), CoreError> {
        // Why: 将业务数据写入缓冲末尾，是编码阶段最常见的操作。
        // How: 直接使用 `Vec::extend_from_slice` 追加字节。
        // What: 成功后 `written()` 自增，调用方可继续申请更多空间。
        self.buf.extend_from_slice(src);
        Ok(())
    }

    fn write_from(
        &mut self,
        src: &mut dyn ReadableBuffer,
        len: usize,
    ) -> spark_core::Result<(), CoreError> {
        // Why: 支持从已有只读缓冲复制数据，满足零拷贝降级需求。
        // How: 先校验 `len` 是否小于等于剩余字节，再使用临时 `Vec` 中转，最后写入当前缓冲。
        // What: 若 `len` 超出范围或底层复制失败，将返回 `CoreError`。
        if len > src.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                format!(
                    "write_from requested {} bytes but source only has {}",
                    len,
                    src.remaining()
                ),
            ));
        }
        let mut temp = vec![0u8; len];
        src.copy_into_slice(&mut temp)?;
        self.buf.extend_from_slice(&temp);
        Ok(())
    }

    fn chunk_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        if self.buf.len() == self.buf.capacity() {
            self.buf.reserve(1);
        }
        let spare = self.buf.capacity().saturating_sub(self.buf.len());
        if spare == 0 {
            return empty_mut_slice();
        }
        unsafe {
            let start = self.buf.as_mut_ptr().add(self.buf.len()) as *mut MaybeUninit<u8>;
            core::slice::from_raw_parts_mut(start, spare)
        }
    }

    fn advance_mut(&mut self, len: usize) -> spark_core::Result<(), CoreError> {
        let spare = self.buf.capacity().saturating_sub(self.buf.len());
        if len > spare {
            return Err(CoreError::new(
                codes::APP_INVALID_ARGUMENT,
                format!(
                    "advance_mut requested {} bytes but only {} spare",
                    len, spare
                ),
            ));
        }
        unsafe {
            let new_len = self.buf.len() + len;
            self.buf.set_len(new_len);
        }
        Ok(())
    }

    fn clear(&mut self) {
        // Why: 允许调用方重复利用缓冲。
        // How: 调用 `Vec::clear`，保留容量避免重复分配。
        self.buf.clear();
    }

    fn freeze(self: Box<Self>) -> spark_core::Result<Box<dyn ReadableBuffer>, CoreError> {
        // Why: 将可写缓冲转换为只读视图，供传输层或解码阶段消费。
        // How: 通过 `mem::take` 拿到内部 `Vec`，并构造 `VecReadableBuffer`。
        // What: 返回值实现 `ReadableBuffer`，读指针从开头开始。
        let mut this = *self;
        let data = mem::take(&mut this.buf);
        Ok(Box::new(VecReadableBuffer::from_vec(data)))
    }
}

fn empty_mut_slice() -> &'static mut [MaybeUninit<u8>] {
    static mut EMPTY: [MaybeUninit<u8>; 0] = [];
    unsafe { &mut EMPTY[..] }
}

/// 只读缓冲的最小实现，用于演示如何满足 `ReadableBuffer` 契约。
///
/// # 设计动机 (Why)
/// - 解释“读指针”“拆分”“扁平化”在契约中的具体行为。
///
/// # 行为概览 (How)
/// - 通过 `cursor` 追踪当前读取位置；
/// - `split_to` 复制所需片段并前移指针；
/// - `try_into_vec` 返回剩余字节的拥有型向量。
///
/// # 契约说明 (What)
/// - `from_vec` 接收 `Vec<u8>` 并假设读指针从 0 开始；
/// - 所有操作都要保证 `remaining()` 与实际数据一致。
///
/// # 权衡提示 (Trade-offs)
/// - 使用复制实现 `split_to`，简化教学；真实系统可结合引用计数避免重复分配。
struct VecReadableBuffer {
    data: Vec<u8>,
    cursor: usize,
}

impl VecReadableBuffer {
    fn from_vec(data: Vec<u8>) -> Self {
        Self { data, cursor: 0 }
    }
}

impl ReadableBuffer for VecReadableBuffer {
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.cursor)
    }

    fn chunk(&self) -> &[u8] {
        &self.data[self.cursor..]
    }

    fn split_to(&mut self, len: usize) -> spark_core::Result<Box<dyn ReadableBuffer>, CoreError> {
        // Why: 支持按帧拆分缓冲（例如协议将前 N 字节视为头部）。
        // How: 校验 `len` 合法后复制对应片段，更新当前游标。
        // What: 返回新的只读缓冲；若长度不足则返回错误。
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                format!(
                    "split_to requested {} bytes but remaining is {}",
                    len,
                    self.remaining()
                ),
            ));
        }
        let end = self.cursor + len;
        let slice = self.data[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(Box::new(VecReadableBuffer::from_vec(slice)))
    }

    fn advance(&mut self, len: usize) -> spark_core::Result<(), CoreError> {
        // Why: 在无需保留拆分结果时前移读指针。
        // How: 与 `split_to` 相同，先校验后更新游标。
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                format!(
                    "advance requested {} bytes but remaining is {}",
                    len,
                    self.remaining()
                ),
            ));
        }
        self.cursor += len;
        Ok(())
    }

    fn copy_into_slice(&mut self, dst: &mut [u8]) -> spark_core::Result<(), CoreError> {
        // Why: 兼容需要平坦字节数组的场景（如外部 FFI）。
        // How: 校验长度后复制，并推进读指针。
        if dst.len() > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                format!(
                    "copy_into_slice requested {} bytes but remaining is {}",
                    dst.len(),
                    self.remaining()
                ),
            ));
        }
        let end = self.cursor + dst.len();
        dst.copy_from_slice(&self.data[self.cursor..end]);
        self.cursor = end;
        Ok(())
    }

    fn try_into_vec(self: Box<Self>) -> spark_core::Result<Vec<u8>, CoreError> {
        // Why: 将剩余数据打包为拥有型向量，常用于日志或跨语言传输。
        // How: 取出结构体所有权，并使用 `split_off` 返回剩余字节。
        let mut this = *self;
        Ok(this.data.split_off(this.cursor))
    }
}

/// 程序入口：构造上下文、执行编码与解码，并打印结果。
///
/// # 设计动机 (Why)
/// - 串联所有组件，证明“最小概念集合”即可运行端到端流程。
///
/// # 执行流程 (How)
/// 1. 初始化缓冲池与 Codec；
/// 2. 使用 `EncodeContext` 编码消息并记录帧长度；
/// 3. 将生成的缓冲传入 `DecodeContext` 解码；
/// 4. 打印最终结果。
///
/// # 契约说明 (What)
/// - 返回 `Result`，便于在脚本或 CI 中直接判断成功与否；
/// - 遇到 `DecodeOutcome::Incomplete` 会转化为 `CoreError`，确保示例输出明确。
fn main() {
    if let Err(error) = run() {
        eprintln!(
            "[spark-codec-line/minimal] error: [{}] {}",
            error.code(),
            error.message()
        );
        std::process::exit(1);
    }
}

/// 核心逻辑封装在独立函数中，方便单元测试或后续扩展。
fn run() -> spark_core::Result<(), CoreError> {
    let pool = SimpleBufferPool;
    let codec = LineDelimitedCodec::new();

    let payload = "hello spark".to_string();
    let mut encode_ctx = EncodeContext::new(&pool);
    let encoded = codec.encode(&payload, &mut encode_ctx)?;

    let mut buffer = encoded.into_buffer();
    let encoded_len = buffer.remaining();

    let mut decode_ctx = DecodeContext::new(&pool);
    let decoded = match codec.decode(buffer.as_mut(), &mut decode_ctx)? {
        DecodeOutcome::Complete(text) => text,
        DecodeOutcome::Incomplete => {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "unexpected incomplete frame in minimal example",
            ));
        }
        DecodeOutcome::Skipped => {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "codec skipped frame in minimal example",
            ));
        }
        _ => {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "codec returned unknown outcome in minimal example",
            ));
        }
    };

    println!(
        "[spark-codec-line/minimal] encoded={} bytes, decoded=\"{}\"",
        encoded_len, decoded
    );

    Ok(())
}
