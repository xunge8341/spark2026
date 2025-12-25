use alloc::{format, string::String};

use spark_codecs::CoreError;
use spark_codecs::buffer::ErasedSparkBuf;
use spark_codecs::codes;
use spark_codecs::{
    Codec, CodecDescriptor, ContentEncoding, ContentType, DecodeContext, DecodeOutcome,
    EncodeContext, EncodedPayload,
};

const NEWLINE: u8 = b'\n';

/// 基于换行符的文本编解码器，实现 `spark-core` 的泛型 [`Codec`] 契约。
///
/// # 设计动机（Why）
/// - 在不改动核心 crate 的前提下，验证外部扩展如何复用 `EncodeContext`/`DecodeContext` 与缓冲池契约；
/// - 行分隔文本协议常见于日志流、消息总线示例，语义直观，有助于团队快速对齐扩展点用法；
/// - 编解码逻辑覆盖预算检查、错误上报等关键分支，形成“教案级”参考实现。
///
/// # 行为概览（How）
/// - `encode`：将业务字符串写入缓冲并追加换行符，必要时校验帧大小预算；
/// - `decode`：扫描首个连续字节块，遇到换行符即切分并解析 UTF-8 字符串；
/// - 描述符固定为 `text/plain; charset=utf-8` + `identity` 压缩，可直接注册到 `CodecRegistry`。
///
/// # 契约说明（What）
/// - **输入类型**：入站/出站均为 `String`，满足 `Send + Sync + 'static` 要求；
/// - **前置条件**：调用方需确保输入文本不包含 `\0` 等业务层禁止字符；
/// - **后置条件**：成功编码返回只读缓冲；成功解码返回一行文本并消费原缓冲对应字节。
///
/// # 权衡与风险（Trade-offs）
/// - 仅处理连续缓冲场景，若底层实现为分片缓冲需结合 `DecodeContext::acquire_scratch` 做自定义聚合；
/// - 不提供转义策略，遇到多行或包含 `\n` 的场景应拓展协议或使用长度前缀。
#[derive(Debug, Clone)]
pub struct LineDelimitedCodec {
    descriptor: CodecDescriptor,
}

impl LineDelimitedCodec {
    /// 构建新的换行分帧编解码器实例。
    ///
    /// # 教案式说明
    /// - **Why**：封装 `CodecDescriptor` 构造，避免调用方重复填写内容类型与编码；
    /// - **How**：内部创建 `text/plain; charset=utf-8` 与 `identity` 组合，再存入结构体；
    /// - **What**：返回的实例无状态，可安全在多线程中共享；
    /// - **Trade-offs**：`CodecDescriptor` 使用 `Cow<'static, str>` 牺牲少量堆分配换取灵活命名。
    pub fn new() -> Self {
        let descriptor = CodecDescriptor::new(
            ContentType::new("text/plain; charset=utf-8"),
            ContentEncoding::identity(),
        );
        Self { descriptor }
    }

    /// 返回内部保存的编解码描述符快照。
    pub fn descriptor(&self) -> &CodecDescriptor {
        &self.descriptor
    }
}

impl Default for LineDelimitedCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec for LineDelimitedCodec {
    type Incoming = String;
    type Outgoing = String;

    fn descriptor(&self) -> &CodecDescriptor {
        self.descriptor()
    }

    fn encode(
        &self,
        item: &Self::Outgoing,
        ctx: &mut EncodeContext<'_>,
    ) -> spark_core::Result<EncodedPayload, CoreError> {
        // === 教案级注释 ===
        // Why: 保证编码阶段符合帧预算与缓冲池契约，防止单帧超长拖垮下游。
        // How:
        // 1. 计算换行后帧长，若超出 `EncodeContext` 提供的预算直接返回 `protocol.budget_exceeded`；
        // 2. 从缓冲池租借所需容量的可写缓冲，并写入业务字节 + 换行符；
        // 3. 将缓冲冻结为只读视图，封装成 `EncodedPayload` 返回。
        // What: 输入 `item` 为即将写出的业务字符串，返回只读负载供传输层使用。
        // Trade-offs: 采用一次性 `put_slice`，牺牲部分零拷贝能力换取直观示例；真实场景可结合分片写入优化。
        let required = item.len() + 1; // 追加换行符后的帧长度。

        if let Some(limit) = ctx.max_frame_size()
            && required > limit
        {
            return Err(CoreError::new(
                codes::PROTOCOL_BUDGET_EXCEEDED,
                format!(
                    "line payload length {} exceeds encode budget {}",
                    required - 1,
                    limit
                ),
            ));
        }

        let mut buffer = ctx.acquire_buffer(required)?;
        buffer.put_slice(item.as_bytes())?;
        buffer.put_slice(&[NEWLINE])?;
        let frozen = buffer.freeze()?;
        Ok(EncodedPayload::from_buffer(frozen))
    }

    fn decode(
        &self,
        src: &mut ErasedSparkBuf,
        ctx: &mut DecodeContext<'_>,
    ) -> spark_core::Result<DecodeOutcome<Self::Incoming>, CoreError> {
        // === 教案级注释 ===
        // Why: 解码阶段需要在不复制整个缓冲的前提下识别行结束符，同时保证预算与 UTF-8 有效性。
        // How:
        // 1. 观察当前连续字节块（`chunk`），查找首个 `\n` 位置；
        // 2. 若找到换行符，使用 `split_to` 拆分对应帧，并通过 `try_into_vec` + `String::from_utf8` 构造业务字符串；
        // 3. 若未找到则返回 `Incomplete`，等待上层拼齐完整帧；
        // 4. 整个过程中校验帧长不超过 `DecodeContext` 提供的预算，避免攻击者构造超长行。
        // What: 输入 `src` 为待解码缓冲，返回 `DecodeOutcome::Complete(String)` 或 `Incomplete`。
        // Trade-offs: 示例假设底层缓冲为连续内存块；若实现返回分片，可在外层结合 scratch 缓冲重组后再调用本函数。
        let chunk = src.chunk();
        if chunk.is_empty() {
            return Ok(DecodeOutcome::Incomplete);
        }

        if let Some(pos) = chunk.iter().position(|byte| *byte == NEWLINE) {
            match ctx.max_frame_size() {
                Some(limit) if pos > limit => {
                    return Err(CoreError::new(
                        codes::PROTOCOL_BUDGET_EXCEEDED,
                        format!(
                            "line payload length {} exceeds decode budget {}",
                            pos, limit
                        ),
                    ));
                }
                _ => {}
            }

            let frame_len = pos + 1; // 带换行符长度。
            let frame = src.split_to(frame_len)?;
            let mut raw = frame.try_into_vec()?;
            match raw.pop() {
                Some(byte) if byte == NEWLINE => {
                    return match String::from_utf8(raw) {
                        Ok(text) => Ok(DecodeOutcome::Complete(text)),
                        Err(err) => Err(CoreError::new(
                            codes::PROTOCOL_DECODE,
                            format!("line payload is not valid UTF-8: {}", err),
                        )),
                    };
                }
                _ => {
                    return Err(CoreError::new(
                        codes::PROTOCOL_DECODE,
                        "line frame missing trailing newline",
                    ));
                }
            }
        }

        if let Some(limit) = ctx.max_frame_size()
            && chunk.len() > limit
        {
            return Err(CoreError::new(
                codes::PROTOCOL_BUDGET_EXCEEDED,
                format!(
                    "current chunk length {} exceeds decode budget {} before newline",
                    chunk.len(),
                    limit
                ),
            ));
        }

        Ok(DecodeOutcome::Incomplete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec::Vec;
    use core::mem::MaybeUninit;
    use spark_codecs::buffer::{BufferPool, PoolStats, ReadableBuffer, WritableBuffer};
    use spark_codecs::{DecodeContext, DecodeOutcome, EncodeContext};

    /// 简易内存缓冲池，向编码/解码上下文提供堆分配缓冲。
    ///
    /// # 教案说明
    /// - **Why**：在测试环境中复现缓冲池交互，验证扩展能否在零依赖情况下复用 `BufferAllocator` 契约；
    /// - **How**：每次租借返回新的 `Vec` 包装实现，统计接口则返回缺省值以降低干扰；
    /// - **What**：满足 `BufferPool` trait，便于直接传入 `EncodeContext`/`DecodeContext`；
    /// - **Trade-offs**：不维护共享状态，无法覆盖真实池化行为，但足以支撑单元测试。
    #[derive(Default)]
    struct TestBufferPool;

    impl BufferPool for TestBufferPool {
        fn acquire(
            &self,
            min_capacity: usize,
        ) -> spark_core::Result<Box<dyn WritableBuffer>, CoreError> {
            // Why: 为编码侧租借足够容量的缓冲，避免测试因容量不足失败。
            // How: 创建预留 `min_capacity` 的 `Vec<u8>` 并包装为 `TestWritable`。
            // What: 返回实现 `WritableBuffer` 的对象安全盒子。
            // Trade-offs: 每次租借都分配新 `Vec`，未模拟池复用。
            Ok(Box::new(TestWritable {
                data: Vec::with_capacity(min_capacity),
            }))
        }

        fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
            Ok(0)
        }

        fn statistics(&self) -> spark_core::Result<PoolStats, CoreError> {
            Ok(PoolStats::default())
        }
    }

    /// 可写缓冲实现，内部使用 `Vec<u8>` 存放数据并支持冻结为只读视图。
    struct TestWritable {
        data: Vec<u8>,
    }

    impl WritableBuffer for TestWritable {
        fn capacity(&self) -> usize {
            self.data.capacity()
        }

        fn remaining_mut(&self) -> usize {
            self.capacity() - self.data.len()
        }

        fn written(&self) -> usize {
            self.data.len()
        }

        fn reserve(&mut self, additional: usize) -> spark_core::Result<(), CoreError> {
            self.data
                .try_reserve(additional)
                .map_err(|_| CoreError::new("buffer.reserve_failed", "Vec reserve failed"))
        }

        fn put_slice(&mut self, src: &[u8]) -> spark_core::Result<(), CoreError> {
            self.data.extend_from_slice(src);
            Ok(())
        }

        fn write_from(
            &mut self,
            src: &mut dyn ReadableBuffer,
            len: usize,
        ) -> spark_core::Result<(), CoreError> {
            let segment = src.split_to(len)?;
            let chunk = segment.try_into_vec()?;
            self.data.extend_from_slice(&chunk);
            Ok(())
        }

        fn chunk_mut(&mut self) -> &mut [MaybeUninit<u8>] {
            if self.data.len() == self.data.capacity() {
                // 触发增长以保证至少存在一个待写区域。
                self.data.reserve(1);
            }
            let spare = self.data.capacity().saturating_sub(self.data.len());
            if spare == 0 {
                return empty_mut_slice();
            }
            unsafe {
                let start = self.data.as_mut_ptr().add(self.data.len()) as *mut MaybeUninit<u8>;
                core::slice::from_raw_parts_mut(start, spare)
            }
        }

        fn advance_mut(&mut self, len: usize) -> spark_core::Result<(), CoreError> {
            let spare = self.data.capacity().saturating_sub(self.data.len());
            if len > spare {
                return Err(CoreError::new(
                    "buffer.out_of_range",
                    "advance_mut beyond remaining capacity",
                ));
            }
            unsafe {
                let new_len = self.data.len() + len;
                self.data.set_len(new_len);
            }
            Ok(())
        }

        fn clear(&mut self) {
            self.data.clear();
        }

        fn freeze(self: Box<Self>) -> spark_core::Result<Box<dyn ReadableBuffer>, CoreError> {
            Ok(Box::new(TestReadable {
                data: self.data,
                read: 0,
            }))
        }
    }

    /// 只读缓冲实现，支持分割与复制，便于模拟运行时的对象安全缓冲。
    struct TestReadable {
        data: Vec<u8>,
        read: usize,
    }

    impl TestReadable {
        fn from_bytes(bytes: Vec<u8>) -> Self {
            Self {
                data: bytes,
                read: 0,
            }
        }
    }

    impl ReadableBuffer for TestReadable {
        fn remaining(&self) -> usize {
            self.data.len().saturating_sub(self.read)
        }

        fn chunk(&self) -> &[u8] {
            &self.data[self.read..]
        }

        fn split_to(
            &mut self,
            len: usize,
        ) -> spark_core::Result<Box<dyn ReadableBuffer>, CoreError> {
            if len > self.remaining() {
                return Err(CoreError::new(
                    "buffer.out_of_range",
                    "split_to beyond remaining",
                ));
            }
            let end = self.read + len;
            let segment = self.data[self.read..end].to_vec();
            self.read = end;
            Ok(Box::new(TestReadable {
                data: segment,
                read: 0,
            }))
        }

        fn advance(&mut self, len: usize) -> spark_core::Result<(), CoreError> {
            if len > self.remaining() {
                return Err(CoreError::new(
                    "buffer.out_of_range",
                    "advance beyond remaining",
                ));
            }
            self.read += len;
            Ok(())
        }

        fn copy_into_slice(&mut self, dst: &mut [u8]) -> spark_core::Result<(), CoreError> {
            if dst.len() > self.remaining() {
                return Err(CoreError::new(
                    "buffer.out_of_range",
                    "copy_into_slice beyond remaining",
                ));
            }
            let end = self.read + dst.len();
            dst.copy_from_slice(&self.data[self.read..end]);
            self.read = end;
            Ok(())
        }

        fn try_into_vec(self: Box<Self>) -> spark_core::Result<Vec<u8>, CoreError> {
            let TestReadable { data, read } = *self;
            Ok(data[read..].to_vec())
        }
    }

    fn empty_mut_slice() -> &'static mut [MaybeUninit<u8>] {
        static mut EMPTY: [MaybeUninit<u8>; 0] = [];
        unsafe { &mut EMPTY[..] }
    }

    #[test]
    fn encode_appends_newline() {
        // Why: 确认编码逻辑正确追加换行符并遵守缓冲池契约。
        // How: 使用测试缓冲池创建 `EncodeContext`，对字符串编码并检查输出字节。
        let codec = LineDelimitedCodec::new();
        let pool = TestBufferPool;
        let mut ctx = EncodeContext::new(&pool);
        let payload = codec
            .encode(&"hello".to_string(), &mut ctx)
            .expect("encode succeeds");
        let buffer = payload.into_buffer();
        let bytes = buffer.try_into_vec().expect("convert to vec");
        assert_eq!(bytes, b"hello\n");
    }

    #[test]
    fn decode_returns_complete_when_newline_present() {
        // Why: 验证解码器在检测到换行符时能返回完整业务字符串并消费对应字节。
        // How: 构造包含两行的缓冲，调用 `decode` 应返回首行，同时剩余缓冲只保留第二行。
        let codec = LineDelimitedCodec::new();
        let pool = TestBufferPool;
        let mut src: Box<ErasedSparkBuf> =
            Box::new(TestReadable::from_bytes(b"hello\nworld\n".to_vec()));
        let mut ctx = DecodeContext::new(&pool);
        let outcome = codec.decode(&mut *src, &mut ctx).expect("decode succeeds");
        match outcome {
            DecodeOutcome::Complete(text) => assert_eq!(text, "hello"),
            other => panic!("unexpected outcome: {:?}", other),
        }
        let remaining = src.try_into_vec().expect("take remaining bytes");
        assert_eq!(remaining, b"world\n");
    }

    #[test]
    fn decode_marks_incomplete_without_newline() {
        // Why: 确认当缓冲缺少换行符时，解码器不会消耗数据而是提示继续等待。
        let codec = LineDelimitedCodec::new();
        let pool = TestBufferPool;
        let mut src: Box<ErasedSparkBuf> = Box::new(TestReadable::from_bytes(b"partial".to_vec()));
        let mut ctx = DecodeContext::new(&pool);
        let outcome = codec.decode(&mut *src, &mut ctx).expect("decode ok");
        assert!(matches!(outcome, DecodeOutcome::Incomplete));
        let remaining = src.try_into_vec().expect("take remaining");
        assert_eq!(remaining, b"partial");
    }

    #[test]
    fn decode_respects_frame_budget() {
        // Why: 验证帧预算超限时会返回 `protocol.budget_exceeded` 错误，防止畸形输入拖垮系统。
        let codec = LineDelimitedCodec::new();
        let pool = TestBufferPool;
        let mut src: Box<ErasedSparkBuf> =
            Box::new(TestReadable::from_bytes(b"toolong\n".to_vec()));
        let mut ctx = DecodeContext::with_max_frame_size(&pool, Some(3));
        let err = codec
            .decode(&mut *src, &mut ctx)
            .expect_err("expect budget error");
        assert_eq!(err.code(), codes::PROTOCOL_BUDGET_EXCEEDED);
    }
}
