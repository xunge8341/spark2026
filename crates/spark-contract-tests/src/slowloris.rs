use crate::case::{TckCase, TckSuite};
use spark_codec_line::LineDelimitedCodec;
use spark_core::CoreError;
use spark_core::buffer::{BufferPool, PoolStats, ReadableBuffer, WritableBuffer};
use spark_core::codec::{DecodeContext, DecodeOutcome, Decoder};
use spark_core::error::codes;

const CASES: &[TckCase] = &[
    TckCase {
        name: "slow_reader_drains_within_budget",
        test: slow_reader_drains_within_budget,
    },
    TckCase {
        name: "slow_reader_exceeding_budget_is_rejected",
        test: slow_reader_exceeding_budget_is_rejected,
    },
];

const SUITE: TckSuite = TckSuite {
    name: "slowloris",
    cases: CASES,
};

/// 返回“慢连接防护”主题的测试套件。
///
/// # 教案式说明
/// - **意图 (Why)**：验证 `DecodeContext` 在面对慢速读者（Slowloris 攻击典型模型）时，是否能结合
///   编解码器及时拒绝超限帧，避免连接长期占用线程。
/// - **逻辑 (How)**：构造限速缓冲读取器，分阶段向 `LineDelimitedCodec` 暴露字节；观察当可见窗口
///   达到最大帧预算前后的行为差异。
/// - **契约 (What)**：返回 `'static` 套件引用，供宏或手动执行入口使用。
pub const fn suite() -> &'static TckSuite {
    &SUITE
}

/// 模拟慢速客户端在预算范围内逐字节填充完整帧，应当最终成功解码。
///
/// # 教案式拆解
/// - **意图 (Why)**：确保编解码器不会因为数据到达缓慢而提前报错，验证“慢读但合法”场景仍能
///   正常解码，防止误杀低速链路。
/// - **步骤 (How)**：
///   1. 使用 `RateLimitedReadable` 每次仅暴露 2 字节；
///   2. 调用 `decode` 直到返回 `Complete`，期间 `DecodeOutcome::Incomplete` 代表仍在等待；
///   3. 每次未完成时显式“滴流”更多字节，模拟限速网络。
/// - **契约 (What)**：最终解码结果为 `"ping"`，缓冲区无剩余可读字节。
fn slow_reader_drains_within_budget() {
    let codec = LineDelimitedCodec::new();
    let pool = DummyPool;
    let mut ctx = DecodeContext::with_max_frame_size(&pool, Some(8));
    let mut buffer = RateLimitedReadable::new(b"ping\n".to_vec(), 2);

    // 限制循环次数，防止实现异常时出现死循环。
    for _ in 0..8 {
        match codec
            .decode(&mut buffer, &mut ctx)
            .expect("预算内解码不应失败")
        {
            DecodeOutcome::Incomplete => {
                // 追加一批字节，模拟慢速链路刚刚发送更多数据。
                assert!(
                    buffer.reveal_next_window(),
                    "在遇到换行符前应当还有待揭示的数据"
                );
            }
            DecodeOutcome::Complete(text) => {
                assert_eq!(text, "ping", "Slowloris 模型下仍应正确解码完整帧");
                assert_eq!(buffer.remaining(), 0, "消费后不应留下多余可见字节");
                return;
            }
            DecodeOutcome::Skipped => {
                panic!("Slowloris 模型不应触发 Skipped 分支");
            }
            other => panic!("解码 Slowloris 流时遇到未知分支：{other:?}"),
        }
    }

    panic!("限速读者应在预算内完成解码，但仍未返回 Complete");
}

/// 模拟慢速客户端永远不发送换行符，逐步耗尽预算后必须立即拒绝连接。
///
/// # 教案式拆解
/// - **意图 (Why)**：Slowloris 攻击通过缓慢追加字节试图长期占用线程，本用例确保当可见窗口超过
///   帧预算时立刻返回 `protocol.budget_exceeded`，而非继续等待。
/// - **步骤 (How)**：
///   1. 构造不含换行符的 7 字节载荷，预算上限设为 5；
///   2. 每次仅暴露 2 字节，重复调用 `decode`；
///   3. 当可见窗口增长至 6 时，编解码器应立刻抛错。
/// - **契约 (What)**：返回的错误码为 `protocol.budget_exceeded`，表明实现及时拒绝慢读攻击。
fn slow_reader_exceeding_budget_is_rejected() {
    let codec = LineDelimitedCodec::new();
    let pool = DummyPool;
    let mut ctx = DecodeContext::with_max_frame_size(&pool, Some(5));
    let mut buffer = RateLimitedReadable::new(b"abcdefg".to_vec(), 2);

    for _ in 0..8 {
        match codec.decode(&mut buffer, &mut ctx) {
            Ok(DecodeOutcome::Incomplete) => {
                if !buffer.reveal_next_window() {
                    panic!("若仍未触发预算错误，则必须还有新字节等待揭示");
                }
            }
            Ok(DecodeOutcome::Complete(_)) => {
                panic!("慢读攻击不应解码出完整帧");
            }
            Ok(DecodeOutcome::Skipped) => {
                panic!("慢读攻击不应被视作可跳过帧");
            }
            Ok(other) => {
                panic!("慢读攻击返回了未知解码结果：{other:?}");
            }
            Err(err) => {
                assert_eq!(
                    err.code(),
                    codes::PROTOCOL_BUDGET_EXCEEDED,
                    "超出预算时必须返回协议级限流错误"
                );
                return;
            }
        }
    }

    panic!("缓慢泄露字节应触发预算错误，但循环结束仍未报错");
}

/// 只读缓冲实现：维护“当前可见窗口”，逐步向编解码器暴露字节。
///
/// # 教案式说明
/// - **意图 (Why)**：复现 Slowloris 攻击的关键特征——每次仅发送极少量数据，迫使服务端长时间
///   阻塞等待，从而验证框架的防护策略。
/// - **逻辑 (How)**：
///   - `visible` 表示目前对上层可见的末端位置；
///   - `step` 为每次 `reveal_next_window` 扩大的窗口宽度；
///   - `chunk` 始终返回 `[cursor, visible)` 区间，确保未“发送”的字节不会暴露给解码器；
///   - `split_to` 在摘取前缀时返回 `OwnedReadable`，模拟协议层复制帧内容。
/// - **契约 (What)**：所有 `ReadableBuffer` 方法均基于当前可见区间进行边界检查，违反约束时抛出
///   `protocol.decode` 错误，帮助测试精确定位违规访问。
struct RateLimitedReadable {
    data: Vec<u8>,
    cursor: usize,
    visible: usize,
    step: usize,
}

impl RateLimitedReadable {
    /// 构建限速缓冲，初始仅暴露 `step` 字节。
    fn new(data: Vec<u8>, step: usize) -> Self {
        let initial = step.min(data.len());
        Self {
            data,
            cursor: 0,
            visible: initial,
            step: step.max(1),
        }
    }

    /// 向前端暴露下一批字节，返回是否成功扩大窗口。
    fn reveal_next_window(&mut self) -> bool {
        if self.visible >= self.data.len() {
            return false;
        }
        self.visible = (self.visible + self.step).min(self.data.len());
        true
    }
}

impl ReadableBuffer for RateLimitedReadable {
    fn remaining(&self) -> usize {
        self.visible.saturating_sub(self.cursor)
    }

    fn chunk(&self) -> &[u8] {
        &self.data[self.cursor..self.visible]
    }

    fn split_to(&mut self, len: usize) -> spark_core::Result<Box<dyn ReadableBuffer>, CoreError> {
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "split length exceeds visible window",
            ));
        }
        let end = self.cursor + len;
        let slice = self.data[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(Box::new(OwnedReadable::new(slice)))
    }

    fn advance(&mut self, len: usize) -> spark_core::Result<(), CoreError> {
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "advance beyond visible window",
            ));
        }
        self.cursor += len;
        Ok(())
    }

    fn copy_into_slice(&mut self, dst: &mut [u8]) -> spark_core::Result<(), CoreError> {
        if dst.len() > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "copy exceeds visible bytes",
            ));
        }
        let end = self.cursor + dst.len();
        dst.copy_from_slice(&self.data[self.cursor..end]);
        self.cursor = end;
        Ok(())
    }

    fn try_into_vec(self: Box<Self>) -> spark_core::Result<Vec<u8>, CoreError> {
        Ok(self.data[self.cursor..self.visible].to_vec())
    }
}

/// 拆分片段使用的简易只读缓冲，实现完整的 `ReadableBuffer` 契约。
#[derive(Debug, Clone)]
struct OwnedReadable {
    data: Vec<u8>,
    cursor: usize,
}

impl OwnedReadable {
    fn new(data: Vec<u8>) -> Self {
        Self { data, cursor: 0 }
    }
}

impl ReadableBuffer for OwnedReadable {
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.cursor)
    }

    fn chunk(&self) -> &[u8] {
        &self.data[self.cursor..]
    }

    fn split_to(&mut self, len: usize) -> spark_core::Result<Box<dyn ReadableBuffer>, CoreError> {
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "owned buffer split overflow",
            ));
        }
        let end = self.cursor + len;
        let slice = self.data[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(Box::new(OwnedReadable::new(slice)))
    }

    fn advance(&mut self, len: usize) -> spark_core::Result<(), CoreError> {
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "owned buffer advance overflow",
            ));
        }
        self.cursor += len;
        Ok(())
    }

    fn copy_into_slice(&mut self, dst: &mut [u8]) -> spark_core::Result<(), CoreError> {
        if dst.len() > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "owned buffer copy overflow",
            ));
        }
        let end = self.cursor + dst.len();
        dst.copy_from_slice(&self.data[self.cursor..end]);
        self.cursor = end;
        Ok(())
    }

    fn try_into_vec(self: Box<Self>) -> spark_core::Result<Vec<u8>, CoreError> {
        Ok(self.data)
    }
}

/// 测试用缓冲池：解码流程不会租借缓冲，返回统计快照即可。
struct DummyPool;

impl BufferPool for DummyPool {
    fn acquire(
        &self,
        min_capacity: usize,
    ) -> spark_core::Result<Box<dyn WritableBuffer>, CoreError> {
        Err(CoreError::new(
            "buffer.disabled",
            format!("DummyPool 不支持租借缓冲（请求 {min_capacity} 字节）"),
        ))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
        Ok(0)
    }

    fn statistics(&self) -> spark_core::Result<PoolStats, CoreError> {
        Ok(PoolStats::default())
    }
}
