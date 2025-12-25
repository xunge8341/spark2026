use core::time::Duration;

use crate::observability::{
    AttributeSet, InstrumentDescriptor, MetricsProvider, metrics::contract::codec as contract,
};

/// 编解码阶段枚举，区分 Encode 与 Decode。
///
/// # 设计动机（Why）
/// - 指标契约要求所有编解码指标通过 `codec.mode` 区分方向；
/// - 通过枚举集中映射到描述符，减少在实现中手动选择指标的样板代码。
///
/// # 契约说明（What）
/// - `Encode`：表示业务对象序列化为字节的阶段；
/// - `Decode`：表示从字节反序列化为业务对象的阶段；
/// - **前置条件**：调用方需确保传入的属性集合已经携带 `codec.name`、`codec.mode` 等标签；
/// - **后置条件**：辅助方法会返回契约规定的描述符或标签值，供指标录入使用。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecPhase {
    Encode,
    Decode,
}

impl CodecPhase {
    /// 获取对应的耗时直方图描述符。
    #[inline]
    fn duration_descriptor(&self) -> &'static InstrumentDescriptor<'static> {
        match self {
            CodecPhase::Encode => &contract::ENCODE_DURATION,
            CodecPhase::Decode => &contract::DECODE_DURATION,
        }
    }

    /// 获取对应的字节计数器描述符。
    #[inline]
    fn bytes_descriptor(&self) -> &'static InstrumentDescriptor<'static> {
        match self {
            CodecPhase::Encode => &contract::ENCODE_BYTES,
            CodecPhase::Decode => &contract::DECODE_BYTES,
        }
    }

    /// 获取对应的错误计数器描述符。
    #[inline]
    fn errors_descriptor(&self) -> &'static InstrumentDescriptor<'static> {
        match self {
            CodecPhase::Encode => &contract::ENCODE_ERRORS,
            CodecPhase::Decode => &contract::DECODE_ERRORS,
        }
    }

    /// 返回契约中预定义的 `codec.mode` 标签值。
    #[inline]
    pub fn mode_label(self) -> &'static str {
        match self {
            CodecPhase::Encode => contract::MODE_ENCODE,
            CodecPhase::Decode => contract::MODE_DECODE,
        }
    }
}

/// 编解码指标挂钩。
///
/// # 设计动机（Why）
/// - 为编解码实现提供最小样板，统一记录耗时、字节量与错误计数；
/// - 封装与 [`MetricsProvider`] 的交互，允许未来在内部增加批量缓冲或线程本地优化。
///
/// # 契约说明（What）
/// - `CodecMetricsHook` 不持有所有权，仅借用 `MetricsProvider`；
/// - 需在编码/解码流程的关键节点调用对应方法；
/// - **前置条件**：属性集合必须限制在契约提供的标签范围内，避免高基数；
/// - **后置条件**：调用后指标即刻写入或进入后端缓冲。
pub struct CodecMetricsHook<'a> {
    provider: &'a dyn MetricsProvider,
}

impl<'a> CodecMetricsHook<'a> {
    /// 构造编解码指标挂钩。
    pub fn new(provider: &'a dyn MetricsProvider) -> Self {
        Self { provider }
    }

    /// 记录单次编解码操作的耗时。
    ///
    /// # 调用契约
    /// - **输入**：`phase` 决定使用编码或解码直方图；`duration` 为操作耗时；
    /// - **前置条件**：应在操作结束时调用，确保涵盖完整执行时间；
    /// - **后置条件**：对应的直方图累加一个样本（毫秒单位）。
    pub fn record_duration(
        &self,
        phase: CodecPhase,
        duration: Duration,
        attributes: AttributeSet<'_>,
    ) {
        let duration_ms = duration.as_secs_f64() * 1_000.0;
        self.provider
            .record_histogram(phase.duration_descriptor(), duration_ms, attributes);
    }

    /// 记录编解码后产生或消费的字节量。
    ///
    /// # 调用契约
    /// - **输入**：`bytes` 为本次操作处理的字节数量；
    /// - **前置条件**：允许重复调用，使用增量值累加；
    /// - **后置条件**：相应计数器累加 `bytes`，用于统计吞吐量。
    pub fn record_bytes(&self, phase: CodecPhase, bytes: u64, attributes: AttributeSet<'_>) {
        self.provider
            .record_counter_add(phase.bytes_descriptor(), bytes, attributes);
    }

    /// 记录一次编解码错误。
    ///
    /// # 调用契约
    /// - **输入**：`attributes` 需额外包含 `error.kind`，标识错误分类；
    /// - **前置条件**：在错误被抛出或返回前调用，以确保失败分支被捕获；
    /// - **后置条件**：相应的错误计数器累加 1。
    pub fn record_error(&self, phase: CodecPhase, attributes: AttributeSet<'_>) {
        self.provider
            .record_counter_add(phase.errors_descriptor(), 1, attributes);
    }
}
