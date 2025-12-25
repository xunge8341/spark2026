use super::attributes::AttributeSet;
use alloc::sync::Arc;

use crate::sealed::Sealed;

/// 指标仪表的元数据描述。
///
/// # 设计背景（Why）
/// - 吸收 OpenTelemetry Instrument Descriptor 的设计，统一指标名称、描述与单位的声明方式。
/// - 通过借用方式传递元数据，避免在热路径重复分配字符串。
///
/// # 契约说明（What）
/// - `name` 必须遵循 `namespace.metric_name` 的蛇形命名，并保持全局唯一。
/// - `description` 建议提供人类可读说明，便于文档化；`unit` 遵循 UCUM 或惯用单位（如 `ms`、`bytes`）。
/// - **前置条件**：调用方需在创建仪表前完成命名规范审查，避免产生重复指标。
/// - **后置条件**：元数据仅在调用期间有效，实现方不得持久化引用；如需持久化应克隆为拥有所有权的副本。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstrumentDescriptor<'a> {
    pub name: &'a str,
    pub description: Option<&'a str>,
    pub unit: Option<&'a str>,
}

impl<'a> InstrumentDescriptor<'a> {
    /// 构造元数据描述。
    pub const fn new(name: &'a str) -> Self {
        Self {
            name,
            description: None,
            unit: None,
        }
    }

    /// 附加说明文本。
    pub const fn with_description(mut self, description: &'a str) -> Self {
        self.description = Some(description);
        self
    }

    /// 附加单位信息。
    pub const fn with_unit(mut self, unit: &'a str) -> Self {
        self.unit = Some(unit);
        self
    }
}

/// 单调递增计数器。
///
/// # 设计背景（Why）
/// - 参照 Prometheus Counter、OpenTelemetry `Counter` 设计，记录事件发生次数或累计量。
///
/// # 契约说明（What）
/// - **前置条件**：`value` 必须非负，且调用方需保证单调递增语义。
/// - **后置条件**：实现必须在内部保证线程安全；若底层后端暂不可用，应丢弃或缓存数据而非阻塞热路径。
///
/// # 风险提示（Trade-offs）
/// - 高频调用建议批量聚合后再提交，避免对远端后端造成压力。
pub trait Counter: Send + Sync + Sealed {
    /// 累加指标值。
    fn add(&self, value: u64, attributes: AttributeSet<'_>);

    /// 累加 1 的便捷方法。
    fn increment(&self, attributes: AttributeSet<'_>) {
        self.add(1, attributes);
    }
}

/// 可上下波动的测量仪表。
///
/// # 设计背景（Why）
/// - 对应 OpenTelemetry `UpDownCounter` 与 Prometheus Gauge，适用于连接数、队列长度等瞬时值。
///
/// # 契约说明（What）
/// - **前置条件**：`value` 可正可负，调用方需确保最终值在业务允许范围内。
/// - **后置条件**：实现需具备线程安全；建议提供无锁或细粒度锁优化。
///
/// # 风险提示（Trade-offs）
/// - 若底层实现使用互斥锁，在高频场景可能成为瓶颈；可结合原子类型或分片计数器优化。
pub trait Gauge: Send + Sync + Sealed {
    /// 直接设置数值。
    fn set(&self, value: f64, attributes: AttributeSet<'_>);

    /// 增加数值。
    fn increment(&self, delta: f64, attributes: AttributeSet<'_>);

    /// 减少数值。
    fn decrement(&self, delta: f64, attributes: AttributeSet<'_>);
}

/// 直方图指标。
///
/// # 设计背景（Why）
/// - 对齐 OpenTelemetry `Histogram`，记录延迟、大小等分布数据，支持 P99 等百分位分析。
///
/// # 契约说明（What）
/// - **前置条件**：`value` 应满足业务语义（如延迟 >= 0）。
/// - **后置条件**：实现可选择固定桶、指数桶或高精度直方图，只要能保证顺序一致性。
///
/// # 风险提示（Trade-offs）
/// - 高精度配置可能带来内存与 CPU 开销；建议根据场景权衡桶数量。
pub trait Histogram: Send + Sync + Sealed {
    /// 记录样本值。
    fn record(&self, value: f64, attributes: AttributeSet<'_>);
}

/// 指标提供者的抽象。
///
/// # 设计背景（Why）
/// - 统一各类后端（Prometheus、StatsD、OpenTelemetry Collector）的仪表创建流程。
/// - 兼容学术研究中的自定义后端，允许注入实验性聚合算法。
///
/// # 契约说明（What）
/// - **前置条件**：调用方在创建仪表前需提供符合规范的 [`InstrumentDescriptor`]。
/// - **后置条件**：返回的仪表实例应可长期使用；实现可缓存实例避免重复创建。
/// - **直接记录**：对于性能敏感路径，可使用 `record_*` 快捷方法，避免持有 `Arc` 并降低虚函数调用次数。
/// - **兼容性**：默认实现回退到 `counter/gauge/histogram` 构造方法，以便旧版实现无需修改即可获得新接口。
///
/// # 风险提示（Trade-offs）
/// - 后端初始化失败时，建议返回降级实现（如空操作仪表），而非直接 panic。
pub trait MetricsProvider: Send + Sync + 'static + Sealed {
    /// 获取或创建单调递增计数器。
    fn counter(&self, descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Counter>;

    /// 获取或创建可上下波动的仪表。
    fn gauge(&self, descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Gauge>;

    /// 获取或创建直方图。
    fn histogram(&self, descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Histogram>;

    /// 直接记录计数器增量，避免在调用方持有 `Arc`。
    ///
    /// # 设计动机（Why）
    /// - 高频指标场景中，频繁克隆或持有 `Arc` 会增加引用计数与虚函数分发开销。
    /// - 提供一次性记录接口，使实现方能对热点路径采用批处理、线程本地缓冲或 lock-free 汇聚策略。
    /// - 在整体架构中，该方法补充了 `counter` 构造器，适用于“即用即抛”场景；共享调用路径与后端聚合层。
    ///
    /// # 契约说明（What）
    /// - **输入参数**：`descriptor` 必须指向一个已注册或可惰性创建的计数器元数据；`value` 为非负增量；`attributes` 描述维度标签。
    /// - **前置条件**：调用方应确保指标名称符合 `namespace.metric` 规范，且在调用时后端已完成必要初始化。
    /// - **后置条件**：成功执行后，该增量会被提交至底层计数器；如实现选择缓冲策略，应保证最终至少一次送达。
    ///
    /// # 执行逻辑（How）
    /// - 默认实现调用 `counter().add`，以保持旧调用路径的行为一致。
    /// - 自定义实现可绕过 `Arc` 构造，直接定位后端缓冲区，从而减少一次虚函数派发。
    ///
    /// # 风险与权衡（Trade-offs）
    /// - 若实现绕过 `counter` 缓存，需自行处理并发同步；否则可能破坏计数器的单调性。
    /// - 对降级为空操作的实现，应同样覆盖该方法以保持性能一致。
    fn record_counter_add(
        &self,
        descriptor: &InstrumentDescriptor<'_>,
        value: u64,
        attributes: AttributeSet<'_>,
    ) {
        self.counter(descriptor).add(value, attributes);
    }

    /// 直接设置或调整 Gauge 数值。
    ///
    /// # 设计动机（Why）
    /// - Gauge 常用于实时快照，调用端往往只需临时写入一次，不值得保留 `Arc<dyn Gauge>`。
    /// - 提供直接写入接口，便于实现者用更轻量的数据结构（如原子变量或线程本地缓冲）聚合瞬时值。
    ///
    /// # 契约说明（What）
    /// - **输入参数**：`value` 为目标浮点值；`attributes` 描述附加标签。
    /// - **前置条件**：调用前需保证 `descriptor` 对应的 Gauge 在语义上允许被覆盖；必要时需确保数值仍在业务允许范围。
    /// - **后置条件**：默认实现会调用 `set` 并立即提交；自定义实现可缓存待刷新状态，但需保证最终一致性。
    ///
    /// # 执行逻辑（How）
    /// - 默认实现委托给 `gauge().set`，沿用既有缓存策略。
    /// - 实现者可通过复写，直接定位 Gauge 实例或采用无锁写入方案。
    ///
    /// # 风险与权衡（Trade-offs）
    /// - 若使用延迟刷新的策略，需考虑并发读写对快照准确性的影响。
    /// - 大量不同标签组合下直接调用仍可能触发内部缓存扩容，建议结合标签池复用。
    fn record_gauge_set(
        &self,
        descriptor: &InstrumentDescriptor<'_>,
        value: f64,
        attributes: AttributeSet<'_>,
    ) {
        self.gauge(descriptor).set(value, attributes);
    }

    /// 直接记录直方图样本。
    ///
    /// # 设计动机（Why）
    /// - 直方图通常用于延迟、大小的热点度量；在追求极致延迟时，希望避免为每次记录获取 `Arc`。
    /// - 允许实现者直接写入采样缓存或指数桶聚合器，从而减少锁竞争。
    ///
    /// # 契约说明（What）
    /// - **输入参数**：`value` 为待记录样本，必须符合业务约束（如延迟必须非负）。
    /// - **前置条件**：调用方需确保指标描述符与直方图定义匹配（桶配置、单位等）。
    /// - **后置条件**：默认实现立即记录样本；若实现选择延迟聚合，应保证刷写时保持顺序一致性。
    ///
    /// # 执行逻辑（How）
    /// - 默认实现直接复用 `histogram().record`，无需额外适配。
    /// - 可以通过重写实现自定义缓冲，例如 per-thread reservoir 或 HdrHistogram。
    ///
    /// # 风险与权衡（Trade-offs）
    /// - 自定义实现必须考虑高吞吐下的桶溢出与内存使用。
    /// - 如采用抽样策略，应在文档中注明可能的精度损失。
    fn record_histogram(
        &self,
        descriptor: &InstrumentDescriptor<'_>,
        value: f64,
        attributes: AttributeSet<'_>,
    ) {
        self.histogram(descriptor).record(value, attributes);
    }
}

/// 指标契约命名空间。
///
/// # 设计动机（Why）
/// - 将 Service/Codec/Transport/Limits 等核心域的指标名称、单位与稳定标签集中声明，避免散落各处导致命名漂移；
/// - 便于后续在编译期做静态审计或生成文档（通过引用这些常量即可构建表格与说明）。
///
/// # 使用方式（How）
/// - 业务代码或框架内部若需创建仪表，只需引用相应的常量，例如 `contract::service::REQUEST_DURATION`；
/// - 标签键同样在此集中定义，可结合 [`crate::observability::attributes::KeyValue`] 构建稳定的属性集合；
/// - 与本文档配套的 `docs/observability/metrics.md` 通过引用这些常量保持“代码即契约”。
pub mod contract {
    use super::InstrumentDescriptor;

    /// Service 域指标契约定义。
    ///
    /// # 内容说明（What）
    /// - 包含请求生命周期相关的计数器、直方图与瞬时 Gauge；
    /// - 列举所有允许的标签键与部分标签枚举值，帮助调用方控制基数。
    pub mod service {
        use super::InstrumentDescriptor;

        /// 请求总次数（成功 + 失败）。
        pub const REQUEST_TOTAL: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.request.total")
                .with_description("单个 Service 的累计请求次数，包含成功与失败")
                .with_unit("requests");

        /// 请求延迟分布。
        pub const REQUEST_DURATION: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.request.duration")
                .with_description("Service 端到端处理时长，单位毫秒")
                .with_unit("ms");

        /// `poll_ready` 结果统计，涵盖 Ready/Busy/BudgetExhausted/RetryAfter 四态。
        pub const REQUEST_READY_STATE: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.request.ready_state")
                .with_description(
                    "Service::poll_ready 的返回分布，用于观测繁忙、预算耗尽与 RetryAfter 节律",
                )
                .with_unit("checks");

        /// RetryAfter 事件计数器，帮助分析退避节律。
        pub const RETRY_AFTER_TOTAL: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.request.retry_after_total")
                .with_description("在 poll_ready 中收到 RetryAfter 信号的次数")
                .with_unit("events");

        /// RetryAfter 推荐延迟的直方图，单位毫秒。
        pub const RETRY_AFTER_DELAY_MS: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.request.retry_after_delay_ms")
                .with_description(
                    "RetryAfter 建议等待时长的分布，结合 histogram_quantile 评估退避策略",
                )
                .with_unit("ms");

        /// 并发中的请求数量。
        pub const REQUEST_INFLIGHT: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.request.inflight")
                .with_description("当前尚未完成的请求数，用于衡量排队/并发压力")
                .with_unit("requests");

        /// 失败请求次数。
        pub const REQUEST_ERRORS: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.request.errors")
                .with_description("根据稳定错误分类统计的失败调用次数")
                .with_unit("requests");

        /// 入站有效载荷体积。
        pub const BYTES_INBOUND: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.bytes.inbound")
                .with_description("自客户端接收到的业务载荷大小")
                .with_unit("bytes");

        /// 出站有效载荷体积。
        pub const BYTES_OUTBOUND: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.bytes.outbound")
                .with_description("向客户端发送的业务载荷大小")
                .with_unit("bytes");

        /// 关键标签常量由 `observability::keys::metrics::service` 自动生成，确保指标/日志与契约同步。
        pub use crate::observability::keys::metrics::service::{
            ATTR_ERROR_KIND, ATTR_OPERATION, ATTR_OUTCOME, ATTR_PEER_IDENTITY, ATTR_PROTOCOL,
            ATTR_READY_DETAIL, ATTR_READY_STATE, ATTR_ROUTE_ID, ATTR_SERVICE_NAME,
            ATTR_STATUS_CODE,
        };
        /// ReadyState 与 Outcome 的标签值同样来自 SOT，避免与仪表盘枚举脱节。
        pub use crate::observability::keys::metrics::service::{
            OUTCOME_ERROR, OUTCOME_SUCCESS, READY_DETAIL_CUSTOM, READY_DETAIL_DOWNSTREAM,
            READY_DETAIL_PLACEHOLDER, READY_DETAIL_QUEUE_FULL, READY_DETAIL_RETRY_AFTER,
            READY_DETAIL_UPSTREAM, READY_STATE_BUDGET_EXHAUSTED, READY_STATE_BUSY,
            READY_STATE_READY, READY_STATE_RETRY_AFTER,
        };
    }

    /// Codec 域指标契约定义。
    ///
    /// # 内容说明
    /// - 关注编解码耗时、字节规模与错误分类；
    /// - `codec.mode` 标签区分 encode / decode，便于统一写入。
    pub mod codec {
        use super::InstrumentDescriptor;

        /// 编码耗时。
        pub const ENCODE_DURATION: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.codec.encode.duration")
                .with_description("单次编码操作的耗时，通常在 Handler 写回响应时记录")
                .with_unit("ms");

        /// 解码耗时。
        pub const DECODE_DURATION: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.codec.decode.duration")
                .with_description("单次解码操作的耗时，用于监控协议解析复杂度")
                .with_unit("ms");

        /// 编码输出字节数。
        pub const ENCODE_BYTES: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.codec.encode.bytes")
                .with_description("编码后产生的字节数量")
                .with_unit("bytes");

        /// 解码输入字节数。
        pub const DECODE_BYTES: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.codec.decode.bytes")
                .with_description("解码器消费的字节数量")
                .with_unit("bytes");

        /// 编码错误次数。
        pub const ENCODE_ERRORS: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.codec.encode.errors")
                .with_description("编码阶段出现的错误计数")
                .with_unit("errors");

        /// 解码错误次数。
        pub const DECODE_ERRORS: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.codec.decode.errors")
                .with_description("解码阶段出现的错误计数")
                .with_unit("errors");

        /// 编解码标签常量统一来源于 `observability::keys::metrics::codec`，保持编码/解码枚举一致。
        pub use crate::observability::keys::metrics::codec::{
            ATTR_CODEC_NAME, ATTR_CONTENT_TYPE, ATTR_ERROR_KIND, ATTR_MODE, MODE_DECODE,
            MODE_ENCODE,
        };
    }

    /// Transport 域指标契约定义。
    ///
    /// # 内容说明
    /// - 对齐连接生命周期与链路吞吐；
    /// - 结合 `listener.id` 与 `peer.role` 控制基数，便于单实例聚合。
    pub mod transport {
        use super::InstrumentDescriptor;

        /// 当前活跃连接数。
        pub const CONNECTIONS_ACTIVE: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.transport.connections")
                .with_description("单实例当前维护的活跃连接数量")
                .with_unit("connections");

        /// 建连尝试次数。
        pub const CONNECTION_ATTEMPTS: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.transport.connection.attempts")
                .with_description("监听或拨号产生的连接尝试计数，包括成功和失败")
                .with_unit("connections");

        /// 连接失败次数。
        pub const CONNECTION_FAILURES: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.transport.connection.failures")
                .with_description("建连阶段的失败次数，便于快速捕获证书或网络问题")
                .with_unit("connections");

        /// 握手耗时。
        pub const HANDSHAKE_DURATION: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.transport.handshake.duration")
                .with_description("连接自发起到握手完成的耗时")
                .with_unit("ms");

        /// 入站链路字节量。
        pub const BYTES_INBOUND: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.transport.bytes.inbound")
                .with_description("底层传输层接收的字节总量")
                .with_unit("bytes");

        /// 出站链路字节量。
        pub const BYTES_OUTBOUND: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.transport.bytes.outbound")
                .with_description("底层传输层发送的字节总量")
                .with_unit("bytes");

        /// 传输层标签与枚举值复用合约生成的常量，避免 PromQL/日志字段出现漂移。
        pub use crate::observability::keys::metrics::transport::{
            ATTR_ERROR_KIND, ATTR_LISTENER_ID, ATTR_PEER_ROLE, ATTR_PROTOCOL, ATTR_RESULT,
            ATTR_SOCKET_FAMILY, RESULT_FAILURE, RESULT_SUCCESS, ROLE_CLIENT, ROLE_SERVER,
        };
    }

    /// Pipeline 域指标契约定义。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：为运行时热插拔与配置热更新提供统一的纪元与变更事件基线，
    ///   方便 SRE 将数据面观测与控制面发布流程对齐。
    /// - **逻辑（How）**：通过 `spark.pipeline.epoch` Gauge 暴露当前逻辑纪元，
    ///   并以 `spark.pipeline.mutation.total` Counter 按操作类型记录 add/remove/replace 事件，
    ///   同时定义稳定标签（控制器、Pipeline 标识、操作类型、纪元数值）以支撑指标与日志的联动查询。
    /// - **契约（What）**：
    ///   - 纪元 Gauge 必须在变更提交后写入，以保证“观察到跳变即可判断新拓扑可见”；
    ///   - `pipeline.mutation.op` 仅允许使用下方定义的常量，便于 PromQL 模板（如 `sum by (op)`）复用；
    ///   - `pipeline.id` 需保持低基数，通常对应 Channel ID 或逻辑管线标识。
    /// - **风险提示（Trade-offs）**：若未来引入多种控制器实现，务必在变更评审中同步更新控制器标签取值，
    ///   否则 Grafana/告警规则会因枚举漂移产生盲点；当 Pipeline 数量巨大时，可考虑引入按租户分片的聚合策略。
    pub mod pipeline {
        use super::InstrumentDescriptor;

        /// Pipeline 最新运行纪元。
        pub const EPOCH: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.pipeline.epoch")
                .with_description("Pipeline 运行时逻辑纪元，用于验证热插拔后的拓扑是否已经对等生效")
                .with_unit("epoch");

        /// Pipeline 变更事件计数。
        pub const MUTATION_TOTAL: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.pipeline.mutation.total")
                .with_description("Pipeline Handler 变更事件（add/remove/replace）的累计次数")
                .with_unit("events");

        /// Pipeline 标签与操作枚举由合约生成，保证控制面与数据面的一致性。
        pub use crate::observability::keys::metrics::pipeline::{
            ATTR_CONTROLLER, ATTR_EPOCH, ATTR_MUTATION_OP, ATTR_PIPELINE_ID, CONTROLLER_HOT_SWAP,
            OP_ADD, OP_REMOVE, OP_REPLACE,
        };
    }

    /// Limits 域指标契约定义。
    ///
    /// # 内容说明
    /// - 反映资源限额的使用情况、触发频率与策略结果，支撑丢弃/降级比的观测。
    pub mod limits {
        use super::InstrumentDescriptor;

        /// 资源当前使用量。
        pub const RESOURCE_USAGE: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.limits.usage")
                .with_description("目标资源的即时占用量")
                .with_unit("units");

        /// 资源限额配置。
        pub const RESOURCE_LIMIT: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.limits.limit")
                .with_description("目标资源的配置上限")
                .with_unit("units");

        /// 超限触发次数。
        pub const HIT_TOTAL: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.limits.hit")
                .with_description("资源达到限额时的触发次数")
                .with_unit("events");

        /// 超限导致的直接丢弃次数。
        pub const DROP_TOTAL: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.limits.drop")
                .with_description("因限额触发而拒绝请求的次数")
                .with_unit("events");

        /// 超限导致的降级次数。
        pub const DEGRADE_TOTAL: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.limits.degrade")
                .with_description("因限额触发而执行降级策略的次数")
                .with_unit("events");

        /// 排队策略下的队列深度。
        pub const QUEUE_DEPTH: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.limits.queue.depth")
                .with_description("排队策略执行时的实时队列深度")
                .with_unit("entries");

        /// 限额标签常量来自合约，保证治理策略与指标维度同步。
        pub use crate::observability::keys::metrics::limits::{ATTR_ACTION, ATTR_RESOURCE};
    }

    /// 热更新域指标契约定义。
    ///
    /// # 设计意图（Why）
    /// - 统一暴露运行时热更新过程中的纪元与延迟，便于 SRE 在 Grafana/Prometheus 中排查配置是否落地；
    /// - 与运行时文档 `docs/runbook/pipeline-epoch.md` 保持一致的命名规范，方便联动分析。
    pub mod hot_reload {
        use super::InstrumentDescriptor;

        /// 运行时配置纪元，单调递增。
        pub const CONFIG_EPOCH: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.hot_reload.config_epoch")
                .with_description("运行时已应用配置的纪元号，单调递增")
                .with_unit("epoch");

        /// 热更新应用耗时直方图，单位毫秒。
        pub const APPLY_LATENCY_MS: InstrumentDescriptor<'static> =
            InstrumentDescriptor::new("spark.hot_reload.apply_latency_ms")
                .with_description("从收到配置更新到完成应用的耗时分布，单位毫秒")
                .with_unit("ms");

        /// 热更新组件标签复用 SOT 常量，确保运行时与文档描述一致。
        pub use crate::observability::keys::metrics::hot_reload::ATTR_COMPONENT;
    }
}
