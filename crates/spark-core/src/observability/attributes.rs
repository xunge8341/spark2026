use alloc::{borrow::Cow, string::String, vec::Vec};

/// 观测性属性键的通用别名。
///
/// # 设计背景（Why）
/// - 参考 OpenTelemetry `Key` 的抽象，将键限定为 UTF-8 字符串，确保跨语言与跨协议的一致性。
/// - 采用 `Cow<'a, str>` 以兼顾静态常量与运行时动态生成的键名，避免频繁分配。
///
/// # 契约说明（What）
/// - **允许值**：`Cow::Borrowed` 与 `Cow::Owned` 均可；调用方需保证键名遵循低基数、蛇形命名约定。
/// - **前置条件**：键名必须是 ASCII 可打印字符组合，便于序列化输出；违反约束时实现需记录警告或丢弃。
/// - **后置条件**：键名会在指标与日志的导出链路中按原样传递。
///
/// # 风险提示（Trade-offs）
/// - 未对键长做硬性限制，建议遵循业界实践控制在 128 字符以内，避免下游系统截断。
pub type AttributeKey<'a> = Cow<'a, str>;

/// 描述单个属性键值对的结构化条目。
///
/// # 设计背景（Why）
/// - 借鉴 Datadog、Honeycomb 在结构化日志中的“Key-Value Field”设计，确保指标、日志共享同一建模方式。
/// - 通过 `MetricAttributeValue` 支持多种标量类型，避免将数值强制转换为字符串导致的信息损失。
///
/// # 逻辑解析（How）
/// - `key` 使用 [`AttributeKey`]，允许静态或动态字符串。
/// - `value` 使用 [`MetricAttributeValue`]，封装对布尔、整数、浮点与文本的支持。
/// - `KeyValue::new` 在内部执行最小化拷贝，尽可能复用借用数据。
///
/// # 契约说明（What）
/// - **前置条件**：调用方需保证 `key` 低基数，且不会与框架保留键（如 `service.name`）冲突。
/// - **后置条件**：`KeyValue` 可安全在多线程间克隆（`Clone`），但本身不提供同步原语。
///
/// # 风险提示（Trade-offs）
/// - 未对 `value` 进行去重，使用者需在高频路径上自行管理缓冲复用以减轻 GC/Allocator 压力。
#[derive(Clone, Debug, PartialEq)]
pub struct KeyValue<'a> {
    pub key: AttributeKey<'a>,
    pub value: MetricAttributeValue<'a>,
}

impl<'a> KeyValue<'a> {
    /// 构建新的属性键值对。
    ///
    /// # 契约说明
    /// - **输入参数**：
    ///   - `key`：属性键名，支持静态字符串或运行时分配的 `String`。
    ///   - `value`：属性值，将通过 [`MetricAttributeValue::from`] 自动适配常用原始类型。
    /// - **前置条件**：调用方需确保键值组合不会导致高基数问题；必要时可在外层做采样或聚合。
    /// - **后置条件**：返回的 [`KeyValue`] 拥有 `value` 数据（若为 `Owned`），适合在异步任务间传递。
    ///
    /// # 风险提示
    /// - 频繁调用可能带来堆分配成本；建议在热路径重用 `Vec<KeyValue>` 缓冲。
    pub fn new(
        key: impl Into<AttributeKey<'a>>,
        value: impl Into<MetricAttributeValue<'a>>,
    ) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// 属性集合的借用视图。
///
/// # 设计背景（Why）
/// - 采用切片引用的形式，避免 trait 对象方法签名中的生命周期复杂度，同时鼓励调用方在外部复用缓冲。
///
/// # 契约说明（What）
/// - **语义**：该别名仅提供只读视图，不承担所有权；生命周期由调用方管理。
/// - **前置条件**：切片中的 [`KeyValue`] 条目必须满足键值契约，不可包含重复键。
/// - **后置条件**：实现方不得缓存该引用超出调用栈范围，以防悬垂指针。
///
/// # 风险提示（Trade-offs）
/// - 若需要所有权转移，请在调用前通过 `Vec::into_boxed_slice` 等方式克隆数据。
pub type AttributeSet<'a> = &'a [KeyValue<'a>];

/// 指标与日志属性值的统一枚举。
///
/// # 设计背景（Why）
/// - 参考 OpenTelemetry 与 Prometheus 的数据模型，提供布尔、整数、浮点、文本四种最常用的标量类型。
/// - 允许文本类型在借用与拥有之间切换，以平衡性能与灵活度。
///
/// # 逻辑解析（How）
/// - `Text` 变体使用 `Cow<'a, str>`，减少多余复制。
/// - 数值类型通过 `From` 实现，与原始类型的转换为零成本或最小成本。
///
/// # 契约说明（What）
/// - **前置条件**：数值应满足指标语义（例如延迟不可为负），该约束需在上层业务中保证。
/// - **后置条件**：枚举实现 `Clone`，可安全在多线程中复制；但不保证线程安全访问。
///
/// # 风险提示（Trade-offs）
/// - 未区分有符号与无符号整型，统一折叠为 `i64`；当上游传入超出范围的 `u64` 时将执行饱和转换，可能损失信息。
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum MetricAttributeValue<'a> {
    Text(Cow<'a, str>),
    Bool(bool),
    F64(f64),
    I64(i64),
}

impl<'a> From<&'a str> for MetricAttributeValue<'a> {
    fn from(value: &'a str) -> Self {
        Self::Text(Cow::Borrowed(value))
    }
}

impl From<String> for MetricAttributeValue<'_> {
    fn from(value: String) -> Self {
        Self::Text(Cow::Owned(value))
    }
}

impl<'a> From<Cow<'a, str>> for MetricAttributeValue<'a> {
    fn from(value: Cow<'a, str>) -> Self {
        Self::Text(value)
    }
}

impl From<bool> for MetricAttributeValue<'_> {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<f64> for MetricAttributeValue<'_> {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<f32> for MetricAttributeValue<'_> {
    fn from(value: f32) -> Self {
        Self::F64(value.into())
    }
}

impl From<i64> for MetricAttributeValue<'_> {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<i32> for MetricAttributeValue<'_> {
    fn from(value: i32) -> Self {
        Self::I64(value.into())
    }
}

impl From<u64> for MetricAttributeValue<'_> {
    fn from(value: u64) -> Self {
        if value > i64::MAX as u64 {
            MetricAttributeValue::I64(i64::MAX)
        } else {
            MetricAttributeValue::I64(value as i64)
        }
    }
}

impl From<u32> for MetricAttributeValue<'_> {
    fn from(value: u32) -> Self {
        MetricAttributeValue::I64(value as i64)
    }
}

impl<'a> MetricAttributeValue<'a> {
    /// 将属性值转化为拥有所有权的形式，适合长期缓存或跨线程传递。
    ///
    /// # 契约说明
    /// - **前置条件**：调用方确认需要所有权（例如缓存至 `Arc`）。
    /// - **后置条件**：返回的新枚举生命周期提升为 `'static`，从而在异步任务间安全共享。
    ///
    /// # 风险提示
    /// - 文本值会触发堆分配；在热路径调用时需谨慎，必要时请复用字符串池。
    pub fn into_owned(self) -> MetricAttributeValue<'static> {
        match self {
            MetricAttributeValue::Text(text) => {
                MetricAttributeValue::Text(Cow::Owned(text.into_owned()))
            }
            MetricAttributeValue::Bool(value) => MetricAttributeValue::Bool(value),
            MetricAttributeValue::F64(value) => MetricAttributeValue::F64(value),
            MetricAttributeValue::I64(value) => MetricAttributeValue::I64(value),
        }
    }
}

/// 辅助类型：用于构造拥有所有权的属性集合。
///
/// # 设计背景（Why）
/// - 在部分场景（例如延迟批量上报指标）中，需要暂存属性集合；提供该结构以减少重复实现。
///
/// # 逻辑解析（How）
/// - 内部维护 `Vec<KeyValue<'static>>`，通过 [`Self::push_owned`] 接受可转换为 `'static` 的键值。
/// - 完成构建后，可通过 [`Self::as_slice`] 暴露为 [`AttributeSet`]，以符合各指标、日志接口的输入要求。
///
/// # 契约说明（What）
/// - **前置条件**：[`Self::push_owned`] 会将键和值全部转换为拥有所有权的形式，可能触发分配。
/// - **后置条件**：`OwnedAttributeSet` 可重复用于多次观测上报，避免重新分配。
///
/// # 风险提示（Trade-offs）
/// - 当前未实现容量收缩；在大量临时属性场景下需显式 `clear` 以避免容量膨胀。
#[derive(Default, Clone, Debug)]
pub struct OwnedAttributeSet {
    entries: Vec<KeyValue<'static>>,
}

impl OwnedAttributeSet {
    /// 创建空的属性集合。
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// 将借用视图中的键值对扩展为拥有所有权的集合。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：`ServiceMetricsHook` 等组件需要在保留原有标签的同时追加新的观测标签，
    ///   若每次手动克隆 `Vec<KeyValue>` 容易遗漏字段。此方法提供集中化的扩展入口，
    ///   确保标签复制逻辑一致且易于审计。
    /// - **契约 (What)**：
    ///   - **输入**：`borrowed` 为只读的属性切片，生命周期可短于 `OwnedAttributeSet`；
    ///   - **输出**：当前集合会追加对应键值的拥有所有权副本；已有条目保持不变；
    ///   - **前置条件**：调用方需保证切片中不存在重复键或超出基数约束的值；
    ///   - **后置条件**：每个键值均转化为 `'static` 生命周期的 [`KeyValue`]，适合跨异步任务传递。
    /// - **实现 (How)**：逐条克隆键（通过 [`Cow::into_owned`]）和值（通过 [`MetricAttributeValue::into_owned`]），
    ///   并推入内部 `Vec`；使用 `reserve` 优化批量复制时的分配次数。
    /// - **风险与权衡 (Trade-offs)**：复制操作会导致额外的分配成本；若在高频路径调用，建议复用
    ///   `OwnedAttributeSet` 并配合 [`OwnedAttributeSet::clear`] 降低分配次数。
    pub fn extend_from(&mut self, borrowed: AttributeSet<'_>) {
        self.entries.reserve(borrowed.len());
        for kv in borrowed {
            self.entries.push(KeyValue {
                key: Cow::Owned(kv.key.clone().into_owned()),
                value: kv.value.clone().into_owned(),
            });
        }
    }

    /// 将键值对以拥有所有权的方式追加到集合中。
    ///
    /// # 契约说明
    /// - **输入参数**：
    ///   - `key`: 可转换为 [`AttributeKey<'static>`] 的键名。
    ///   - `value`: 可转换为 [`MetricAttributeValue<'static>`] 的值。
    /// - **前置条件**：调用者需确保键值不会导致高基数；`value` 的 `into_owned` 可能产生分配。
    /// - **后置条件**：集合长度增加一条记录，可立即通过 [`Self::as_slice`] 读取。
    ///
    /// # 风险提示
    /// - 过度增长会导致堆内存占用增加；可结合 `clear` 回收逻辑。
    pub fn push_owned(
        &mut self,
        key: impl Into<AttributeKey<'static>>,
        value: impl Into<MetricAttributeValue<'static>>,
    ) {
        self.entries.push(KeyValue {
            key: key.into(),
            value: value.into(),
        });
    }

    /// 以切片形式访问属性集合，供指标与日志接口消费。
    pub fn as_slice(&self) -> AttributeSet<'_> {
        self.entries.as_slice()
    }

    /// 清空集合但保留容量，用于循环复用。
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}
