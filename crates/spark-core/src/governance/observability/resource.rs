use alloc::{borrow::Cow, vec::Vec};

/// 资源属性键值的最小抽象，覆盖 `service.name`、`service.version` 等稳定标签。
///
/// # 教案式说明
/// - **意图（Why）**：在 `spark-core` 暴露一份只读、无运行时依赖的资源属性模型，让 `spark-otel` 等观测实现能够共享统一语义，避免
///   各自定义字符串常量造成分歧。
/// - **逻辑（How）**：以 [`Cow<'a, str>`] 持有键和值，兼顾常量与运行时拼接场景；类型为不可变结构体，调用方只能通过构造函数与访
///   问器读取内部数据。
/// - **契约（What）**：键名建议遵循 OpenTelemetry 规范的点分命名（如 `service.instance.id`）；值遵循 UTF-8，禁止包含控制字符。
/// - **风险提示（Trade-offs）**：类型本身不做合法性校验，以免重复逻辑；若业务需要严格校验，应在构造前由上层完成过滤。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResourceAttr<'a> {
    key: Cow<'a, str>,
    value: Cow<'a, str>,
}

impl<'a> ResourceAttr<'a> {
    /// 构造新的资源属性。
    ///
    /// # 契约说明
    /// - **输入参数**：
    ///   - `key`：资源属性键，支持静态字符串或运行时 `String`。
    ///   - `value`：属性值，通常来源于配置或主机环境变量。
    /// - **前置条件**：调用方需保证键名低基数且不与框架保留键冲突；值需要满足 UTF-8。
    /// - **后置条件**：实例拥有键值所有权或借用引用，不会主动复制数据。
    pub fn new(key: impl Into<Cow<'a, str>>, value: impl Into<Cow<'a, str>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// 返回属性键的只读视图。
    ///
    /// # 契约说明
    /// - **返回值**：`&str` 切片，生命周期与当前实例一致。
    /// - **注意事项**：调用方不可在返回值上执行变更操作；如需修改请重新构造属性。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 返回属性值的只读视图。
    pub fn value(&self) -> &str {
        &self.value
    }

    /// 将当前属性转换为 `'static` 生命周期，便于长期缓存。
    ///
    /// # 契约说明
    /// - **前置条件**：调用方确认需要拥有所有权（如写入 `Arc` 或全局静态）。
    /// - **后置条件**：返回的实例生命周期提升为 `'static`，内部可能触发一次堆分配。
    pub fn into_owned(self) -> ResourceAttr<'static> {
        ResourceAttr {
            key: Cow::Owned(self.key.into_owned()),
            value: Cow::Owned(self.value.into_owned()),
        }
    }
}

/// 资源属性集合的借用视图，供宿主运行时以零拷贝方式传递给导出器。
pub type ResourceAttrSet<'a> = &'a [ResourceAttr<'a>];

/// 拥有所有权的资源属性集合构造器。
///
/// # 教案式说明
/// - **意图（Why）**：在收集期望导出的全部属性时，需要一个可变缓冲来累积键值对；该结构封装 `Vec` 并保持 API 简洁。
/// - **逻辑（How）**：内部维护 `Vec<ResourceAttr<'static>>`，通过 `push_owned` 将任意生命周期的输入提升为 `'static`。
/// - **契约（What）**：`as_slice` 提供只读视图，满足 `ResourceAttrSet` 契约；`clear` 允许重用缓冲。
/// - **风险提示（Trade-offs）**：向量增长时会分配内存，建议在宿主启动阶段预估容量并调用 [`Self::with_capacity`]。
#[derive(Clone, Debug, Default)]
pub struct OwnedResourceAttrs {
    entries: Vec<ResourceAttr<'static>>,
}

impl OwnedResourceAttrs {
    /// 创建空的属性集合。
    pub fn new() -> Self {
        Self::default()
    }

    /// 按预估容量创建属性集合，减少重分配。
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// 追加一个拥有所有权的资源属性。
    ///
    /// # 契约说明
    /// - **输入参数**：任意可转换为 `'static` 生命周期的键和值。
    /// - **后置条件**：内部向量长度加一，可能触发重新分配。
    pub fn push_owned(
        &mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<Cow<'static, str>>,
    ) {
        let attr = ResourceAttr {
            key: key.into(),
            value: value.into(),
        };
        self.entries.push(attr);
    }

    /// 将任意借用属性提升为拥有所有权后追加。
    pub fn push_attr(&mut self, attr: ResourceAttr<'_>) {
        self.entries.push(attr.into_owned());
    }

    /// 提供只读切片视图，满足导出接口所需的契约。
    pub fn as_slice(&self) -> ResourceAttrSet<'_> {
        &self.entries
    }

    /// 清空已有属性，但保留已分配容量以便复用。
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}
