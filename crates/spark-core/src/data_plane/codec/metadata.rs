use alloc::{borrow::Cow, vec::Vec};

/// `ContentType` 以 IANA `media-type` 约定描述消息的主语义。
///
/// # 设计背景（Why）
/// - 行业头部框架（gRPC、HTTP/2、Kafka 协议头）均以标准化 MIME 类型标识负载语义，可与各语言生态兼容。
/// - 通过 `Cow<'static, str>` 兼容静态常量与运行时协商出的扩展类型，避免过度复制。
///
/// # 逻辑解析（How）
/// - `new` 接收任何可转为 `Cow<'static, str>` 的类型，允许静态字面量与动态字符串共存。
/// - `as_str` 暴露底层切片，便于注册中心或日志系统直接使用。
///
/// # 契约说明（What）
/// - **前置条件**：传入的媒体类型必须满足 IANA `type/subtype` 格式，推荐全小写。
/// - **后置条件**：实例保证内部存储 `'static` 生命周期，方便在注册表中长期缓存。
///
/// # 风险提示（Trade-offs）
/// - 为保持灵活性并未验证合法性；在边界系统（如边缘节点）调用者需额外校验避免注入攻击。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContentType(Cow<'static, str>);

impl ContentType {
    /// 创建新的内容类型。
    pub fn new(value: impl Into<Cow<'static, str>>) -> Self {
        Self(value.into())
    }

    /// 返回底层字符串表示。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// `ContentEncoding` 表达压缩、加密等内容层附加算法。
///
/// # 设计背景（Why）
/// - 参考 HTTP `Content-Encoding` 与 gRPC `grpc-encoding` 约定，统一标识压缩算法，便于多语言互通。
/// - 预留 `identity` 常量表达“无变换”，满足最小实现。
///
/// # 逻辑解析（How）
/// - `ContentEncoding::identity()` 返回共享常量，避免重复分配。
/// - `is_identity` 帮助调用方快速分支以跳过不必要的解压流程。
///
/// # 契约说明（What）
/// - **前置条件**：值应遵循小写连字符风格（如 `gzip`、`zstd`、`aes-256-gcm`）。
/// - **后置条件**：结构体始终可安全拷贝，适合放入 `HashMap` 或广播到监控系统。
///
/// # 风险提示（Trade-offs）
/// - 该结构并未暗含安全语义；若算法涉及密钥，需由上层协商或密钥管理系统保证。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContentEncoding(Cow<'static, str>);

impl ContentEncoding {
    /// 语义上等价于 HTTP `identity`，表示未进行额外处理。
    pub fn identity() -> Self {
        Self(Cow::Borrowed("identity"))
    }

    /// 创建新的编码标识。
    pub fn new(value: impl Into<Cow<'static, str>>) -> Self {
        Self(value.into())
    }

    /// 返回底层字符串。
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 是否与 `identity` 等价。
    pub fn is_identity(&self) -> bool {
        self.0 == "identity"
    }
}

/// `SchemaDescriptor` 记录消息 schema 的名称、版本与可选指纹。
///
/// # 设计背景（Why）
/// - 参考 Avro Schema Registry、Protobuf `FileDescriptor` 与 Apache Arrow `Schema`，在跨语言序列化中携带可演化的 schema 元信息。
/// - 指纹字段允许集成 Confluent Schema Registry 这类业界事实标准。
///
/// # 逻辑解析（How）
/// - `name` 一般对应 schema 集合或 Protobuf 包名。
/// - `version` 采用可选字符串，支持 `major.minor.patch` 或语义化标签。
/// - `fingerprint` 允许实现者存放哈希（如 SHA-256 前 32 位），用于快速校验兼容性。
///
/// # 契约说明（What）
/// - **前置条件**：若提供 `fingerprint`，应保证哈希算法与长度在协商阶段一致。
/// - **后置条件**：结构体不绑定具体注册中心，实现者可在握手时自定义扩展字段。
///
/// # 风险提示（Trade-offs）
/// - 长度可变的 `fingerprint` 以 `Vec<u8>` 存储，可能触发堆分配；若对性能敏感，可在自定义实现中使用固定长度数组。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaDescriptor {
    name: Cow<'static, str>,
    version: Option<Cow<'static, str>>,
    fingerprint: Option<Vec<u8>>,
}

impl SchemaDescriptor {
    /// 构建仅包含名称的 schema 描述。
    pub fn with_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            version: None,
            fingerprint: None,
        }
    }

    /// 为 schema 设置语义版本。
    pub fn with_version(mut self, version: impl Into<Cow<'static, str>>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// 为 schema 附加指纹。
    pub fn with_fingerprint(mut self, fingerprint: impl Into<Vec<u8>>) -> Self {
        self.fingerprint = Some(fingerprint.into());
        self
    }

    /// 获取 schema 名称。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 获取可选版本。
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// 获取可选指纹。
    pub fn fingerprint(&self) -> Option<&[u8]> {
        self.fingerprint.as_deref()
    }
}

/// `CodecDescriptor` 聚合内容类型、内容编码与可选 schema 信息，作为编解码契约的最小识别单元。
///
/// # 设计背景（Why）
/// - 借鉴 Netty `Codec`、gRPC `MethodDescriptor` 与 Kafka `RecordBatch` 元信息结构，将协商所需信息集中在一个不可变描述中。
/// - 描述符可用于注册中心索引、握手阶段比对，或写入遥测数据，实现跨平台一致识别。
///
/// # 逻辑解析（How）
/// - `new` 至少需要内容类型与内容编码；schema 可选。
/// - `with_schema` 允许链式构建，契合同步传递。
/// - 只读访问器用于运行时透传到日志、指标或握手响应。
///
/// # 契约说明（What）
/// - **前置条件**：内容类型与编码必须与实际负载匹配，否则会导致消费者解码失败。
/// - **后置条件**：实例可以安全地在多线程间共享（`Clone + Send + Sync` 派生由外部确保）。
///
/// # 风险提示（Trade-offs）
/// - 本结构未提供变更通知机制；若在运行时修改需重新广播给所有消费者，以免产生数据倾斜或解码错误。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecDescriptor {
    content_type: ContentType,
    content_encoding: ContentEncoding,
    schema: Option<SchemaDescriptor>,
}

impl CodecDescriptor {
    /// 构建新的描述符。
    pub fn new(content_type: ContentType, content_encoding: ContentEncoding) -> Self {
        Self {
            content_type,
            content_encoding,
            schema: None,
        }
    }

    /// 附加 schema 元信息。
    pub fn with_schema(mut self, schema: SchemaDescriptor) -> Self {
        self.schema = Some(schema);
        self
    }

    /// 获取内容类型。
    pub fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    /// 获取内容编码。
    pub fn content_encoding(&self) -> &ContentEncoding {
        &self.content_encoding
    }

    /// 获取可选 schema。
    pub fn schema(&self) -> Option<&SchemaDescriptor> {
        self.schema.as_ref()
    }
}
