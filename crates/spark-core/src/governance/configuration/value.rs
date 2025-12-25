use alloc::borrow::Cow;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;
use core::time::Duration;

/// 配置项附带的可选元数据。
///
/// ### 设计目标（Why）
/// - 将业界常见的配置属性（可热更新、是否加密、是否实验性）显式化，帮助上层治理策略决策。
/// - 在无 `std` 的情况下仍能表达布尔标记，便于跨平台复用。
///
/// ### 逻辑概览（How）
/// - `hot_reloadable`：是否支持运行时热更新。
/// - `encrypted`：是否需要外部密钥管理器参与解密。
/// - `experimental`：标记当前配置是否尚处稳定性观察期。
/// - `tags`：额外标签，沿用 CNCF 项目推崇的键值标签理念。
///
/// ### 契约说明（What）
/// - **前置条件**：标签键值需满足 UTF-8，可被任意序列化框架或 FFI 层安全消费。
/// - **后置条件**：实现 `Default`，便于调用方仅关注需要的字段。
///
/// ### 设计取舍（Trade-offs）
/// - 仅使用向量存储标签，避免在 `no_std` 场景强行引入 `HashMap`；上层可选择合适的查找结构。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfigMetadata {
    pub hot_reloadable: bool,
    pub encrypted: bool,
    pub experimental: bool,
    pub tags: Vec<(Cow<'static, str>, Cow<'static, str>)>,
}

/// 配置值的枚举表示。
///
/// ### 设计目标（Why）
/// - 参考 HashiCorp Consul、AWS AppConfig 与 Envoy xDS 对配置值的通用抽象，确保跨生态互操作。
/// - Rust 层使用强类型枚举，避免传统字符串配置带来的解析歧义。
///
/// ### 逻辑解析（How）
/// - 支持基础标量类型（布尔、整数、浮点、字符串、字节、时间间隔）。
/// - 通过 `List` 嵌套表示轻量数组，覆盖多 Endpoint、白名单等场景。
/// - 预留 `Dictionary` 支持键值对结构，但为了无 `std` 约束只允许嵌套 `(key, value)` 列表。
///
/// ### 契约定义（What）
/// - **前置条件**：`Dictionary` 中的键必须唯一；调用方需自行保证。
/// - **输入/输出**：枚举值可配合 [`ConfigMetadata`] 一并返回。
/// - **后置条件**：所有变体均实现 `Clone`，适用于广播、快照与缓存。
///
/// ### 设计取舍与风险（Trade-offs）
/// - 不直接实现 `serde::Serialize/Deserialize`，以免框架在公共 API 上强绑定序列化方案；若需 JSON/Protobuf，可借助下方内部表示自行转换。
/// - `List` 与 `Dictionary` 采用 `Vec` 以保持顺序性，便于与 YAML/JSON 对齐；若需高性能查询可在业务侧转换。
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ConfigValue {
    Boolean(bool, ConfigMetadata),
    Integer(i64, ConfigMetadata),
    Float(f64, ConfigMetadata),
    Text(Cow<'static, str>, ConfigMetadata),
    Binary(Cow<'static, [u8]>, ConfigMetadata),
    Duration(Duration, ConfigMetadata),
    List(Vec<ConfigValue>, ConfigMetadata),
    Dictionary(Vec<(Cow<'static, str>, ConfigValue)>, ConfigMetadata),
}

impl ConfigValue {
    /// 返回与配置值绑定的元数据。
    ///
    /// ### 契约（What）
    /// - **输入**：对配置值的不可变引用。
    /// - **输出**：元数据的不可变引用。
    ///
    /// ### 逻辑（How）
    /// - 通过匹配枚举变体直接返回引用，不发生克隆。
    #[inline]
    pub fn metadata(&self) -> &ConfigMetadata {
        match self {
            Self::Boolean(_, meta)
            | Self::Integer(_, meta)
            | Self::Float(_, meta)
            | Self::Text(_, meta)
            | Self::Binary(_, meta)
            | Self::Duration(_, meta)
            | Self::List(_, meta)
            | Self::Dictionary(_, meta) => meta,
        }
    }
}

/// 表示配置值序列化/反序列化过程中可能出现的错误。
///
/// ### 设计动机（Why）
/// - 当我们将 `ConfigValue` 映射到外部格式（例如 JSON 或审计事件载荷）时，需要统一的错误语义描述非法输入，
///   以避免上层重复维护零散的错误字符串。
///
/// ### 契约说明（What）
/// - **输入**：来自结构化载荷的中间表示。
/// - **输出**：若成功则还原为 `ConfigValue`，否则返回错误，调用方需据此决定降级策略。
///
/// ### 风险提示（Trade-offs）
/// - 当前仅对时间间隔的纳秒字段做上限校验，其它字段的语义校验交由上层完成，以保持本模块的通用性。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConfigValueReprError {
    InvalidDuration { secs: u64, nanos: u32 },
}

impl fmt::Display for ConfigValueReprError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDuration { secs, nanos } => write!(
                f,
                "invalid duration representation: secs={} nanos={} (nanos must be < 1_000_000_000)",
                secs, nanos
            ),
        }
    }
}

impl ConfigMetadata {
    /// 将元数据转换为内部可序列化表示。
    ///
    /// ### 设计要点（Why）
    /// - 避免直接暴露 `serde` 依赖，通过中间结构让调用方在需要时自行序列化。
    ///
    /// ### 前置条件（Preconditions）
    /// - 调用方必须保证 `tags` 中的键值满足 UTF-8 编码，可安全转为 `String`。
    pub(crate) fn to_repr(&self) -> ConfigMetadataRepr {
        ConfigMetadataRepr {
            hot_reloadable: self.hot_reloadable,
            encrypted: self.encrypted,
            experimental: self.experimental,
            tags: self
                .tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// 根据中间表示还原元数据。
    ///
    /// ### 后置条件（Postconditions）
    /// - 返回的 [`ConfigMetadata`] 将所有标签存储为拥有型 `Cow::Owned`，确保生命周期独立于输入缓冲区。
    pub(crate) fn from_repr(repr: ConfigMetadataRepr) -> Self {
        Self {
            hot_reloadable: repr.hot_reloadable,
            encrypted: repr.encrypted,
            experimental: repr.experimental,
            tags: repr
                .tags
                .into_iter()
                .map(|(k, v)| (Cow::Owned(k), Cow::Owned(v)))
                .collect(),
        }
    }
}

impl ConfigValue {
    /// 将配置值转换为序列化友好的表示。
    pub(crate) fn to_repr(&self) -> ConfigValueRepr {
        ConfigValueRepr::from(self)
    }

    /// 根据中间表示重建配置值。
    pub(crate) fn from_repr(repr: ConfigValueRepr) -> crate::Result<Self, ConfigValueReprError> {
        match repr {
            ConfigValueRepr::Boolean { value, metadata } => Ok(ConfigValue::Boolean(
                value,
                ConfigMetadata::from_repr(metadata),
            )),
            ConfigValueRepr::Integer { value, metadata } => Ok(ConfigValue::Integer(
                value,
                ConfigMetadata::from_repr(metadata),
            )),
            ConfigValueRepr::Float { value, metadata } => Ok(ConfigValue::Float(
                value,
                ConfigMetadata::from_repr(metadata),
            )),
            ConfigValueRepr::Text { value, metadata } => Ok(ConfigValue::Text(
                Cow::Owned(value),
                ConfigMetadata::from_repr(metadata),
            )),
            ConfigValueRepr::Binary { value, metadata } => Ok(ConfigValue::Binary(
                Cow::Owned(value),
                ConfigMetadata::from_repr(metadata),
            )),
            ConfigValueRepr::Duration {
                secs,
                nanos,
                metadata,
            } => {
                if nanos >= 1_000_000_000 {
                    return Err(ConfigValueReprError::InvalidDuration { secs, nanos });
                }
                Ok(ConfigValue::Duration(
                    Duration::new(secs, nanos),
                    ConfigMetadata::from_repr(metadata),
                ))
            }
            ConfigValueRepr::List { values, metadata } => {
                let metadata = ConfigMetadata::from_repr(metadata);
                let converted = values
                    .into_iter()
                    .map(ConfigValue::from_repr)
                    .collect::<crate::Result<Vec<_>, _>>()?;
                Ok(ConfigValue::List(converted, metadata))
            }
            ConfigValueRepr::Dictionary { entries, metadata } => {
                let metadata = ConfigMetadata::from_repr(metadata);
                let converted = entries
                    .into_iter()
                    .map(|(k, v)| Ok((Cow::Owned(k), ConfigValue::from_repr(v)?)))
                    .collect::<crate::Result<Vec<_>, ConfigValueReprError>>()?;
                Ok(ConfigValue::Dictionary(converted, metadata))
            }
        }
    }
}

pub(crate) use serde_repr::{ConfigMetadataRepr, ConfigValueRepr};

mod serde_repr {
    //! `ConfigValue` 与 `ConfigMetadata` 的内部序列化表示。
    use super::*;
    use alloc::vec::Vec;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub(crate) struct ConfigMetadataRepr {
        pub hot_reloadable: bool,
        pub encrypted: bool,
        pub experimental: bool,
        pub tags: Vec<(String, String)>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    pub(crate) enum ConfigValueRepr {
        Boolean {
            value: bool,
            metadata: ConfigMetadataRepr,
        },
        Integer {
            value: i64,
            metadata: ConfigMetadataRepr,
        },
        Float {
            value: f64,
            metadata: ConfigMetadataRepr,
        },
        Text {
            value: String,
            metadata: ConfigMetadataRepr,
        },
        Binary {
            value: alloc::vec::Vec<u8>,
            metadata: ConfigMetadataRepr,
        },
        Duration {
            secs: u64,
            nanos: u32,
            metadata: ConfigMetadataRepr,
        },
        List {
            values: alloc::vec::Vec<ConfigValueRepr>,
            metadata: ConfigMetadataRepr,
        },
        Dictionary {
            entries: alloc::vec::Vec<(String, ConfigValueRepr)>,
            metadata: ConfigMetadataRepr,
        },
    }

    impl From<&ConfigMetadata> for ConfigMetadataRepr {
        fn from(metadata: &ConfigMetadata) -> Self {
            metadata.to_repr()
        }
    }

    impl From<ConfigMetadataRepr> for ConfigMetadata {
        fn from(repr: ConfigMetadataRepr) -> Self {
            ConfigMetadata::from_repr(repr)
        }
    }

    impl From<&ConfigValue> for ConfigValueRepr {
        fn from(value: &ConfigValue) -> Self {
            match value {
                ConfigValue::Boolean(v, meta) => Self::Boolean {
                    value: *v,
                    metadata: ConfigMetadataRepr::from(meta),
                },
                ConfigValue::Integer(v, meta) => Self::Integer {
                    value: *v,
                    metadata: ConfigMetadataRepr::from(meta),
                },
                ConfigValue::Float(v, meta) => Self::Float {
                    value: *v,
                    metadata: ConfigMetadataRepr::from(meta),
                },
                ConfigValue::Text(v, meta) => Self::Text {
                    value: v.to_string(),
                    metadata: ConfigMetadataRepr::from(meta),
                },
                ConfigValue::Binary(v, meta) => Self::Binary {
                    value: v.to_vec(),
                    metadata: ConfigMetadataRepr::from(meta),
                },
                ConfigValue::Duration(duration, meta) => Self::Duration {
                    secs: duration.as_secs(),
                    nanos: duration.subsec_nanos(),
                    metadata: ConfigMetadataRepr::from(meta),
                },
                ConfigValue::List(values, meta) => Self::List {
                    values: values.iter().map(ConfigValueRepr::from).collect(),
                    metadata: ConfigMetadataRepr::from(meta),
                },
                ConfigValue::Dictionary(entries, meta) => Self::Dictionary {
                    entries: entries
                        .iter()
                        .map(|(k, v)| (k.to_string(), ConfigValueRepr::from(v)))
                        .collect(),
                    metadata: ConfigMetadataRepr::from(meta),
                },
            }
        }
    }
}
