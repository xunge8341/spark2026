use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};
use core::fmt::{self, Write as _};

use super::{
    ConfigKey, ConfigMetadata, ConfigValue, ConfigurationLayer, ProfileDescriptor, ProfileLayering,
    ResolvedConfiguration, SourceMetadata,
};

/// 配置快照中单个键值对的序列化表示。
///
/// ### 设计目标（Why）
/// - 将 [`ConfigKey`] 与其值的敏感信息脱敏后进行持久化，便于审计和实验复现。
/// - 提供稳定的结构，保证跨版本快照具备向后兼容性。
///
/// ### 契约说明（What）
/// - `key`：采用 `domain::name@scope` 的稳定字符串，便于人类与机器同时理解。
/// - `value`：经过脱敏处理的枚举表示，敏感内容会统一输出 `[REDACTED]`。
/// - `metadata`：透传非敏感的配置元信息，帮助评估热更新与实验标记。
///
/// ### 实现细节（How）
/// - 通过内部辅助函数 `SnapshotValue::from_config_value` 在构造时完成递归脱敏，避免序列化期间再次判断。
/// - `SnapshotMetadata` 仅保留布尔与标签，确保快照中不会泄露密钥或凭据。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub key: String,
    pub value: SnapshotValue,
    pub metadata: SnapshotMetadata,
}

impl SnapshotEntry {
    /// 从配置键值对构造快照条目。
    ///
    /// ### 参数（Inputs）
    /// - `key`：原始配置键的引用。
    /// - `value`：对应的配置值引用。
    ///
    /// ### 前置条件（Preconditions）
    /// - `key` 与 `value` 必须来自同一 [`ResolvedConfiguration`]，否则快照内容将缺乏一致性保障。
    ///
    /// ### 后置条件（Postconditions）
    /// - 返回的结构体包含一个脱敏后的深拷贝，可安全用于序列化与跨线程传递。
    pub fn from_key_value(key: &ConfigKey, value: &ConfigValue) -> Self {
        Self {
            key: key.to_string(),
            value: SnapshotValue::from_config_value(value),
            metadata: SnapshotMetadata::from_metadata(value.metadata()),
        }
    }
}

/// 快照中的配置元数据表示。
///
/// ### 设计目标（Why）
/// - 仅保留与敏感信息无关的状态位，方便在审计中判断配置可否热更新、是否处于实验阶段。
/// - 通过标签 `tags` 暴露额外上下文，但调用方应避免在标签中存放密钥。
///
/// ### 实现细节（How）
/// - 直接复制布尔字段，`tags` 会深拷贝键值但不会再做额外脱敏处理。
/// - 若业务侧需要进一步约束标签内容，应在写入配置中心时完成校验。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMetadata {
    pub hot_reloadable: bool,
    pub encrypted: bool,
    pub experimental: bool,
    pub tags: Vec<(String, String)>,
}

impl SnapshotMetadata {
    fn from_metadata(metadata: &ConfigMetadata) -> Self {
        Self {
            hot_reloadable: metadata.hot_reloadable,
            encrypted: metadata.encrypted,
            experimental: metadata.experimental,
            tags: metadata
                .tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

/// 快照中支持的脱敏后配置值。
///
/// ### 设计目标（Why）
/// - 通过枚举变体表达不同类型，同时提供统一的 `Redacted` 变体屏蔽敏感值。
/// - 避免在序列化阶段重复判定是否敏感，提升性能并降低逻辑复杂度。
///
/// ### 契约说明（What）
/// - `Redacted`：用于所有标记为加密或被动泄露风险的值（如二进制）。
/// - 其余标量按照原样保存；列表、字典会递归脱敏子元素。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotValue {
    Redacted,
    Boolean(bool),
    Integer(i64),
    Float(u64),
    Text(String),
    Duration(u128),
    List(Vec<SnapshotValue>),
    Dictionary(Vec<(String, SnapshotValue)>),
}

impl SnapshotValue {
    /// 根据配置值构造脱敏表示。
    ///
    /// ### 逻辑解析（How）
    /// - 若元数据标记 `encrypted`，无论实际类型为何都统一脱敏。
    /// - 对于二进制值，即便未标记加密，也视为敏感信息直接脱敏。
    /// - 列表、字典会在递归过程中继续应用上述规则，确保嵌套结构同样安全。
    fn from_config_value(value: &ConfigValue) -> Self {
        if value.metadata().encrypted {
            return SnapshotValue::Redacted;
        }
        match value {
            ConfigValue::Boolean(v, _) => SnapshotValue::Boolean(*v),
            ConfigValue::Integer(v, _) => SnapshotValue::Integer(*v),
            ConfigValue::Float(v, _) => SnapshotValue::Float(v.to_bits()),
            ConfigValue::Text(v, _) => SnapshotValue::Text(v.to_string()),
            ConfigValue::Binary(_, _) => SnapshotValue::Redacted,
            ConfigValue::Duration(duration, _) => SnapshotValue::Duration(duration.as_nanos()),
            ConfigValue::List(values, metadata) => {
                if metadata.encrypted {
                    SnapshotValue::Redacted
                } else {
                    SnapshotValue::List(
                        values
                            .iter()
                            .map(SnapshotValue::from_config_value)
                            .collect(),
                    )
                }
            }
            ConfigValue::Dictionary(entries, metadata) => {
                if metadata.encrypted {
                    SnapshotValue::Redacted
                } else {
                    SnapshotValue::Dictionary(
                        entries
                            .iter()
                            .map(|(k, v)| (k.to_string(), SnapshotValue::from_config_value(v)))
                            .collect(),
                    )
                }
            }
        }
    }

    /// 将当前值写入 JSON 字符串缓冲区。
    ///
    /// ### 设计说明（How）
    /// - 采用手写序列化避免额外依赖，在 `no_std + alloc` 场景仍可使用。
    /// - 所有字符串都会调用 [`escape_json_string`] 进行转义，防止特殊字符破坏文档结构。
    fn write_json(&self, out: &mut String, indent: usize) -> fmt::Result {
        match self {
            SnapshotValue::Redacted => out.write_str("\"[REDACTED]\""),
            SnapshotValue::Boolean(v) => {
                if *v {
                    out.write_str("true")
                } else {
                    out.write_str("false")
                }
            }
            SnapshotValue::Integer(v) => write!(out, "{}", v),
            SnapshotValue::Float(bits) => {
                let value = f64::from_bits(*bits);
                if value.is_finite() {
                    write!(out, "{}", value)
                } else {
                    out.write_str("\"[NON_FINITE]\"")
                }
            }
            SnapshotValue::Text(v) => {
                write!(out, "\"{}\"", escape_json_string(v))
            }
            SnapshotValue::Duration(nanos) => write!(out, "{{\"nanos\":{}}}", nanos),
            SnapshotValue::List(values) => {
                out.write_char('[')?;
                if !values.is_empty() {
                    out.write_char('\n')?;
                }
                for (idx, value) in values.iter().enumerate() {
                    write_indent(out, indent + 2)?;
                    value.write_json(out, indent + 2)?;
                    if idx + 1 != values.len() {
                        out.write_char(',')?;
                    }
                    out.write_char('\n')?;
                }
                write_indent(out, indent)?;
                out.write_char(']')
            }
            SnapshotValue::Dictionary(entries) => {
                out.write_char('{')?;
                if !entries.is_empty() {
                    out.write_char('\n')?;
                }
                for (idx, (key, value)) in entries.iter().enumerate() {
                    write_indent(out, indent + 2)?;
                    write!(out, "\"{}\": ", escape_json_string(key))?;
                    value.write_json(out, indent + 2)?;
                    if idx + 1 != entries.len() {
                        out.write_char(',')?;
                    }
                    out.write_char('\n')?;
                }
                write_indent(out, indent)?;
                out.write_char('}')
            }
        }
    }
}

/// 配置快照概要信息。
///
/// ### 设计目的（Why）
/// - 在审计场景需要快速确认快照来源与层级信息，因此额外保留 Profile 与 Layer 的概览。
/// - 提供 `to_json` 产出稳定文本，便于存档或 diff。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigurationSnapshot {
    pub profile: SnapshotProfile,
    pub version: u64,
    pub layers: Vec<SnapshotLayer>,
    pub entries: Vec<SnapshotEntry>,
}

impl ConfigurationSnapshot {
    /// 基于构建结果生成快照。
    ///
    /// ### 参数（Inputs）
    /// - `profile`：当前使用的 Profile 描述。
    /// - `layers`：构建过程中得到的配置层集合。
    /// - `resolved`：最终合并后的配置结果，用于生成键值列表。
    ///
    /// ### 前置条件（Preconditions）
    /// - `resolved.values` 应与 `layers` 对齐，否则快照无法完整反映数据来源。
    ///
    /// ### 后置条件（Postconditions）
    /// - 返回的快照为完全脱敏的结构，适合直接写入审计存储。
    pub fn capture(
        profile: &ProfileDescriptor,
        layers: &[ConfigurationLayer],
        resolved: &ResolvedConfiguration,
    ) -> Self {
        let profile_snapshot = SnapshotProfile::from_descriptor(profile);
        let layer_snapshots = layers.iter().map(SnapshotLayer::from_metadata).collect();
        let entries = resolved
            .values
            .iter()
            .map(|(key, value)| SnapshotEntry::from_key_value(key, value))
            .collect();

        Self {
            profile: profile_snapshot,
            version: resolved.version,
            layers: layer_snapshots,
            entries,
        }
    }

    /// 将快照序列化为 JSON 文本。
    ///
    /// ### 逻辑概览（How）
    /// - 采用稳定的键顺序：Profile → Version → Layers → Entries。
    /// - 使用两空格缩进保证 diff 友好。
    ///
    /// ### 返回值（What）
    /// - 完整 JSON 字符串；调用方可直接写入磁盘或用于比对。
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        out.push_str("{\n");
        write_indent(&mut out, 2).expect("indent write");
        out.push_str("\"profile\": {\n");
        self.profile.write_json(&mut out, 4).expect("profile json");
        out.push('\n');
        write_indent(&mut out, 2).expect("indent write");
        write!(
            &mut out,
            "}},\n  \"version\": {},\n  \"layers\": [\n",
            self.version
        )
        .expect("write header");

        for (idx, layer) in self.layers.iter().enumerate() {
            layer.write_json(&mut out, 4).expect("layer json");
            if idx + 1 != self.layers.len() {
                out.push_str(",\n");
            } else {
                out.push('\n');
            }
        }

        write_indent(&mut out, 2).expect("indent write");
        out.push_str("],\n  \"entries\": [\n");

        for (idx, entry) in self.entries.iter().enumerate() {
            entry.write_json(&mut out, 4).expect("entry json");
            if idx + 1 != self.entries.len() {
                out.push_str(",\n");
            } else {
                out.push('\n');
            }
        }

        write_indent(&mut out, 2).expect("indent write");
        out.push_str("]\n}");
        out
    }
}

/// Profile 的快照视图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotProfile {
    pub identifier: String,
    pub layering: &'static str,
    pub extends: Vec<String>,
    pub summary: String,
}

impl SnapshotProfile {
    fn from_descriptor(descriptor: &ProfileDescriptor) -> Self {
        Self {
            identifier: descriptor.identifier.as_str().to_string(),
            layering: layering_label(descriptor.layering),
            extends: descriptor
                .extends
                .iter()
                .map(|id| id.as_str().to_string())
                .collect(),
            summary: descriptor.summary.to_string(),
        }
    }

    fn write_json(&self, out: &mut String, indent: usize) -> fmt::Result {
        write_indent(out, indent)?;
        writeln!(
            out,
            "\"identifier\": \"{}\",",
            escape_json_string(&self.identifier)
        )?;
        write_indent(out, indent)?;
        writeln!(out, "\"layering\": \"{}\",", self.layering)?;
        write_indent(out, indent)?;
        out.push_str("\"extends\": [");
        if !self.extends.is_empty() {
            out.push('\n');
            for (idx, extend) in self.extends.iter().enumerate() {
                write_indent(out, indent + 2)?;
                write!(out, "\"{}\"", escape_json_string(extend))?;
                if idx + 1 != self.extends.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            write_indent(out, indent)?;
        }
        out.push_str("],\n");
        write_indent(out, indent)?;
        write!(
            out,
            "\"summary\": \"{}\"",
            escape_json_string(&self.summary)
        )
    }
}

/// 单个 Layer 的快照视图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotLayer {
    pub name: String,
    pub priority: u16,
    pub version: Option<String>,
}

impl SnapshotLayer {
    fn from_metadata(layer: &ConfigurationLayer) -> Self {
        let metadata: &SourceMetadata = &layer.metadata;
        Self {
            name: metadata.name.to_string(),
            priority: metadata.priority,
            version: metadata.version.as_ref().map(ToString::to_string),
        }
    }

    fn write_json(&self, out: &mut String, indent: usize) -> fmt::Result {
        write_indent(out, indent)?;
        out.push('{');
        out.push('\n');
        write_indent(out, indent + 2)?;
        writeln!(out, "\"name\": \"{}\",", escape_json_string(&self.name))?;
        write_indent(out, indent + 2)?;
        writeln!(out, "\"priority\": {},", self.priority)?;
        write_indent(out, indent + 2)?;
        match &self.version {
            Some(version) => {
                writeln!(out, "\"version\": \"{}\"", escape_json_string(version))?;
            }
            None => {
                out.push_str("\"version\": null\n");
            }
        }
        write_indent(out, indent)?;
        out.push('}');
        Ok(())
    }
}

impl SnapshotEntry {
    fn write_json(&self, out: &mut String, indent: usize) -> fmt::Result {
        write_indent(out, indent)?;
        out.push('{');
        out.push('\n');
        write_indent(out, indent + 2)?;
        writeln!(out, "\"key\": \"{}\",", escape_json_string(&self.key))?;
        write_indent(out, indent + 2)?;
        out.push_str("\"value\": ");
        self.value.write_json(out, indent + 2)?;
        out.push_str(",\n");
        write_indent(out, indent + 2)?;
        out.push_str("\"metadata\": {");
        out.push('\n');
        write_indent(out, indent + 4)?;
        writeln!(
            out,
            "\"hot_reloadable\": {},",
            if self.metadata.hot_reloadable {
                "true"
            } else {
                "false"
            }
        )?;
        write_indent(out, indent + 4)?;
        writeln!(
            out,
            "\"encrypted\": {},",
            if self.metadata.encrypted {
                "true"
            } else {
                "false"
            }
        )?;
        write_indent(out, indent + 4)?;
        write!(
            out,
            "\"experimental\": {}",
            if self.metadata.experimental {
                "true"
            } else {
                "false"
            }
        )?;
        if self.metadata.tags.is_empty() {
            out.push_str(",\n");
            write_indent(out, indent + 4)?;
            out.push_str("\"tags\": []\n");
        } else {
            out.push_str(",\n");
            write_indent(out, indent + 4)?;
            out.push_str("\"tags\": [\n");
            for (idx, (key, value)) in self.metadata.tags.iter().enumerate() {
                write_indent(out, indent + 6)?;
                write!(
                    out,
                    "{{\"key\": \"{}\", \"value\": \"{}\"}}",
                    escape_json_string(key),
                    escape_json_string(value)
                )?;
                if idx + 1 != self.metadata.tags.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            write_indent(out, indent + 4)?;
            out.push_str("]\n");
        }
        write_indent(out, indent + 2)?;
        out.push_str("}\n");
        write_indent(out, indent)?;
        out.push('}');
        Ok(())
    }
}

fn layering_label(layering: ProfileLayering) -> &'static str {
    match layering {
        ProfileLayering::BaseFirst => "base_first",
        ProfileLayering::OverrideFirst => "override_first",
    }
}

fn write_indent(out: &mut String, indent: usize) -> fmt::Result {
    for _ in 0..indent {
        out.write_char(' ')?;
    }
    Ok(())
}

fn escape_json_string(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => {
                let code = ch as u32;
                escaped.push_str("\\u");
                let hex = format!("{code:04x}");
                escaped.push_str(&hex);
            }
            ch => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{borrow::Cow, collections::BTreeMap};

    use crate::configuration::{
        ConfigKey, ConfigMetadata, ConfigScope, ConfigValue, ProfileId, ProfileLayering,
    };

    #[test]
    fn snapshot_redacts_encrypted_text() {
        let profile = ProfileDescriptor::new(
            ProfileId::new("test"),
            vec![],
            ProfileLayering::BaseFirst,
            "unit test",
        );
        let layer = ConfigurationLayer {
            metadata: SourceMetadata::new("unit", 0, None),
            entries: vec![],
        };
        let mut values = BTreeMap::new();
        values.insert(
            ConfigKey::new("domain", "name", ConfigScope::Global, "summary"),
            ConfigValue::Text(
                Cow::Borrowed("secret"),
                ConfigMetadata {
                    hot_reloadable: false,
                    encrypted: true,
                    experimental: false,
                    tags: Vec::new(),
                },
            ),
        );
        let resolved = ResolvedConfiguration { values, version: 1 };
        let snapshot = ConfigurationSnapshot::capture(&profile, &[layer], &resolved);
        let json = snapshot.to_json();
        assert!(json.contains("[REDACTED]"));
        assert!(!json.contains("secret"));
    }
}
