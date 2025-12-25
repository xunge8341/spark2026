use alloc::string::String;

use sha2::{Digest, Sha256};

use crate::configuration::{ConfigKey, ConfigMetadata, ConfigValue};

/// 负责生成稳定哈希的辅助器。
///
/// ## 设计动机（Why）
/// - 审计事件要求将变更前后的状态编码为哈希链，以检测事件缺失或篡改。
/// - 通过集中化实现，确保运行时与回放工具之间使用一致的编码逻辑。
pub struct AuditStateHasher;

impl AuditStateHasher {
    /// 计算完整配置映射的哈希。
    ///
    /// ### 参数（Inputs）
    /// - `iter`: 迭代器，按稳定顺序返回 `(ConfigKey, ConfigValue)` 对。
    ///
    /// ### 逻辑（How）
    /// - 对每个键写入字符串形式 `domain::name@scope`；
    /// - 对值执行类型分派，逐项写入标记、原始值或子项；
    /// - 最终返回 SHA-256 的十六进制字符串。
    pub fn hash_configuration<'a, I>(iter: I) -> String
    where
        I: IntoIterator<Item = (&'a ConfigKey, &'a ConfigValue)>,
    {
        let mut hasher = Sha256::new();
        for (key, value) in iter {
            hasher.update(key.to_string().as_bytes());
            hasher.update([0u8]);
            Self::hash_value(&mut hasher, value);
            hasher.update([0xFF]);
        }
        let digest = hasher.finalize();
        hex_encode(&digest)
    }

    fn hash_value(hasher: &mut Sha256, value: &ConfigValue) {
        match value {
            ConfigValue::Boolean(v, meta) => {
                hasher.update(b"bool");
                hasher.update([*v as u8]);
                Self::hash_metadata(hasher, meta);
            }
            ConfigValue::Integer(v, meta) => {
                hasher.update(b"int");
                hasher.update(v.to_le_bytes());
                Self::hash_metadata(hasher, meta);
            }
            ConfigValue::Float(v, meta) => {
                hasher.update(b"float");
                hasher.update(v.to_bits().to_le_bytes());
                Self::hash_metadata(hasher, meta);
            }
            ConfigValue::Text(v, meta) => {
                hasher.update(b"text");
                hasher.update(v.as_bytes());
                Self::hash_metadata(hasher, meta);
            }
            ConfigValue::Binary(v, meta) => {
                hasher.update(b"binary");
                hasher.update(v);
                Self::hash_metadata(hasher, meta);
            }
            ConfigValue::Duration(duration, meta) => {
                hasher.update(b"duration");
                hasher.update(duration.as_nanos().to_le_bytes());
                Self::hash_metadata(hasher, meta);
            }
            ConfigValue::List(values, meta) => {
                hasher.update(b"list");
                Self::hash_metadata(hasher, meta);
                for item in values {
                    Self::hash_value(hasher, item);
                }
            }
            ConfigValue::Dictionary(entries, meta) => {
                hasher.update(b"dict");
                Self::hash_metadata(hasher, meta);
                let mut sorted = entries.clone();
                sorted.sort_by(|(a, _), (b, _)| a.cmp(b));
                for (key, value) in sorted {
                    hasher.update(key.as_bytes());
                    Self::hash_value(hasher, &value);
                }
            }
        }
    }

    fn hash_metadata(hasher: &mut Sha256, metadata: &ConfigMetadata) {
        hasher.update([metadata.hot_reloadable as u8]);
        hasher.update([metadata.encrypted as u8]);
        hasher.update([metadata.experimental as u8]);
        for (key, value) in &metadata.tags {
            hasher.update(key.as_bytes());
            hasher.update(value.as_bytes());
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(TABLE[(byte >> 4) as usize] as char);
        out.push(TABLE[(byte & 0x0F) as usize] as char);
    }
    out
}
