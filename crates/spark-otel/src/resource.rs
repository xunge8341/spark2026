use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use spark_core::observability::ResourceAttrSet;

/// 根据 `spark-core` 的资源属性集合构造 OpenTelemetry `Resource`。
///
/// # 教案式说明
/// - **意图（Why）**：宿主在构建 Resource 时复用 `spark-core` 的轻量类型，保持可观测性标签语义一致；
/// - **逻辑（How）**：逐条映射为 OpenTelemetry 的 [`KeyValue`]，并调用 [`Resource::new`] 生成 SDK 所需结构；
/// - **契约（What）**：
///   - 输入切片应来自 `ResourceAttrSet`，通常由 `OwnedResourceAttrs` 提供；
///   - 返回的 `Resource` 不包含 schema URL；
///   - 若存在重复键，OpenTelemetry `Resource::new` 将保留最后一次出现的值。
pub fn resource_from_attrs(attrs: ResourceAttrSet<'_>) -> Resource {
    let owned = attrs
        .iter()
        .map(|attr| KeyValue::new(attr.key().to_string(), attr.value().to_string()));
    Resource::new(owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::Value;
    use spark_core::observability::OwnedResourceAttrs;
    use std::collections::HashMap;

    #[test]
    fn resource_mapping_preserves_all_attributes() {
        let mut owned = OwnedResourceAttrs::new();
        owned.push_owned("service.name", "demo");
        owned.push_owned("deployment.environment", "staging");

        let resource = resource_from_attrs(owned.as_slice());
        let mut map = HashMap::new();
        for (key, value) in resource.iter() {
            let text = match value {
                Value::String(s) => s.to_string(),
                Value::Bool(flag) => flag.to_string(),
                Value::F64(number) => number.to_string(),
                Value::I64(number) => number.to_string(),
                Value::Array(array) => format!("{:?}", array),
            };
            map.insert(key.as_str().to_string(), text);
        }

        assert_eq!(map.get("service.name").unwrap(), "demo");
        assert_eq!(map.get("deployment.environment").unwrap(), "staging");
    }
}
