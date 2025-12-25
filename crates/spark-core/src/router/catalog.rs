use alloc::vec::Vec;

use super::{RouteId, RouteMetadata, RoutePattern};

/// 可枚举的路由描述信息，用于构建快照或对外暴露目录。
///
/// # 教案级说明
/// - **意图 (Why)**：以轻量结构承载模式、ID 与静态元数据，便于 UI/调试工具读取；
/// - **契约 (What)**：`id` 与 `metadata` 可选，便于在热更新时先填充模式再补充其他信息；
/// - **设计 (How)**：提供 Builder 风格方法 `with_id` 与 `with_metadata`，避免构造时漏填字段；
/// - **风险 (Trade-offs)**：结构体本身克隆成本与字段大小线性相关，建议在快照维持共享引用。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteDescriptor {
    pattern: RoutePattern,
    id: Option<RouteId>,
    metadata: Option<RouteMetadata>,
}

impl RouteDescriptor {
    /// 创建仅包含模式的描述对象。
    pub fn new(pattern: RoutePattern) -> Self {
        Self {
            pattern,
            id: None,
            metadata: None,
        }
    }

    /// 补充稳定 ID。
    pub fn with_id(mut self, id: RouteId) -> Self {
        self.id = Some(id);
        self
    }

    /// 补充静态元数据。
    pub fn with_metadata(mut self, metadata: RouteMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// 读取路由模式。
    pub fn pattern(&self) -> &RoutePattern {
        &self.pattern
    }

    /// 可选访问路由 ID。
    pub fn id(&self) -> Option<&RouteId> {
        self.id.as_ref()
    }

    /// 可选访问静态元数据。
    pub fn metadata(&self) -> Option<&RouteMetadata> {
        self.metadata.as_ref()
    }
}

/// 路由目录快照，维护可迭代的描述集合。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteCatalog {
    entries: Vec<RouteDescriptor>,
}

impl RouteCatalog {
    /// 创建空目录。
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// 追加一条描述记录。
    pub fn push(&mut self, descriptor: RouteDescriptor) {
        self.entries.push(descriptor);
    }

    /// 迭代当前目录内容。
    pub fn iter(&self) -> core::slice::Iter<'_, RouteDescriptor> {
        self.entries.iter()
    }
}

impl IntoIterator for RouteCatalog {
    type Item = RouteDescriptor;
    type IntoIter = alloc::vec::IntoIter<RouteDescriptor>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}
