use alloc::boxed::Box;
use core::any::{Any, TypeId};

use crate::sealed::Sealed;

/// Handler 与 Middleware 共享的扩展存储接口。
///
/// # 设计背景（Why）
/// - 借鉴 Netty `AttributeMap`、Hyper `Extensions`、Tower `Extensions`，提供类型安全、线程安全的跨阶段共享数据方案。
/// - 在生产环境中可封装为无锁或分片锁结构；在科研环境中可替换为追踪型存储（如记录读写历史）。
///
/// # 契约说明（What）
/// - 键使用 [`TypeId`]，调用方应通过新类型封装避免碰撞。
/// - 所有值需满足 `'static + Send + Sync`，确保跨线程访问安全。
/// - `insert`、`remove`、`get` 均需为 O(1) 或摊还 O(1)，以满足热路径性能要求。
///
/// # 风险提示（Trade-offs）
/// - 若实现采用引用计数容器，需注意避免循环引用；可通过弱引用或在 `remove` 时打破环。
/// - `get` 返回引用，其生命周期受限于 `&self`，调用方应在当前回调内消费避免悬垂。
pub trait ExtensionsMap: Send + Sync + Sealed {
    /// 插入指定类型 ID 对应的扩展数据。
    fn insert(&self, key: TypeId, value: Box<dyn Any + Send + Sync>);

    /// 获取扩展数据的共享引用。
    fn get<'a>(&'a self, key: &TypeId) -> Option<&'a (dyn Any + Send + Sync + 'static)>;

    /// 移除扩展数据，返回拥有所有权的 Box。
    fn remove(&self, key: &TypeId) -> Option<Box<dyn Any + Send + Sync>>;

    /// 判断扩展是否存在。
    fn contains_key(&self, key: &TypeId) -> bool;

    /// 清空所有扩展。
    fn clear(&self);
}
