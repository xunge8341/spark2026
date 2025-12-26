//! 动态属性容器（Attributes）
//!
//! 这是 Core 对治理数据开放的**唯一合法签证**：
//! - Core 不感知具体类型语义；
//! - Governance/Middleware 可通过 `insert/get_clone` 携带 Identity、Audit Tag、Trace 元数据等。
//!
//! 设计目标：
//! - `no_std + alloc` 兼容（使用 `BTreeMap`）；
//! - 线程安全（使用 `spin::RwLock`）；
//! - 最小依赖与最小功能集（Foundation 阶段仅提供插入与 Clone 读取）。

extern crate alloc;

use alloc::{boxed::Box, collections::BTreeMap};
use core::any::{Any, TypeId};
use core::fmt;

use spin::RwLock;

/// 请求范围（request-scoped）的动态属性容器。
#[derive(Default)]
pub struct Attributes {
    map: RwLock<BTreeMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

impl Attributes {
    /// 创建一个空的属性容器。
    pub fn new() -> Self {
        Self::default()
    }

    /// 插入一个类型化值。
    pub fn insert<T: Send + Sync + 'static>(&self, val: T) {
        let mut write = self.map.write();
        write.insert(TypeId::of::<T>(), Box::new(val));
    }

    /// 以 Clone 方式读取（Foundation：避免暴露锁守卫类型）。
    pub fn get_clone<T: Clone + Send + Sync + 'static>(&self) -> Option<T> {
        let read = self.map.read();
        read.get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())
            .cloned()
    }

    /// 清空容器。
    pub fn clear(&self) {
        let mut write = self.map.write();
        write.clear();
    }
}

impl fmt::Debug for Attributes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Attributes").finish_non_exhaustive()
    }
}
