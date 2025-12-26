//! 类型安全的动态属性容器：面向核心内核（kernel）暴露的最小共享扩展点。
//!
//! # 模块意图（Why）
//! - 为调用上下文、Pipeline 扩展等提供“自由附着”的存储位，不强加具体业务语义；
//! - 通过 `TypeId` 做键、`Box<dyn Any>` 做值，实现“按类型”检索，避免字符串键带来的运行时错误；
//! - 线程安全且支持 `no_std + alloc`，可在平台/数据面全链路复用。
//!
//! # 契约定义（What）
//! - **输入**：调用方传入任意 `Send + Sync + 'static` 的类型实例；
//! - **输出**：可通过类型安全的读守卫访问，或以所有权方式移除旧值；
//! - **前置条件**：调用方需确保同一类型的值语义唯一，否则后插入会覆盖之前的值；
//! - **后置条件**：容器维护 `TypeId -> Box<dyn Any>` 的映射，读写均受 `RwLock` 保护。
//!
//! # 实现总览（How）
//! 1. 使用 `spin::RwLock` 包装 `BTreeMap<TypeId, Box<dyn Any + Send + Sync>>` 提供并发读写；
//! 2. 读操作返回自定义的 [`AttributeReadGuard`]，在类型转换成功后携带读锁生命周期，保证引用安全；
//! 3. 写操作提供插入、移除与“若不存在则创建”的接口，便于上层以声明式方式管理属性；
//! 4. 通过 `fmt::Debug` 实现仅暴露类型数量，避免泄露具体值。
//!
//! # 风险与注意事项（Trade-offs & Gotchas）
//! - 容器按类型唯一存储，若业务需要“同类型多实例”，应自定义包裹类型作为键；
//! - 读写均为无阻塞自旋锁，适合短临界区；长时间持有守卫可能影响低延迟场景；
//! - Downcast 依赖 `TypeId`，跨 crate 定义的同名类型不会冲突，但类型重定义会视为不同键。

extern crate alloc;

use alloc::{boxed::Box, collections::BTreeMap};
use core::any::{Any, TypeId};
use core::fmt;
use core::marker::PhantomData;
use core::ops::Deref;

use spin::{RwLock, RwLockReadGuard};

/// 内部映射类型：以 `TypeId` 为键存储任意线程安全的值。
type AttributeMap = BTreeMap<TypeId, Box<dyn Any + Send + Sync>>;

/// 只读守卫：在 `RwLock` 生命周期内为调用方提供类型安全的借用。
///
/// # 设计思路（Why）
/// - 直接返回 `&T` 将导致锁提前释放后悬垂；通过持有读锁守卫避免借用失效；
/// - 让调用方以解引用的方式使用目标类型，同时隐藏底层锁的具体实现。
///
/// # 使用契约（What）
/// - 调用方不可存储超出生命周期的引用；
/// - 若需要长期持有，可在自己的结构中包裹 `AttributeReadGuard`，保证锁语义显式化。
pub struct AttributeReadGuard<'a, T: Send + Sync + 'static> {
    guard: RwLockReadGuard<'a, AttributeMap>,
    marker: PhantomData<&'a T>,
}

impl<'a, T: Send + Sync + 'static> Deref for AttributeReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())
            .expect("属性守卫在创建时已校验类型存在")
    }
}

/// 请求范围（request-scoped）的动态属性容器。
#[derive(Default)]
pub struct Attributes {
    map: RwLock<AttributeMap>,
}

impl Attributes {
    /// 创建一个空的属性容器。
    ///
    /// ## 行为（How）
    /// - 使用 `Default` 初始化内部 `BTreeMap` 与 `RwLock`；
    /// - 保持零分配，直到首次插入值时才会分配节点。
    pub const fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
        }
    }

    /// 插入或覆盖指定类型的值，并返回旧值（若存在）。
    ///
    /// ## 参数（What）
    /// - `value`：待存储的实例，必须满足 `Send + Sync + 'static` 以保证线程安全与类型唯一性。
    ///
    /// ## 逻辑（How）
    /// - 获取写锁，依据 `TypeId` 作为键插入新值；
    /// - 若存在旧值，尝试 downcast 成 `T` 并以所有权方式返回，失败则视为类型不匹配并丢弃旧值。
    ///
    /// ## 后置条件（What）
    /// - 容器中该类型的值被替换为新的实例；
    /// - 调用方可根据返回值判断是否覆盖了已有配置。
    pub fn insert<T: Send + Sync + 'static>(&self, value: T) -> Option<T> {
        let mut guard = self.map.write();
        let previous = guard.insert(TypeId::of::<T>(), Box::new(value));

        previous
            .and_then(|boxed| boxed.downcast::<T>().ok())
            .map(|boxed| *boxed)
    }

    /// 获取指定类型的共享引用，并绑定读锁生命周期。
    ///
    /// ## 前置条件（What）
    /// - 调用方需了解存储的具体类型；类型不匹配将返回 `None`。
    ///
    /// ## 逻辑（How）
    /// - 持有读锁后根据 `TypeId` 查找；
    /// - 成功 downcast 时构造 [`AttributeReadGuard`]，使引用与锁生命周期一致。
    ///
    /// ## 后置条件（What）
    /// - 返回的守卫在生命周期内保证引用有效；
    /// - 若不存在或类型不符则返回 `None`，不产生副作用。
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<AttributeReadGuard<'_, T>> {
        let guard = self.map.read();
        guard
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())?;

        Some(AttributeReadGuard {
            guard,
            marker: PhantomData,
        })
    }

    /// 获取或在缺失时创建指定类型的值，并返回只读守卫。
    ///
    /// ## 参数（What）
    /// - `builder`：在类型缺失时创建默认值的闭包，需返回 `T`。
    ///
    /// ## 逻辑（How）
    /// - 先尝试获取读锁并查找；若存在则直接返回守卫；
    /// - 若不存在，升级为写锁插入新值，再降级为读锁返回守卫；
    /// - 避免不必要的克隆，确保只在缺失分支构造实例。
    ///
    /// ## 后置条件（What）
    /// - 容器保证存在该类型的值；
    /// - 返回的守卫总是指向当前存储的实例。
    pub fn get_or_insert_with<T, F>(&self, builder: F) -> AttributeReadGuard<'_, T>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> T,
    {
        if let Some(existing) = self.get::<T>() {
            return existing;
        }

        {
            let mut write_guard = self.map.write();
            write_guard
                .entry(TypeId::of::<T>())
                .or_insert_with(|| Box::new(builder()));
        }

        // 插入完成后重新获取读锁，确保返回的引用与读锁生命周期绑定。
        self.get::<T>()
            .expect("插入后应当能够读取到刚写入的属性：内部状态不一致")
    }

    /// 移除并返回指定类型的值（若存在）。
    ///
    /// ## 逻辑（How）
    /// - 通过写锁删除映射项；
    /// - 成功时 downcast 为具体类型并以所有权形式交还调用方。
    ///
    /// ## 风险与边界（Trade-offs）
    /// - 若调用方错误地假设类型兼容，会导致返回 `None`，需提前校验或借助单元测试覆盖。
    pub fn remove<T: Send + Sync + 'static>(&self) -> Option<T> {
        let mut guard = self.map.write();
        guard
            .remove(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast::<T>().ok())
            .map(|boxed| *boxed)
    }

    /// 清空容器中的所有属性。
    ///
    /// ## 注意事项
    /// - 操作为破坏性清理，调用前应确认无其他组件依赖现有属性。
    pub fn clear(&self) {
        let mut guard = self.map.write();
        guard.clear();
    }
}

impl fmt::Debug for Attributes {
    /// 仅暴露属性数量，避免在日志中泄漏具体类型与内容。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.map.read().len();
        f.debug_struct("Attributes").field("len", &count).finish()
    }
}
