//! 统一的堆分配计数工具，供基准测试与契约测试复用。
//!
//! # 设计初衷（Why）
//! - 服务宏需在“构造期一次分配、调用期零分配”的前提下运作，因此需要可复用的计数器验证热路径；
//! - 合约测试与基准测试均会串行运行多段代码，通过集中式的工具模块避免重复实现全局分配器；
//! - 以 `#[path = "..."]` 方式复用本文件，无需将工具发布为独立 crate，即可在多处使用统一语义。
//!
//! # 工作原理（How）
//! - 通过自定义 [`GlobalAlloc`] 实现，在 `alloc`/`alloc_zeroed`/`realloc` 返回成功指针后递增计数；
//! - 以互斥锁序列化测试/基准代码块，避免多个线程同时修改全局计数导致读数混乱；
//! - 提供 `reset`/`snapshot`/`total` 等便捷函数，让调用方以最低成本圈定测量窗口。
//!
//! # 契约说明（What）
//! - **前置条件**：调用方需在测试/基准入口声明 `#[global_allocator]` 使用 [`CountingAllocator`]；
//! - **输入输出**：通过 [`AllocationScope::lock`] 获得互斥锁后执行待测代码，随后以 [`snapshot_allocations`] 或
//!   [`total_allocations`] 读取分配次数；
//! - **后置条件**：互斥锁释放时自动恢复默认状态，不会持久修改调用方环境。
//!
//! # 风险提示（Trade-offs & Gotchas）
//! - 计数器基于原子操作，虽然性能损耗极低，但仍建议仅在测试/基准场景使用；
//! - 若调用方忘记通过 [`AllocationScope::reset`] 清零，上一次测试的残留计数会污染结果，应避免跨用例复用。

use std::{
    alloc::{GlobalAlloc, Layout, System},
    fmt,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
};

/// 记录三类堆分配事件的计数器集合。
///
/// # 结构说明（How）
/// - `alloc`：普通 `alloc` 调用次数；
/// - `alloc_zeroed`：零填充分配次数；
/// - `realloc`：重新分配次数，按成功返回计数。
///
/// # 契约（What）
/// - `total()` 返回三者之和，方便测试快速判断是否存在非零分配；
/// - `Default` 实现返回全零，便于直接作为初始状态。
#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub struct AllocationSnapshot {
    pub alloc: usize,
    pub alloc_zeroed: usize,
    pub realloc: usize,
}

impl AllocationSnapshot {
    /// 聚合所有分配次数。
    pub fn total(&self) -> usize {
        self.alloc + self.alloc_zeroed + self.realloc
    }
}

impl fmt::Debug for AllocationSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AllocationSnapshot")
            .field("alloc", &self.alloc)
            .field("alloc_zeroed", &self.alloc_zeroed)
            .field("realloc", &self.realloc)
            .field("total", &self.total())
            .finish()
    }
}

/// 自定义全局分配器，实现对分配事件的细粒度计数。
///
/// # 设计动机（Why）
/// - 验证 `#[spark::service]` 宏在热路径的分配行为，需要捕获所有堆内存请求；
/// - 通过委托给系统分配器，避免破坏既有语义，仅在成功时递增计数；
/// - 结构体本身不携带状态，可零成本复制，方便在多 crate 中复用。
pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOC_COUNTER.fetch_add(1, Ordering::SeqCst);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            ZEROED_COUNTER.fetch_add(1, Ordering::SeqCst);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            REALLOC_COUNTER.fetch_add(1, Ordering::SeqCst);
        }
        new_ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
}

/// 互斥锁封装，用于串行化待测代码并提供便捷的重置入口。
///
/// # 使用步骤（How）
/// 1. 通过 [`AllocationScope::lock`] 获取作用域守卫；
/// 2. 调用 [`AllocationScope::reset`] 清零计数，圈定新的测量窗口；
/// 3. 在作用域结束时自动解锁，允许下一个测试/基准进入。
#[derive(Default)]
pub struct AllocationScope;

impl AllocationScope {
    /// 获取独占作用域，底层由 `Mutex` 保证串行化。
    pub fn lock() -> AllocationScopeGuard {
        AllocationScopeGuard {
            _guard: ALLOC_LOCK
                .lock()
                .expect("allocation counter mutex poisoned"),
        }
    }
}

/// 作用域守卫，负责在 Drop 时释放互斥锁。
pub struct AllocationScopeGuard {
    _guard: MutexGuard<'static, ()>,
}

impl AllocationScopeGuard {
    /// 将所有计数清零，确保后续读取的数值仅来自当前作用域。
    pub fn reset(&self) {
        reset_counters();
    }

    /// 快速获取当前分配计数。
    pub fn snapshot(&self) -> AllocationSnapshot {
        snapshot_allocations()
    }
}

impl Drop for AllocationScopeGuard {
    fn drop(&mut self) {
        // 自动释放互斥锁，调用方无需显式处理。
    }
}

static ALLOC_COUNTER: AtomicUsize = AtomicUsize::new(0);
static ZEROED_COUNTER: AtomicUsize = AtomicUsize::new(0);
static REALLOC_COUNTER: AtomicUsize = AtomicUsize::new(0);
static ALLOC_LOCK: Mutex<()> = Mutex::new(());

/// 将所有原子计数清零。
pub fn reset_counters() {
    ALLOC_COUNTER.store(0, Ordering::SeqCst);
    ZEROED_COUNTER.store(0, Ordering::SeqCst);
    REALLOC_COUNTER.store(0, Ordering::SeqCst);
}

/// 返回当前计数的结构化快照。
pub fn snapshot_allocations() -> AllocationSnapshot {
    AllocationSnapshot {
        alloc: ALLOC_COUNTER.load(Ordering::SeqCst),
        alloc_zeroed: ZEROED_COUNTER.load(Ordering::SeqCst),
        realloc: REALLOC_COUNTER.load(Ordering::SeqCst),
    }
}

/// 读取所有分配事件的总次数。
pub fn total_allocations() -> usize {
    snapshot_allocations().total()
}
