//! Pipeline 内部热插拔存储组件。
//!
//! # 设计背景（Why）
//! - **热插拔安全切换**：在控制器运行期插入/替换 Handler 时，需要有一个可以无锁读取、同步写入的共享
//!   结构，以确保读线程始终看到稳定的 Handler 列表。本模块基于 [`ArcSwap`](crate::arc_swap::ArcSwap)
//!   封装 `Vec<Arc<...>>`，提供“读零拷贝、写原子替换”的行为。
//! - **Epoch 栅栏语义**：调用方在完成 Handler 列表更新后，通过 `bump_epoch` 上报逻辑时钟；执行路径可
//!   通过比较 `epoch()` 前后差值判断更新是否对所有线程可见，从而构建运行时热更新的安全栅栏。
//! - **契合文档需求**：此模块聚焦内部细节，不向外暴露实现类型，方便在后续迭代中替换为 RCU、EBR 等
//!   机制，同时仍保持对外 API 的稳定性。
//!
//! # 逻辑解析（How）
//! - [`HandlerEpochBuffer`]：维护当前 Handler 链路的快照与逻辑 epoch。读操作调用 [`load`] 获得 `Arc`，
//!   写操作先构造新的 `Arc<Vec<Arc<T>>>`，再调用 [`store`] 原子替换；最后执行 [`bump_epoch`] 告知所有
//!   观察者“新快照已就绪”。
//! - [`HotSwapRegistry`]：以 `ArcSwap<Vec<HandlerRegistration>>` 缓存 introspection 快照，供
//!   [`Pipeline::registry`](super::controller::Pipeline::registry) 返回值复用，避免在热路径上重复分配。
//!
//! # 契约说明（What）
//! - `HandlerEpochBuffer` 要求元素类型 `T` 实现 `Send + Sync`，以确保跨线程访问安全；读操作返回的
//!   `Arc` 可长期保存，内部通过引用计数确保旧快照在无人使用时自动释放。
//! - `HotSwapRegistry` 的 `snapshot` 返回独立的 `Vec` 拷贝，调用方可随意修改，不会影响内部状态。
//!
//! # 风险与考量（Trade-offs）
//! - `epoch` 仅是逻辑时钟，不提供强一致保证；调用方仍需在外层通过互斥或其他策略确保更新步骤的线性化。
//! - 如果未来改用 RCU/EBR，需要评估 `loom` 模型测试的覆盖情况，确保内存回收语义保持不变。

use alloc::{sync::Arc, vec::Vec};
use core::sync::atomic::{AtomicU64, Ordering};

use crate::arc_swap::ArcSwap;

use super::controller::{HandlerRegistration, HandlerRegistry};

/// 为 Handler 链路提供原子快照与 epoch 计数的缓冲区。
///
/// # 教案式说明
/// - **意图（Why）**：在 Pipeline 事件分发过程中，读线程需要无锁读取 Handler 列表，而写线程（热更新）
///   需要一次性替换整条链路并通知所有观察者；`HandlerEpochBuffer` 提供这一协调点。
/// - **逻辑（How）**：
///   1. `load` 通过 `ArcSwap::load_full` 返回当前 `Arc<Vec<Arc<T>>>` 快照；
///   2. `store` 在写路径上原子替换内部指针；
///   3. `bump_epoch` 在完成所有伴随更新（如注册表快照）后自增逻辑时钟。
/// - **契约（What）**：
///   - `T` 必须满足 `Send + Sync + 'static`，以保证多线程可见性与生命周期安全；
///   - `store` 的调用者需负责在调用 `bump_epoch` 前完成所有与链路切换相关的副作用。
/// - **风险提示（Trade-offs）**：
///   - `epoch` 自增采用 `SeqCst`，牺牲部分性能以换取跨线程时序更容易推理；
///   - 若写入频率极高，可考虑未来引入分区链路或批量提交机制以摊销成本。
pub(crate) struct HandlerEpochBuffer<T: Send + Sync + 'static> {
    snapshot: ArcSwap<Vec<Arc<T>>>,
    epoch: AtomicU64,
}

impl<T: Send + Sync + 'static> HandlerEpochBuffer<T> {
    /// 创建空缓冲区，初始 epoch 为 0。
    pub(crate) fn new() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(Vec::new()),
            epoch: AtomicU64::new(0),
        }
    }

    /// 获取当前 Handler 链路快照。
    #[inline]
    pub(crate) fn load(&self) -> Arc<Vec<Arc<T>>> {
        self.snapshot.load_full()
    }

    /// 原子替换链路快照。
    #[inline]
    pub(crate) fn store(&self, snapshot: Arc<Vec<Arc<T>>>) {
        self.snapshot.store(snapshot);
    }

    /// 返回当前逻辑 epoch。
    #[inline]
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// 在完成链路切换后自增 epoch，返回更新后的值。
    #[inline]
    pub(crate) fn bump_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// 维护 Handler 注册表快照的存储器，实现 [`HandlerRegistry`] 接口。
///
/// # 教案式说明
/// - **意图（Why）**：外部观测与调试需要了解链路结构，但不能直接持有内部可变引用，因此提供一个基于
///   `ArcSwap` 的快照容器，确保读取操作始终无锁且返回独立副本。
/// - **逻辑（How）**：更新流程由控制器完成：在构造新的 `Vec<HandlerRegistration>` 后，转换为 `Arc`
///   并调用 [`update`] 原子替换；`snapshot` 则克隆内部向量，调用方可自由检查。
/// - **契约（What）**：`HandlerRegistration` 必须保持值语义；调用方不应假设快照与运行中链路绝对同步，
///   但可以结合 `Pipeline::epoch` 判断更新进度。
/// - **风险提示（Trade-offs）**：为了保证读取零拷贝，内部仍持有 `Arc<Vec<_>>`；若注册表非常大，需评估
///   更新频率对内存占用的影响。
pub(crate) struct HotSwapRegistry {
    entries: ArcSwap<Vec<HandlerRegistration>>,
}

impl HotSwapRegistry {
    /// 构造空注册表。
    pub(crate) fn new() -> Self {
        Self {
            entries: ArcSwap::from_pointee(Vec::new()),
        }
    }

    /// 将新的 Handler 注册表快照提交给观察者。
    pub(crate) fn update(&self, snapshot: Arc<Vec<HandlerRegistration>>) {
        self.entries.store(snapshot);
    }
}

impl HandlerRegistry for HotSwapRegistry {
    fn snapshot(&self) -> Vec<HandlerRegistration> {
        (*self.entries.load_full()).clone()
    }
}
