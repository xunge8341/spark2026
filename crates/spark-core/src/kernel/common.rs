use core::any::Any;

use crate::{deprecation::LEGACY_LOOPBACK_OUTBOUND, observability::Logger, sealed::Sealed};

/// 表示空返回值，常用于无需携带数据的响应。
///
/// # 设计背景（Why）
/// - 某些 Service/Handler 仅需表示成功，无额外载荷。
///
/// # 契约说明（What）
/// - `Empty` 可作为泛型响应类型的占位符，序列化层可选择忽略或输出固定内容。
#[derive(Debug, Clone, Copy)]
pub struct Empty;

/// 将某个类型转换为空值。
///
/// # 契约说明（What）
/// - 提供统一的 `into_empty` 方法，便于在泛型上下文中将 `()` 等类型转为 `Empty`。
pub trait IntoEmpty: Sealed {
    /// 消费自身并返回 `Empty`。
    fn into_empty(self) -> Empty;
}

impl IntoEmpty for () {
    fn into_empty(self) -> Empty {
        Empty
    }
}

/// Loopback 提供在 Handler 内部向自身注入事件的能力。
///
/// # 设计背景（Why）
/// - 支持 Handler 在异步任务完成后回调自身，例如触发补发或心跳。
///
/// # 契约说明（What）
/// - `fire_loopback_inbound` 将事件注入入站链路，`fire_loopback_outbound` 注入出站链路。
/// - 事件类型要求实现 `Any + Send + Sync`，以保证跨线程安全。
///
/// # 风险提示（Trade-offs）
/// - 循环注入可能导致无限递归，调用方需自行控制次数或添加去抖逻辑。
pub trait Loopback: Send + Sync + Sealed {
    /// 向入站路径注入事件。
    fn fire_loopback_inbound(&self, event: impl Any + Send + Sync);

    /// 向出站路径注入事件。
    fn fire_loopback_outbound(&self, event: impl Any + Send + Sync);
}

/// **弃用函数**：旧版 Loopback 出站注入兼容层。
///
/// # 背景说明（Why）
/// - 在 0.0.x 时代，历史代码通过该辅助函数直接转发出站事件；随着核心 Trait 成熟，推荐直接调用
///   [`Loopback::fire_loopback_outbound`] 以减少一层包装。
/// - 为满足“两版过渡”策略，保留该函数至 `0.3.0`，并在运行时提示迁移路径。
///
/// # 逻辑解析（How）
/// 1. 调用 [`LEGACY_LOOPBACK_OUTBOUND`] 的 `emit` 方法输出一次性 WARN 告警。
/// 2. 将事件原样转发给 `loopback.fire_loopback_outbound`，保持功能等价。
///
/// # 契约说明（What）
/// - **输入参数**：
///   - `loopback`：目标 Loopback 实例，类型 `L` 必须实现 [`Loopback`] Trait，可为具体类型或引用包装。
///   - `event`：任意实现 `Any + Send + Sync` 的事件对象。
///   - `logger`：可选结构化日志句柄；缺省时在 `std` 环境降级为标准错误输出。
/// - **前置条件**：调用者已确认仍依赖旧路径，且知晓后续迁移计划。
/// - **后置条件**：事件会被正常转发，且在首次调用时输出弃用告警。
///
/// # 风险提示（Trade-offs & Gotchas）
/// - 若多线程同时调用，告警仍只会出现一次；请在迁移完成后尽快移除调用点。
/// - 在纯 `alloc` 环境中未提供 `logger` 将看不到告警，需要宿主补充兜底通知。
#[deprecated(
    since = "0.1.0",
    note = "removal: v0.3.0; migration: use Loopback::fire_loopback_outbound; tracking: T23"
)]
pub fn legacy_loopback_outbound<L: Loopback + ?Sized>(
    loopback: &L,
    event: impl Any + Send + Sync,
    logger: Option<&dyn Logger>,
) {
    LEGACY_LOOPBACK_OUTBOUND.emit(logger);
    loopback.fire_loopback_outbound(event);
}
