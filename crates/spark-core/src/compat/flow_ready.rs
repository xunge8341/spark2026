use core::task::Poll;

use crate::status::{PollReady, ReadyCheck, ReadyState};

/// # 教案级函数说明：`to_ready_state`
///
/// ## 目标与背景（Why）
/// - 历史上各域的 `poll_ready` 实现沿用了自定义的“繁忙/背压”枚举，迁移到统一的 [`ReadyState`] 需要一层可复用的转换。
/// - 在 `compat_v0` 阶段我们不希望大规模重构旧枚举，因此提供此函数作为桥接入口，帮助旧类型在不修改现有逻辑的情况下进入新语义体系。
///
/// ## 架构定位（Where）
/// - 该函数位于 `spark-core::compat::flow_ready`，属于跨域共享的过渡层，所有旧版就绪信号都应统一调用它进行标准化。
/// - 当各域完成全面迁移后，该模块即可整体删除，核心路径 (`spark-core::status`) 不会受到影响。
///
/// ## 设计策略（Pattern）
/// - 通过要求输入类型实现 `Into<ReadyState>`，让每个域以孤立的 impl 完成适配；避免在此函数内部引入对具体枚举的判定逻辑。
/// - 此策略符合“开闭原则”：新增旧枚举只需实现 `Into`，无需修改函数本体即可复用。
///
/// ## 契约定义（What）
/// - `old`: 任意实现了 `Into<ReadyState>` 的旧版状态类型；常见的是历史的 `BusyState` 或背压枚举。
/// - 返回值：标准化后的 [`ReadyState`]，用于框架内部的统一决策。
/// - 前置条件：调用方必须在编译期确保 `old` 类型已经实现 `Into<ReadyState>`，否则编译无法通过。
/// - 后置条件：函数保证返回值属于 `ReadyState` 的合法分支，不会产生额外的包装层。
///
/// ## 核心逻辑（How）
/// 1. 直接调用 `Into::into`，将旧类型映射为统一的 `ReadyState`。
/// 2. 不引入任何运行时分支，确保零成本抽象；所有转换细节在各域的 `Into` 实现中完成。
///
/// ## 设计权衡与风险（Trade-offs）
/// - 优点：零运行时开销，迁移期间的维护成本最低。
/// - 风险：若某域忘记实现 `Into<ReadyState>` 将在编译时报错；注释中特别强调此约束以提醒使用者。
/// - 跟踪事项：参见《docs/tracking/T107.md#compat-v0-cleanup》；当所有调用方完成迁移后，即可删除 `compat_v0`
///   模块并让调用方直接返回 [`ReadyState`]，并同步清理示例与迁移文档，避免遗留错误指引。
#[inline]
pub fn to_ready_state<T>(old: T) -> ReadyState
where
    T: Into<ReadyState>,
{
    old.into()
}

/// # 教案级函数说明：`to_poll_ready`
///
/// ## 目标与背景（Why）
/// - 旧的 `poll_ready` 实现通常返回自定义的 `Poll` 包装；为兼容新契约，需要一个统一入口将 [`ReadyState`] 包装为 [`PollReady`]。
/// - 该函数保证过渡期内的返回类型保持一致，减少调用方对 `Poll<ReadyCheck<_>>` 细节的感知。
///
/// ## 架构定位（Where）
/// - 作为 `flow_ready` 的配套函数，它在旧域完成 `ReadyState` 转换后负责生成最终的 `PollReady` 值。
/// - 函数位于 `spark-core`，因而所有上层 crate（含外部扩展）都能复用，避免重复实现。
///
/// ## 契约定义（What）
/// - `state`: 已经过 `to_ready_state` 标准化的 [`ReadyState`]。
/// - 返回值：`PollReady<E>`，其中 `E` 为调用方的错误类型，占位但不会在此函数中实际构造。
/// - 前置条件：调用方必须确保输入的 `state` 来自统一语义（通常先调用 [`to_ready_state`])。
/// - 后置条件：返回值永远是 `Poll::Ready(ReadyCheck::Ready(state))`，不会产生 `Pending` 或错误分支。
///
/// ## 执行逻辑（How）
/// 1. 构造 [`ReadyCheck::Ready`]，将状态放入框架约定的枚举包装中。
/// 2. 使用 `Poll::Ready` 返回同步完成的就绪检查结果。
///
/// ## 设计权衡与风险（Trade-offs）
/// - 采用常量时间、无分支实现，确保兼容层不会成为性能瓶颈。
/// - 调用方若需要返回 `Pending`，应在调用本函数前自行判断；这里不处理重试逻辑，以保持语义清晰。
#[rustfmt::skip]
#[inline]
pub fn to_poll_ready<E>(state: ReadyState) ->
    PollReady<E> {
    Poll::Ready(ReadyCheck::Ready(state))
}
