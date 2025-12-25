//! 框架级宏工具集。
//!
//! - 仅包含对显式契约的语法辅助，确保不会隐藏全局状态或引入隐式行为；
//! - 当前提供 [`with_ctx!`]，用于在 Pipeline 事件回调中快速绑定运行时语法糖上下文。

/// 在 Pipeline [`Context`](crate::pipeline::Context) 中绑定运行时语法糖上下文。
///
/// # 设计动机（Why）
/// - 在 Handler/Middleware 中，常见模式是先从 `Context` 取出 [`CallContext`](crate::contract::CallContext)
///   与执行器，再构造异步任务。这段样板既冗长又容易遗漏克隆操作。
/// - 宏通过重绑定局部变量，将同名标识符替换为 [`crate::runtime::CallContext`] 视图，从而与
///   [`crate::runtime::spawn_in`] 等语法糖 API 协同。
///
/// # 展开逻辑（How）
/// - 保存对原始 `Context` 的引用，避免移动所有权；
/// - 使用 [`crate::runtime::PipelineContextCaps`] 适配执行器能力；
/// - 构造语法糖上下文并以同名变量绑定，再返回调用方提供的表达式。
///
/// # 契约说明（What）
/// - **前置条件**：第一个参数必须是实现 [`crate::pipeline::Context`] 的标识符；
/// - **后置条件**：宏内部重绑定同名变量，作用域仅限宏展开块；外层仍可使用原始 `Context`；
/// - **返回值**：与第二个参数表达式类型一致，便于与 `async move { ... }` 协作。
///
/// # 风险提示（Trade-offs）
/// - 宏不会自动克隆 [`CallContext`](crate::contract::CallContext)；若异步任务需要持有所有权，请在
///   任务体内调用 [`crate::runtime::CallContext::clone_call`]；
/// - 若传入的标识符未实现 `Context`，编译器将给出常规的 trait 约束错误，方便定位问题。
#[macro_export]
macro_rules! with_ctx {
    ($ctx:ident, $body:expr) => {{
        let __spark_ctx_ref = &$ctx;
        let __spark_caps = $crate::runtime::PipelineContextCaps::new(__spark_ctx_ref);
        let $ctx = $crate::runtime::CallContext::new(__spark_ctx_ref.call_context(), __spark_caps);
        $body
    }};
}
