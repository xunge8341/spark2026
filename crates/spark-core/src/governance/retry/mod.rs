//! 自适应重试策略命名空间。
//!
//! # 教案式说明
//! - **定位（Where）**：位于 `spark_core::retry`，承载所有围绕 `RetryAdvice` 调节逻辑的实现，
//!   供错误分类与节律追踪模块复用。
//! - **目标（Why）**：在 `ErrorCategory::Retryable` 触发时，根据运行时拥塞信号生成更精准的
//!   `RetryAfter` 建议，减少雪崩式重试带来的二次拥塞。
//! - **结构（What/How）**：目前仅包含 [`adaptive`] 子模块，后续若扩展多种策略（如指数退避、
//!   速率限制器），请在此集中注册以维持命名一致性。
//! - **维护提示（Trade-offs）**：模块化拆分可保持算法实验的可控性，但也意味着新增策略需
//!   同步更新文档（`docs/retry-policy.md`）与测试案例（`tests::retry`）。
pub mod adaptive;
