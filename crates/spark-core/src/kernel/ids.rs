//! 标识符契约，规范跨组件的 ID 生成与校验逻辑。
//!
//! # 设计动机（Why）
//! - 消除“字符串即 ID”造成的隐式耦合，统一通过受约束的新类型表达请求、关联与幂等键；
//! - 为调用方提供标准化的校验与格式化方法，避免在业务层重复实现；
//! - 与 [`crate::types::NonEmptyStr`] 搭配，保证 ID 不会退化为空或纯空白字符串。
//!
//! # 集成方式（How）
//! - 推荐通过 [`crate::prelude`] 一次性引入常见 ID 类型；
//! - 生成 ID 时可复用现有 UUID/雪花算法，只需在落地前调用 `::parse` 进行契约校验。

use crate::{Result, types::NonEmptyStr};
use alloc::sync::Arc;
use core::fmt;

/// 请求标识，贯穿入口 `Service` 与底层传输协议，用于追踪单次调用生命周期。
///
/// # 契约定义（What）
/// - **输入参数**：`value` 必须是非空字符串，建议采用 `<trace_id>:<span_id>` 或 UUID 形式；
/// - **前置条件**：调用方已经确保 ID 唯一性，本类型仅负责结构校验；
/// - **后置条件**：构造成功后，可通过 [`RequestId::as_str`] 以零拷贝方式读取。
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(NonEmptyStr);

impl RequestId {
    /// 从原始字符串解析请求标识。
    pub fn parse(value: impl Into<Arc<str>>) -> Result<Self> {
        Ok(Self(NonEmptyStr::new(value)?))
    }

    /// 返回底层字符串切片。
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 关联标识，串联同一业务流程的多个请求（例如 Saga、补偿任务）。
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CorrelationId(NonEmptyStr);

impl CorrelationId {
    /// 解析关联 ID，通常来源于调用方传播的 `x-correlation-id`。
    pub fn parse(value: impl Into<Arc<str>>) -> Result<Self> {
        Ok(Self(NonEmptyStr::new(value)?))
    }

    /// 读取底层字符串切片。
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 幂等键，确保在网络抖动或重试情况下操作只执行一次。
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IdempotencyKey(NonEmptyStr);

impl IdempotencyKey {
    /// 解析幂等键；若输入为空则返回 `app.invalid_argument` 错误。
    pub fn parse(value: impl Into<Arc<str>>) -> Result<Self> {
        Ok(Self(NonEmptyStr::new(value)?))
    }

    /// 暴露底层字符串，用于落盘或缓存键拼接。
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
