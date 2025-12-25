//! 通用状态模型：统一表达框架级状态机的阶段与健康度。
//!
//! # 设计目标（Why）
//! - 在路由、运行时与 Pipeline 等子系统中共享同一种状态语义，便于在文档与契约测试中复用；
//! - 提供最小可用的 `State`、`Status` 抽象，鼓励实现层通过组合而非重新定义枚举；
//! - 让外部调用方可以基于 `spark-core` 的状态契约构建运维面板或回放工具。

use crate::{CoreError, types::CloseReason};
use alloc::sync::Arc;
use core::fmt;

/// 带错误上下文的有限状态机节点。
///
/// # 设计取舍（Trade-offs）
/// - 使用 [`Arc<CoreError>`] 存储失败原因，允许状态在多线程间安全克隆；
/// - 未实现 `PartialEq/Eq`，避免比较错误实例时产生误导（`CoreError` 不具备值语义）。
#[derive(Clone, Debug)]
pub enum State<T> {
    /// 初始态，尚未加载资源。
    Init,
    /// 激活态，携带可操作的内部数据。
    Active(T),
    /// 已完成态，返回最终结果。
    Completed(T),
    /// 失败态，记录导致状态机终止的核心错误。
    Failed(Arc<CoreError>),
}

impl<T> State<T> {
    /// 是否处于完成态。
    pub fn is_completed(&self) -> bool {
        matches!(self, State::Completed(_))
    }

    /// 提取活跃态的内部数据引用，供观察者使用。
    pub fn as_active(&self) -> Option<&T> {
        match self {
            State::Active(value) => Some(value),
            _ => None,
        }
    }

    /// 查询失败原因，供运维和诊断使用。
    pub fn error(&self) -> Option<&CoreError> {
        match self {
            State::Failed(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

/// 运维视角的健康状态，兼容 Ready/Busy/Closed 等语义。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    /// 正常工作，可接受新流量。
    Ready,
    /// 忙碌状态，可附带原因字符串，便于提示运维操作。
    Busy(&'static str),
    /// 已终止，附带优雅关闭原因。
    Closed(CloseReason),
}

impl Status {
    /// 创建忙碌态，并附带人类可读原因。
    pub fn busy(reason: &'static str) -> Self {
        Status::Busy(reason)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Ready => f.write_str("ready"),
            Status::Busy(reason) => write!(f, "busy: {}", reason),
            Status::Closed(reason) => write!(f, "closed: {}", reason.message()),
        }
    }
}
