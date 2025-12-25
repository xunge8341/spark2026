//! 配置契约集合，统一描述调用超时与运行时策略的抽象模型。
//!
//! # 背景说明（Why）
//! - P1 契约要求“核心超时策略由 `spark-core` 定义且保持稳定”，避免各实现自创字段；
//! - 将常见配置（超时、回退策略）收敛于本模块，搭配 [`crate::types::NonEmptyStr`]、[`crate::ids::RequestId`] 等基础类型使用；
//! - 为 `no_std + alloc` 场景保留最小依赖，仅使用 [`core::time::Duration`] 与 [`alloc`] 容器。
//!
//! # 集成方式（How）
//! - 使用 [`Timeout::try_new`] 构造软、硬超时组合；
//! - 若业务存在多套超时策略，可通过 [`TimeoutProfile`] 表示并在配置中心下发。

use crate::{CoreError, Result, error::codes, types::NonEmptyStr};
use alloc::{sync::Arc, vec::Vec};
use core::time::Duration;

/// 统一的超时契约，同时承载软超时（提醒）与硬超时（强制取消）。
///
/// # 契约定义（What）
/// - `soft`：建议调用方在到期时发出预警或降级；
/// - `hard`：超出后必须中止操作，并触发 [`crate::contract::Cancellation::cancel`]；
/// - 若仅提供硬超时，软超时将与硬超时相同。
///
/// # 逻辑解析（How）
/// 1. 校验 `soft`、`hard` 均大于零；
/// 2. 若提供硬超时，必须 `hard >= soft`；
/// 3. 将参数封装为不可变结构，供运行时与服务层共用。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timeout {
    soft: Duration,
    hard: Duration,
    has_hard: bool,
}

impl Timeout {
    /// 构造超时契约；若缺少硬超时则与软超时一致。
    pub fn try_new(soft: Duration, hard: Option<Duration>) -> Result<Self> {
        if soft.is_zero() {
            return Err(CoreError::new(
                codes::APP_INVALID_ARGUMENT,
                "软超时必须大于 0",
            ));
        }
        let (hard_value, has_hard) = match hard {
            Some(value) => {
                if value < soft {
                    return Err(CoreError::new(
                        codes::APP_INVALID_ARGUMENT,
                        "硬超时必须不小于软超时",
                    ));
                }
                if value.is_zero() {
                    return Err(CoreError::new(
                        codes::APP_INVALID_ARGUMENT,
                        "硬超时必须大于 0",
                    ));
                }
                (value, true)
            }
            None => (soft, false),
        };
        Ok(Self {
            soft,
            hard: hard_value,
            has_hard,
        })
    }

    /// 读取软超时阈值。
    pub fn soft(&self) -> Duration {
        self.soft
    }

    /// 读取硬超时阈值。
    pub fn hard(&self) -> Duration {
        self.hard
    }

    /// 是否显式配置了硬超时。
    pub fn has_hard(&self) -> bool {
        self.has_hard
    }
}

/// 超时策略档案，绑定名称与多组超时配置，便于在运行时动态切换。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutProfile {
    name: NonEmptyStr,
    stages: Vec<Timeout>,
}

impl TimeoutProfile {
    /// 使用可读名称与阶段列表创建档案。
    pub fn try_new(name: impl Into<Arc<str>>, stages: Vec<Timeout>) -> Result<Self> {
        if stages.is_empty() {
            return Err(CoreError::new(
                codes::APP_INVALID_ARGUMENT,
                "TimeoutProfile 至少需要包含一个阶段",
            ));
        }
        Ok(Self {
            name: NonEmptyStr::new(name)?,
            stages,
        })
    }

    /// 档案名称，建议与配置中心条目保持一致。
    pub fn name(&self) -> &NonEmptyStr {
        &self.name
    }

    /// 遍历超时阶段，从短到长依次排列。
    pub fn stages(&self) -> &[Timeout] {
        &self.stages
    }
}
