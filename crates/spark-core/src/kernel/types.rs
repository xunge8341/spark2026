//! 类型契约集中营，供上层依赖直接复用，避免各模块自行约定导致语义漂移。
//!
//! # 设计总览（Why）
//! - 将“预算、非空字符串”等高频契约统一收敛，明确 `spark-core` 为唯一权威来源；
//! - 通过富含上下文的文档注释，帮助新接入团队在不阅读实现细节的情况下理解语义；
//! - 预留 `no_std + alloc` 兼容性，确保在资源受限环境中同样可用。
//!
//! # 集成说明（How）
//! - 推荐通过 [`crate::prelude`] 导入这些类型，或在模块内显式 `use crate::types::{..}`；
//! - 若需要新增基础契约，必须在本模块完成定义并更新 `docs/ARCH-LAYERS.md` 的引入矩阵。

use crate::{CoreError, Result, error::codes};
use alloc::{borrow::Cow, sync::Arc, vec::Vec};
use core::fmt;

#[cfg(not(any(loom, spark_loom)))]
use core::sync::atomic::{AtomicU64, Ordering};
#[cfg(any(loom, spark_loom))]
use loom::sync::atomic::{AtomicU64, Ordering};

/// 非空字符串封装，约束配置、标识符等不被空白值污染。
///
/// # 设计背景（Why）
/// - 评审阶段发现大量“空字符串代表未配置”或“`"   "` 被当作有效标识”的隐性假设；
///   将此约束前移，有助于在构建期暴露错误。
/// - 统一提供 `Arc<str>` 语义，避免热点路径因多次克隆 `String` 产生额外复制。
///
/// # 契约说明（What）
/// - **输入参数**：调用 [`NonEmptyStr::new`] 时接受任意实现 `Into<Arc<str>>` 的值，
///   会在内部执行裁剪检查；若结果为空，返回 [`CoreError`]。
/// - **前置条件**：调用方需确保原始值已完成格式校验（大小写、命名空间等），本类型只负责非空约束。
/// - **后置条件**：实例可安全克隆，克隆成本为一次 `Arc` 增强引用计数。
///
/// # 逻辑解析（How）
/// 1. 将输入转换为 `Arc<str>`，兼容静态字面量与堆字符串；
/// 2. 通过 `trim()` 去除前后空白，判断是否为空；
/// 3. 若为空则返回 `app.invalid_argument` 错误码的 [`CoreError`]；
/// 4. 否则缓存原值，供调用方以零拷贝方式读取。
///
/// # 风险提示（Trade-offs）
/// - 当前实现不会自动进行规范化（如小写化），避免引入不可逆变换；
/// - 若需要“保留原值但禁止全空白”，请在调用方配合存储原始字符串。
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NonEmptyStr(Arc<str>);

impl NonEmptyStr {
    /// 构造受非空约束保护的字符串。
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self> {
        let arc: Arc<str> = value.into();
        if arc.trim().is_empty() {
            return Err(CoreError::new(
                codes::APP_INVALID_ARGUMENT,
                "NonEmptyStr 要求输入不能为空或仅包含空白字符",
            ));
        }
        Ok(Self(arc))
    }

    /// 以 `&str` 视图访问底层数据，供日志或序列化使用。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NonEmptyStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// 预算种类，覆盖协议解码、数据流量以及自定义资源限额。
///
/// # 设计背景（Why）
/// - 统一的预算标识便于在服务、编解码、传输层之间共享资源限额，例如统一的“解码字节数”或“并发请求”预算。
///
/// # 契约说明（What）
/// - 框架预置 `Decode` 与 `Flow` 两种常见预算；`Custom` 可用于扩展其他资源类型（例如 CPU、数据库连接数）。
/// - 自定义标识建议使用 `namespace.key` 形式，便于在日志与指标中区分来源。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BudgetKind {
    /// 协议解码预算（单位自定义，如字节、消息数）。
    Decode,
    /// 数据流量预算（单位自定义，如包数、窗口大小）。
    Flow,
    /// 自定义预算，通过稳定命名区分。
    Custom(Arc<str>),
}

impl BudgetKind {
    /// 构造自定义预算标识。
    pub fn custom(name: impl Into<Arc<str>>) -> Self {
        BudgetKind::Custom(name.into())
    }

    /// 返回面向可观测性的稳定标签值。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：指标与日志在记录资源耗尽场景时需要统一的 `error.budget.kind` 标签；
    /// - **契约（What）**：返回 `Cow<'static, str>`，其中内建预算使用借用值，自定义预算克隆为拥有所有权的字符串；
    /// - **风险提示**：对自定义预算会触发一次堆分配，调用方应尽量复用自定义命名或缓存结果。
    pub fn observability_label(&self) -> Cow<'static, str> {
        match self {
            BudgetKind::Decode => Cow::Borrowed("decode"),
            BudgetKind::Flow => Cow::Borrowed("flow"),
            BudgetKind::Custom(name) => Cow::Owned(name.as_ref().to_string()),
        }
    }
}

/// 预算快照，用于在日志与可观测性中输出剩余额度。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BudgetSnapshot {
    kind: BudgetKind,
    remaining: u64,
    limit: u64,
}

impl BudgetSnapshot {
    /// 创建快照。
    pub fn new(kind: BudgetKind, remaining: u64, limit: u64) -> Self {
        Self {
            kind,
            remaining,
            limit,
        }
    }

    /// 获取预算类型。
    pub fn kind(&self) -> &BudgetKind {
        &self.kind
    }

    /// 查询剩余额度。
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// 查询预算上限。
    pub fn limit(&self) -> u64 {
        self.limit
    }
}

/// 预算消费决策，用于背压枚举与 Service::poll_ready 返回值。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BudgetDecision {
    /// 预算充足，允许继续执行。
    Granted { snapshot: BudgetSnapshot },
    /// 预算已耗尽，需要上层施加背压或降级。
    Exhausted { snapshot: BudgetSnapshot },
}

impl BudgetDecision {
    /// 是否代表预算仍可用。
    pub fn is_granted(&self) -> bool {
        matches!(self, BudgetDecision::Granted { .. })
    }

    /// 快速获取预算快照。
    pub fn snapshot(&self) -> &BudgetSnapshot {
        match self {
            BudgetDecision::Granted { snapshot } | BudgetDecision::Exhausted { snapshot } => {
                snapshot
            }
        }
    }
}

/// 预算控制器，负责跨线程共享剩余额度。
///
/// # 设计背景（Why）
/// - 预算控制需要在 Service、编解码、传输等不同层级共享，因此使用 [`Arc`] + [`AtomicU64`] 保证多线程安全。
/// - 通过 `try_consume` 与 `refund` 实现幂等的租借/归还语义，便于在出错时回滚。
///
/// # 契约说明（What）
/// - `limit` 表达预算上限，`remaining` 初始等于上限。
/// - `try_consume` 会在不足时返回 `BudgetDecision::Exhausted`，并保持剩余额度不变。
/// - `refund` 在安全回滚时归还额度，结果向上取整至上限范围内。
///
/// # 风险提示（Trade-offs）
/// - 当前实现采用乐观自旋更新，适合中等竞争场景；若需严格公平或分布式预算，可在实现层封装更复杂的协调算法。
#[derive(Clone, Debug)]
pub struct Budget {
    kind: BudgetKind,
    remaining: Arc<AtomicU64>,
    limit: u64,
}

impl Budget {
    /// 使用给定上限创建预算。
    pub fn new(kind: BudgetKind, limit: u64) -> Self {
        Self {
            kind,
            remaining: Arc::new(AtomicU64::new(limit)),
            limit,
        }
    }

    /// 创建无限预算，表示不受限的资源池。
    pub fn unbounded(kind: BudgetKind) -> Self {
        Self {
            kind,
            remaining: Arc::new(AtomicU64::new(u64::MAX)),
            limit: u64::MAX,
        }
    }

    /// 返回预算类型。
    pub fn kind(&self) -> &BudgetKind {
        &self.kind
    }

    /// 查询剩余额度。
    pub fn remaining(&self) -> u64 {
        self.remaining.load(Ordering::Acquire)
    }

    /// 获取预算上限。
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// 尝试消费指定额度。
    pub fn try_consume(&self, amount: u64) -> BudgetDecision {
        let mut current = self.remaining.load(Ordering::Acquire);
        loop {
            if current < amount {
                return BudgetDecision::Exhausted {
                    snapshot: BudgetSnapshot::new(self.kind.clone(), current, self.limit),
                };
            }
            match self.remaining.compare_exchange(
                current,
                current - amount,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return BudgetDecision::Granted {
                        snapshot: BudgetSnapshot::new(
                            self.kind.clone(),
                            current - amount,
                            self.limit,
                        ),
                    };
                }
                Err(actual) => {
                    current = actual;
                }
            }
        }
    }

    /// 归还额度，常用于异常回滚。
    pub fn refund(&self, amount: u64) {
        let mut current = self.remaining.load(Ordering::Acquire);
        loop {
            let new_value = current.saturating_add(amount).min(self.limit);
            match self.remaining.compare_exchange(
                current,
                new_value,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// 生成只读快照，用于日志或指标。
    pub fn snapshot(&self) -> BudgetSnapshot {
        BudgetSnapshot::new(self.kind.clone(), self.remaining(), self.limit)
    }
}

/// 标准化的预算集合容器，便于在配置解析或测试中批量创建预算。
#[derive(Clone, Debug, Default)]
pub struct BudgetSet(Vec<Budget>);

impl BudgetSet {
    /// 添加预算并返回自身，便于链式构建。
    pub fn push(mut self, budget: Budget) -> Self {
        self.0.push(budget);
        self
    }

    /// 以切片形式暴露内部预算集合。
    pub fn as_slice(&self) -> &[Budget] {
        &self.0
    }
}

/// 面向协议对象的统一关闭描述，配合 [`crate::protocol::Event`] 传播。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseReason {
    code: Cow<'static, str>,
    message: Cow<'static, str>,
}

impl CloseReason {
    /// 构造关闭原因。
    pub fn new(code: impl Into<Cow<'static, str>>, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    /// 获取关闭码。
    pub fn code(&self) -> &str {
        &self.code
    }

    /// 获取描述。
    pub fn message(&self) -> &str {
        &self.message
    }
}
