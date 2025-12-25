//! 协议层契约定义：消息、帧与事件的稳定语义均在此发布。
//!
//! # 设计目标（Why）
//! - 让传输实现与服务层在讨论协议交互时共享统一语言；
//! - 避免在不同传输 Crate 中复制枚举或结构体，降低升级成本；
//! - 与 [`crate::types`]、[`crate::ids`] 中的基础类型组合，形成从请求到帧的闭环表达。
//!
//! # 使用方式（How）
//! - 构造 `Message` 后可以拆分为一系列 `Frame` 下发到传输层；
//! - 接入侧在解码完成后将帧聚合为 `Event::Message` 或 `Event::Close` 交给上层处理。

use crate::{
    CoreError, Result,
    error::codes,
    ids::RequestId,
    types::{CloseReason, NonEmptyStr},
};
use alloc::{sync::Arc, vec::Vec};
use core::fmt;

/// 协议事件，承载跨层传播的高阶语义。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event<Body> {
    /// 收到新的业务消息，需要继续解码或分发。
    Message(Message<Body>),
    /// 收到链路确认（ack），便于上层更新重放窗口。
    Ack { request: RequestId, last_frame: u32 },
    /// 链路被优雅关闭。
    Close { reason: CloseReason },
}

/// 高阶消息单元，代表一次完整请求或响应。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message<Body> {
    id: RequestId,
    name: Option<NonEmptyStr>,
    metadata: Vec<(NonEmptyStr, NonEmptyStr)>,
    body: Body,
}

impl<Body> Message<Body> {
    /// 构造消息，并对可选名称、元数据执行非空校验。
    ///
    /// # 背景（Why）
    /// - 协议讨论中常需要携带逻辑名称与键值元数据（例如路由键、内容类型）；
    ///   若缺乏统一校验，易出现空字段导致后续阶段 panic。
    ///
    /// # 契约与前置条件（What）
    /// - `id`：来自调用上下文的请求 ID，必须保持全局唯一；
    /// - `name`：可选的人类可读名称，若提供则需满足 [`NonEmptyStr`] 约束；
    /// - `metadata`：将被标准化为非空键值对；
    /// - `body`：业务自定义负载，实现层负责序列化。
    ///
    /// # 执行逻辑（How）
    /// 1. 对 `name` 调用 [`NonEmptyStr::new`]；
    /// 2. 遍历 `metadata`，逐项校验键和值；
    /// 3. 将校验后的数据组装为不可变结构，准备交给上层使用。
    ///
    /// # 风险与权衡（Trade-offs）
    /// - 目前不对 `metadata` 重复键做特殊处理，交由调用方在更高层合并；
    /// - 元数据存储在 `Vec` 中，保证迭代顺序与输入一致，满足审计重放需求。
    pub fn try_new(
        id: RequestId,
        name: Option<Arc<str>>,
        metadata: Vec<(Arc<str>, Arc<str>)>,
        body: Body,
    ) -> Result<Self> {
        let name = match name {
            Some(value) => Some(NonEmptyStr::new(value)?),
            None => None,
        };
        let mut normalized = Vec::with_capacity(metadata.len());
        for (key, value) in metadata {
            normalized.push((NonEmptyStr::new(key)?, NonEmptyStr::new(value)?));
        }
        Ok(Self {
            id,
            name,
            metadata: normalized,
            body,
        })
    }

    /// 消息对应的请求 ID。
    pub fn id(&self) -> &RequestId {
        &self.id
    }

    /// 可选名称，常用于路由或指标标识。
    pub fn name(&self) -> Option<&NonEmptyStr> {
        self.name.as_ref()
    }

    /// 键值元数据集合，遵循非空约束。
    pub fn metadata(&self) -> &[(NonEmptyStr, NonEmptyStr)] {
        &self.metadata
    }

    /// 获取消息体所有权。
    pub fn into_body(self) -> Body {
        self.body
    }
}

/// 单个协议帧，传输层以其为最小调度单位。
///
/// # 契约维度速览
/// - **语义**：`Frame` 将请求 ID、递增序号与 `fin` 终止位绑定，确保消息边界与乱序恢复可被精确推断。
/// - **错误**：`try_new` 在序号耗尽时返回 `CoreError`（`protocol.budget_exceeded`），调用方应中止流式发送。
/// - **并发**：结构体本身 `Send + Sync`（若 `Payload` 满足），适合作为多生产者/消费者队列中的共享消息。
/// - **背压**：帧级背压由上游 `BackpressureSignal` 驱动；当上层指示 `Busy/RetryAfter` 时，应暂停创建新的 `Frame`。
/// - **超时**：构造函数不直接处理超时，需由调用方结合 `CallContext::deadline()` 或传输层调度器决定发送窗口。
/// - **取消**：若收到取消信号，应停止进一步分片并丢弃尚未发送的帧，防止对端收到残缺序列。
/// - **观测标签**：建议在遥测中使用 `frame.request`, `frame.seq`, `frame.fin` 记录关键维度，辅助审计乱序问题。
/// - **示例(伪码)**：
///   ```text
///   seq = 0
///   for chunk in message.chunks():
///       frame = Frame::try_new(req_id, seq, chunk.is_last(), chunk)
///       transport.send(frame)
///       seq += 1
///   ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame<Payload> {
    request: RequestId,
    sequence: u32,
    fin: bool,
    payload: Payload,
}

impl<Payload> Frame<Payload> {
    /// 构造帧；序号必须从 0 开始递增。
    ///
    /// - **语义**：保证 `(request, sequence)` 组合唯一映射消息片段，`fin` 显式声明消息结束。
    /// - **错误**：当 `sequence == u32::MAX` 时返回 `protocol.budget_exceeded`，提示上层终止分片流程。
    /// - **并发**：函数为纯计算，可在多线程上下文中安全调用；需由调用方保证序号生成的原子性。
    /// - **背压**：若上游检测到 `ReadyState::Busy`，应暂停调用该函数；函数本身不感知背压信号。
    /// - **超时**：若在发送前超时，请勿继续构造后续帧，避免对端收到过期数据。
    /// - **取消**：检测到取消时立即停止调用，并回收尚未发送的 `payload`。
    /// - **观测标签**：建议在创建成功后记录 `frame.sequence`、`frame.fin`、`frame.size`（`payload` 字节数）。
    /// - **伪码**：`Frame::try_new(ctx.request_id(), seq.fetch_add(1), is_last, chunk)`。
    pub fn try_new(request: RequestId, sequence: u32, fin: bool, payload: Payload) -> Result<Self> {
        if sequence == u32::MAX {
            return Err(CoreError::new(
                codes::PROTOCOL_BUDGET_EXCEEDED,
                "帧序号达到上限，可能存在无限流重放",
            ));
        }
        Ok(Self {
            request,
            sequence,
            fin,
            payload,
        })
    }

    /// 对应的请求标识。
    pub fn request(&self) -> &RequestId {
        &self.request
    }

    /// 帧序号，从 0 开始递增。
    pub fn sequence(&self) -> u32 {
        self.sequence
    }

    /// 是否为消息结束帧。
    pub fn is_fin(&self) -> bool {
        self.fin
    }

    /// 访问帧载荷。
    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    /// 拆出载荷所有权，便于聚合器重组消息体。
    pub fn into_payload(self) -> Payload {
        self.payload
    }
}

impl<Payload> fmt::Display for Frame<Payload> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Frame#{}(fin={})", self.sequence, self.fin)
    }
}
