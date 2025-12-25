//! # 会话状态管理（CallSession）
//!
//! ## 核心意图（Why）
//! - 建模 B2BUA 在 A/B leg 上的会话生命周期，将 FreeSWITCH `switch_core_session_t`
//!   的核心能力迁移到 Rust 类型系统中；
//! - 确保信令在初始化、早期响铃、通话激活、终止等阶段的状态跃迁具备可验证约束，
//!   避免多线程环境下出现竞态或资源泄露。
//!
//! ## 架构定位（Where）
//! - 该模块位于 `spark-switch::core`，由 `SessionManager` 统一调度并通过 `SparkHosting`
//!   注入给各 `ProxyService` 实例共享；
//! - 会话内部持有 `BoxService`，与 `spark-core` 的对象层服务契约保持一致。
//!
//! ## 教案式使用指南（How）
//! 1. 调用 [`CallSession::new`] 创建会话并注册到 `SessionManager`；
//! 2. 当 B-leg 建立或释放时，分别调用 [`attach_b_leg`](CallSession::attach_b_leg)
//!    与 [`detach_b_leg`](CallSession::detach_b_leg)；
//! 3. 根据信令事件驱动 [`transition`](CallSession::transition) 更新状态；若违反状态图，将返回
//!    [`SwitchError::InvalidStateTransition`](crate::error::SwitchError::InvalidStateTransition)。
//!
//! ## 状态机约束（What）
//! - 合法跃迁：`Initializing → Early → Active → Terminated`，其中 `Initializing`
//!   可直接跳转至 `Terminated`；`Early` 可跳转至 `Active` 或 `Terminated`；
//!   `Active` 仅允许终止；
//! - 非法跃迁均由状态机校验阻止，保证媒体面与信令面的顺序一致性。

#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(feature = "std")]
use std::{borrow::ToOwned, string::String, sync::Arc};

#[cfg(not(feature = "std"))]
use alloc::{borrow::ToOwned, string::String, sync::Arc};

use spark_core::service::BoxService;

use crate::error::SwitchError;

/// 呼叫腿标识。
///
/// # 教案式说明
/// - **意图 (Why)**：区分 A-leg（主叫侧）与 B-leg（被叫侧），用于错误提示与调度日志；
/// - **契约 (What)**：仅包含两个枚举值，满足 `Copy + Eq + Hash`，便于作为 HashMap Key 或指标标签；
/// - **风险 (Trade-offs)**：若未来扩展更多腿（如转接、会议），需同步更新状态机与错误枚举。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CallLeg {
    /// 主叫侧服务实例。
    A,
    /// 被叫侧服务实例。
    B,
}

/// 呼叫状态。
///
/// # 教案式说明
/// - **意图 (Why)**：覆盖 B2BUA 生命周期关键阶段，指导信令/媒体处理流程；
/// - **契约 (What)**：状态间跃迁受 [`CallState::can_transition_to`] 限制；
/// - **风险 (Trade-offs)**：枚举为 `#[non_exhaustive]` 前不鼓励外部匹配 `_`，避免未来新增状态破坏匹配。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CallState {
    /// 会话创建并初始化 A-leg 的阶段。
    Initializing,
    /// 已向被叫发起 Early Media/180 Ringing 等早期响应。
    Early,
    /// A/B leg 均已就绪，可双向传输媒体与信令。
    Active,
    /// 会话终止，所有资源应已回收。
    Terminated,
}

impl CallState {
    /// 判断状态是否允许跃迁至 `target`。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：在 [`CallSession::transition`] 中复用，保持状态图与实现一致；
    /// - **契约 (What)**：输入 `target` 为目标状态；返回布尔值表示是否允许；
    /// - **执行 (How)**：通过匹配元组 `(self, target)` 实现有限状态机判定。
    pub fn can_transition_to(self, target: CallState) -> bool {
        matches!(
            (self, target),
            (CallState::Initializing, CallState::Initializing)
                | (CallState::Initializing, CallState::Early)
                | (CallState::Initializing, CallState::Active)
                | (CallState::Initializing, CallState::Terminated)
                | (CallState::Early, CallState::Early)
                | (CallState::Early, CallState::Active)
                | (CallState::Early, CallState::Terminated)
                | (CallState::Active, CallState::Active)
                | (CallState::Active, CallState::Terminated)
                | (CallState::Terminated, CallState::Terminated)
        )
    }

    /// 状态是否已终止。
    ///
    /// - **意图 (Why)**：终止态需触发资源回收，例如 B-leg service 的释放；
    /// - **契约 (What)**：返回 `true` 表示无需再接受任何跃迁；
    /// - **风险 (Trade-offs)**：若未来新增“挂起”等中间态，需要扩充逻辑避免误判。
    pub fn is_terminal(self) -> bool {
        matches!(self, CallState::Terminated)
    }
}

/// 呼叫会话结构，封装 A/B leg Service 与状态机。
///
/// # 教案式说明
/// - **意图 (Why)**：集中维护会话上下文，提供状态校验与腿管理，避免散落在各 Service 中；
/// - **契约 (What)**：
///   - `call_id`：使用 `Arc<str>` 共享标识，保证跨线程读取零拷贝；
///   - `a_leg`：主叫侧对象层 Service，创建会话时即固定；
///   - `b_leg`：可选的被叫侧 Service，建立 B-leg 后填充；
///   - `state`：遵循 [`CallState`] 状态机；
/// - **风险 (Trade-offs)**：当前实现未存储额外媒体上下文，后续扩展需注意线程安全。
#[derive(Debug)]
pub struct CallSession {
    call_id: Arc<str>,
    state: CallState,
    a_leg: BoxService,
    b_leg: Option<BoxService>,
    media: MediaContext,
}

impl CallSession {
    /// 构造新的会话实例。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：统一初始化状态为 [`CallState::Initializing`]，并持有 A-leg service；
    /// - **契约 (What)**：
    ///   - `call_id`：可转换为 `String` 的会话标识；
    ///   - `a_leg`：已经装配好的主叫 Service，错误类型需为 [`SparkError`](spark_core::SparkError)；
    ///   - **前置条件**：调用者已完成 A-leg service 的构造，并确保其满足线程安全要求；
    ///   - **后置条件**：会话状态为 `Initializing`，B-leg 为空。
    /// - **风险 (Trade-offs)**：当前不会立即注册到 `SessionManager`，需由上层显式调用管理器。
    pub fn new(call_id: impl Into<String>, a_leg: BoxService) -> Self {
        Self {
            call_id: Arc::<str>::from(call_id.into()),
            state: CallState::Initializing,
            a_leg,
            b_leg: None,
            media: MediaContext::default(),
        }
    }

    /// 获取 Call-ID 字符串视图。
    ///
    /// - **意图 (Why)**：提供轻量访问，避免频繁克隆 `Arc`；
    /// - **契约 (What)**：返回值生命周期绑定 `self`，仅供只读使用；
    /// - **风险 (Trade-offs)**：若需要长久持有请使用 [`call_id_arc`](Self::call_id_arc)。
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// 获取共享引用形式的 Call-ID。
    ///
    /// - **意图 (Why)**：配合 `DashMap` 等并发结构使用，避免重复分配；
    /// - **契约 (What)**：返回 `&Arc<str>`；调用方可 `clone` 以在其他线程复用。
    pub fn call_id_arc(&self) -> &Arc<str> {
        &self.call_id
    }

    /// 当前状态。
    pub fn state(&self) -> CallState {
        self.state
    }

    /// 访问 A-leg 服务。
    ///
    /// - **契约 (What)**：返回不可变引用；如需所有权请在上层克隆 `BoxService`。
    pub fn a_leg(&self) -> &BoxService {
        &self.a_leg
    }

    /// 访问 B-leg 服务（只读）。
    pub fn b_leg(&self) -> Option<&BoxService> {
        self.b_leg.as_ref()
    }

    /// 访问 B-leg 服务（可变）。
    pub fn b_leg_mut(&mut self) -> Option<&mut BoxService> {
        self.b_leg.as_mut()
    }

    /// 记录主叫侧提供的 SDP Offer 文本。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：为媒体协商提供源数据，避免 `run_call_flow` 再次访问原始 INVITE；
    /// - **契约 (What)**：
    ///   - `sdp`：完整的 SDP 文本，调用前需确保 UTF-8 与字段完整；
    ///   - **前置条件**：`CallSession` 已代表一个有效 INVITE，重复调用会覆盖旧值；
    ///   - **后置条件**：内部 `media.offer_sdp` 被替换，可供只读访问。
    pub fn set_offer_sdp(&mut self, sdp: String) {
        self.media.offer_sdp = Some(sdp);
    }

    /// 以不可变视图形式读取 SDP Offer。
    ///
    /// - **意图 (Why)**：让状态机与测试在不复制的情况下检查 Offer 文本；
    /// - **契约 (What)**：若尚未写入则返回 `None`，否则返回 `&str` 引用；
    /// - **风险 (Trade-offs)**：返回值绑定 `self` 生命周期，调用者若需长期持有需自行克隆。
    pub fn offer_sdp(&self) -> Option<&str> {
        self.media.offer_sdp.as_deref()
    }

    /// 记录经过本地能力裁剪后准备发送给 B-leg 的 SDP Offer。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：将 `run_call_flow` 根据协商结果裁剪的 SDP 缓存下来，
    ///   便于未来真正向 B-leg 发送 INVITE 时直接复用，避免重复解析/重写；
    /// - **契约 (What)**：`sdp` 需为完整的 SDP 文本；该接口会覆盖旧值；
    /// - **前置条件**：调用前已经完成对 A-leg Offer 的解析并生成裁剪计划；
    /// - **后置条件**：`media.b_leg_offer_sdp` 更新，可通过 [`b_leg_offer_sdp`](Self::b_leg_offer_sdp)
    ///   只读访问。
    pub fn set_b_leg_offer_sdp(&mut self, sdp: String) {
        self.media.b_leg_offer_sdp = Some(sdp);
    }

    /// 读取准备发往 B-leg 的 SDP Offer。
    pub fn b_leg_offer_sdp(&self) -> Option<&str> {
        self.media.b_leg_offer_sdp.as_deref()
    }

    /// 记录 B-leg 反馈的 SDP Answer。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：在向 A-leg 返回 200 OK 前缓存 Answer，保持媒体视图一致；
    /// - **契约 (What)**：调用者需提供 UTF-8 字符串；重复调用会覆盖上一版本；
    /// - **后置条件**：`media.b_leg_answer_sdp` 更新，可被 `b_leg_answer_sdp` 读取。
    pub fn set_b_leg_answer_sdp(&mut self, sdp: String) {
        self.media.b_leg_answer_sdp = Some(sdp);
    }

    /// 读取 B-leg 的 SDP Answer（若已存在）。
    pub fn b_leg_answer_sdp(&self) -> Option<&str> {
        self.media.b_leg_answer_sdp.as_deref()
    }

    /// 更新当前的音频协商状态。
    ///
    /// - **意图 (Why)**：`run_call_flow` 在完成 SDP 解析后需要持久化协商结果；
    /// - **契约 (What)**：`state` 描述最新的协商结论，允许覆盖；
    /// - **前置条件**：调用者必须持有 `&mut self`，以确保在 `DashMap` guard 下安全写入。
    pub fn set_audio_negotiation(&mut self, state: AudioNegotiationState) {
        self.media.audio = state;
    }

    /// 获取音频协商状态的只读视图。
    pub fn audio_negotiation(&self) -> &AudioNegotiationState {
        &self.media.audio
    }

    /// 挂载 B-leg 服务。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：建立被叫侧信令管线，形成完整的 B2BUA；
    /// - **契约 (What)**：
    ///   - `service`：对象层 Service；
    ///   - **前置条件**：当前 `b_leg` 为 `None`；
    ///   - **后置条件**：`b_leg` 填充新服务；若失败，返回 [`SwitchError::LegAlreadyBound`]。
    pub fn attach_b_leg(&mut self, service: BoxService) -> Result<(), SwitchError> {
        if self.b_leg.is_some() {
            return Err(SwitchError::LegAlreadyBound {
                call_id: self.call_id().to_owned(),
                leg: CallLeg::B,
            });
        }

        self.b_leg = Some(service);
        Ok(())
    }

    /// 卸载并返回 B-leg 服务。
    ///
    /// - **意图 (Why)**：在终止或重建 B-leg 时释放旧的 Service，交由调用方处理资源回收；
    /// - **契约 (What)**：无前置条件，返回 `Option<BoxService>`；
    /// - **后置条件**：内部 `b_leg` 置为 `None`。
    pub fn detach_b_leg(&mut self) -> Option<BoxService> {
        self.b_leg.take()
    }

    /// 状态机跃迁。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：驱动会话生命周期，保障信令顺序；
    /// - **契约 (What)**：
    ///   - `next`：目标状态；
    ///   - **前置条件**：调用方持有可变引用，确保无并发写；
    ///   - **后置条件**：若成功，`state` 更新为 `next`；当进入终止态时自动释放 B-leg；
    ///   - **错误**：非法跃迁返回 [`SwitchError::InvalidStateTransition`]。
    /// - **执行 (How)**：先使用 [`CallState::can_transition_to`] 校验，再更新状态并处理终止钩子。
    pub fn transition(&mut self, next: CallState) -> Result<(), SwitchError> {
        if self.state == next {
            return Ok(());
        }

        if !self.state.can_transition_to(next) {
            return Err(SwitchError::InvalidStateTransition {
                call_id: self.call_id().to_owned(),
                from: self.state,
                to: next,
            });
        }

        self.state = next;

        if self.state.is_terminal() {
            self.b_leg = None;
        }

        Ok(())
    }
}

/// 媒体上下文，缓存 SDP 文本与协商结论。
#[derive(Clone, Debug, Default)]
struct MediaContext {
    offer_sdp: Option<String>,
    b_leg_offer_sdp: Option<String>,
    b_leg_answer_sdp: Option<String>,
    audio: AudioNegotiationState,
}

/// 音频协商的离散状态。
///
/// # 教案式说明
/// - **意图 (Why)**：让会话状态机能够感知媒体准备程度，以便决定何时向 A/B leg 回送响应；
/// - **契约 (What)**：
///   - `Pending`：尚未执行协商；
///   - `NoAudio`：Offer 中缺少 `m=audio`；
///   - `Accepted`：包含完整的编解码参数；
///   - `Rejected`：Offer 中存在音频但本地能力无法满足；
/// - **风险 (Trade-offs)**：若未来支持多流，需要扩展为向量形式。
#[derive(Clone, Debug, Default)]
pub enum AudioNegotiationState {
    #[default]
    Pending,
    NoAudio,
    Accepted(AudioNegotiationProfile),
    Rejected,
}

/// 音频协商成功后的详细描述。
///
/// # 教案式注释
/// - **意图 (Why)**：集中描述被接受的负载编号、编解码名称及 DTMF 配置，方便响应构造；
/// - **契约 (What)**：所有字段均为拥有所有权的 `String` 或原始数值，调用者可安全跨线程传递；
/// - **后置条件**：一旦写入 `CallSession`，即可供其它组件直接复用。
#[derive(Clone, Debug)]
pub struct AudioNegotiationProfile {
    pub payload_type: u8,
    pub encoding_name: String,
    pub clock_rate: u32,
    pub encoding_params: Option<String>,
    pub telephone_event: Option<DtmfProfile>,
}

/// RFC 4733 DTMF 能力描述。
#[derive(Clone, Debug)]
pub struct DtmfProfile {
    pub payload_type: u8,
    pub clock_rate: u32,
    pub events: String,
}
