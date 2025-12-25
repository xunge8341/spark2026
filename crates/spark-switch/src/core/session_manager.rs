//! # SessionManager：会话仓储调度器
//!
//! ## 核心意图（Why）
//! - 提供基于 `DashMap` 的并发安全存储，确保多线程环境下对 `CallSession` 的创建、查询、回收具备原子语义；
//! - 作为 spark-hosting 注入的共享资源，被多个 `ProxyService` 同时访问，需保证无锁热点下的可扩展性。
//!
//! ## 架构定位（Where）
//! - 隶属 `spark-switch::core`，与 CallSession 协同工作；
//! - 在 `std` 特性下启用，以利用 `DashMap` 的线程安全能力；`alloc` 构建不会包含该模块。
//!
//! ## 行为契约（What）
//! - `create_session`：若 Call-ID 已存在，返回 [`SwitchError::SessionAlreadyExists`](crate::error::SwitchError::SessionAlreadyExists)；
//! - `get_session`：返回 `DashMap` guard，用于读取；
//! - `remove_session`：原子移除并返回 `CallSession`；
//! - 所有操作均以 Call-ID (`Arc<str>`) 为索引，保证内存共享与字符串零拷贝。
//!
//! ## 风险提示（Trade-offs）
//! - `DashMap` guard 在持有期间会阻塞同分片的写操作，调用者应缩短持有时间；
//! - 未提供自动清理策略，长时间未释放的会话需由上层定期扫描。

use std::sync::Arc;

use dashmap::{
    DashMap,
    mapref::{
        entry::Entry,
        one::{Ref, RefMut},
    },
};

use crate::{core::session::CallSession, error::SwitchError};

/// `SessionManager` 负责集中管理 `CallSession`。
///
/// # 教案式注释
/// - **意图 (Why)**：封装并发安全存储，避免上层直接操作 `DashMap`；
/// - **契约 (What)**：内部以 `Arc<str>` 作为 Key，保证 Call-ID 复用；
/// - **风险 (Trade-offs)**：当前未内建指标统计，后续可结合 `DashMap::shards` 暴露监控。
#[derive(Debug, Default)]
pub struct SessionManager {
    sessions: DashMap<Arc<str>, CallSession>,
}

impl SessionManager {
    /// 创建空的会话管理器。
    pub fn new() -> Self {
        Self::default()
    }

    /// 将新的会话注册到仓储中。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：原子地插入新会话，阻止重复的 Call-ID 覆盖；
    /// - **契约 (What)**：
    ///   - `session`：待插入的会话，调用前必须由上层完成 `CallSession::new`；
    ///   - **前置条件**：同一 Call-ID 不应已存在；若存在则返回 `SessionAlreadyExists`；
    ///   - **后置条件**：成功时 `DashMap` 中出现新条目，返回 `Ok(())`。
    pub fn create_session(&self, session: CallSession) -> Result<(), SwitchError> {
        let call_id = session.call_id_arc().clone();

        match self.sessions.entry(call_id.clone()) {
            Entry::Occupied(_) => Err(SwitchError::SessionAlreadyExists {
                call_id: call_id.as_ref().to_owned(),
            }),
            Entry::Vacant(vacant) => {
                vacant.insert(session);
                Ok(())
            }
        }
    }

    /// 按 Call-ID 获取会话的共享读引用。
    ///
    /// - **契约 (What)**：返回 `Option<Ref>`；调用方可通过 `Deref` 访问 `CallSession`；
    /// - **风险 (Trade-offs)**：持有 guard 期间，同分片写操作会被阻塞，应尽快释放。
    pub fn get_session(&self, call_id: &str) -> Option<SessionRef<'_>> {
        self.sessions.get(call_id)
    }

    /// 按 Call-ID 获取会话的可变引用。
    ///
    /// - **意图 (Why)**：在更新状态或 B-leg 时需要独占访问；
    /// - **契约 (What)**：返回 `Option<RefMut>`，失败表示未注册。
    pub fn get_session_mut(&self, call_id: &str) -> Option<SessionRefMut<'_>> {
        self.sessions.get_mut(call_id)
    }

    /// 移除并返回会话实例。
    ///
    /// - **意图 (Why)**：在会话终止后释放资源，避免悬挂条目；
    /// - **契约 (What)**：返回 `Option<CallSession>`；
    /// - **后置条件**：若存在则从 `DashMap` 中移除对应条目。
    pub fn remove_session(&self, call_id: &str) -> Option<CallSession> {
        self.sessions.remove(call_id).map(|(_, session)| session)
    }

    /// 当前管理的会话数量。
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// 仓储是否为空。
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}

/// `DashMap` 读锁的类型别名，简化调用方签名。
pub type SessionRef<'a> = Ref<'a, Arc<str>, CallSession>;
/// `DashMap` 写锁的类型别名。
pub type SessionRefMut<'a> = RefMut<'a, Arc<str>, CallSession>;
