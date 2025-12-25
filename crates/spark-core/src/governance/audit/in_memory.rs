use alloc::sync::Arc;
use alloc::vec::Vec;

use super::event::AuditEventV1;
use super::recorder::{AuditError, AuditRecorder};

//
// 教案级说明：`loom` 运行时需要接管互斥锁以枚举调度交错，因此在模型检查配置下
// 显式切换到 `loom::sync::Mutex`。在常规构建中仍保留 `std::sync::Mutex`，确保运行时
// 行为与既有实现一致且无额外性能损耗。
#[cfg(all(feature = "std", any(loom, spark_loom)))]
use loom::sync::Mutex;
#[cfg(all(feature = "std", not(any(loom, spark_loom))))]
use std::sync::Mutex;

/// 基于内存的 Recorder，便于测试与演示回放。
#[cfg(feature = "std")]
#[derive(Default)]
pub struct InMemoryAuditRecorder {
    inner: Mutex<InMemoryRecorderState>,
}

#[cfg(feature = "std")]
#[derive(Default)]
struct InMemoryRecorderState {
    events: Vec<AuditEventV1>,
    last_hash: Option<String>,
}

#[cfg(feature = "std")]
impl InMemoryAuditRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// 返回当前已记录的事件副本。
    pub fn events(&self) -> Vec<AuditEventV1> {
        self.inner.lock().expect("poison").events.clone()
    }
}

#[cfg(feature = "std")]
impl AuditRecorder for InMemoryAuditRecorder {
    fn record(&self, event: AuditEventV1) -> crate::Result<(), AuditError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| AuditError::new("recorder poisoned"))?;
        if let Some(previous) = &guard.last_hash
            && previous != &event.state_prev_hash
        {
            return Err(AuditError::new("detected hash gap in audit chain"));
        }
        guard.last_hash = Some(event.state_curr_hash.clone());
        guard.events.push(event);
        Ok(())
    }
}

#[cfg(feature = "std")]
impl InMemoryAuditRecorder {
    /// 构造便于测试使用的 `Arc<dyn AuditRecorder>`。
    pub fn shared(self) -> Arc<dyn AuditRecorder> {
        Arc::new(self)
    }
}
