//! 弃用生命周期管理模块，为框架提供统一的“标注 + 运行时告警”能力。
//!
//! # 设计动机（Why）
//! - T23 要求：每个弃用项必须保障两个版本的过渡窗口，并在运行中给出可执行告警。
//! - 通过集中管理元信息与告警逻辑，避免各模块重复实现 `AtomicBool` 或日志拼装代码。
//! - 兼容 `no_std + alloc` 环境：在无宿主日志时至少提供标准输出降级策略。
//!
//! # 功能概览（How）
//! - [`DeprecationNotice`]：封装弃用符号元数据与一次性告警开关。
//! - [`DeprecationNotice::emit`]：在首次调用时输出 WARN 日志，并确保后续调用静默。
//! - [`LEGACY_LOOPBACK_OUTBOUND`]：展示如何定义具体弃用项，供策略与 CI 样例复用。
//!
//! # 使用契约（What）
//! - **前置条件**：调用方在触发弃用路径时需显式注入日志句柄；若缺失则退化至标准错误输出（仅 `std` 环境）。
//! - **后置条件**：函数保证至多输出一次告警，避免日志风暴；调用者可通过单元测试 API 重置状态。
//!
//! # 风险提示（Trade-offs & Gotchas）
//! - `AtomicBool` 使用 `SeqCst` 内存序，确保多线程场景下不会重复告警，代价是极轻微的性能成本。
//! - 若调用方在纯 `alloc` 环境且未提供日志，将无法看到告警；文档中要求宿主提供兜底通道。
//! - 常量配置在编译期固定；若需热更新请改用宿主侧配置文件。

use crate::observability::{
    Logger, OwnedAttributeSet, keys::logging::deprecation as deprecation_fields,
};
use core::sync::atomic::{AtomicBool, Ordering};

/// 描述单个弃用符号的元信息与告警状态。
///
/// # 结构意图（Why）
/// - 将“符号标识 + 生效版本 + 计划移除版本 + 跟踪链接”打包，方便在日志中输出完整上下文。
/// - 使用内部 `AtomicBool` 记录是否已告警，避免多次输出造成噪音。
///
/// # 字段说明（What）
/// - `symbol`：符号全名，建议遵循 `module::item` 命名以便定位。
/// - `since`：宣告弃用的版本。
/// - `removal`：计划移除的目标版本。
/// - `tracking`：可选的追踪任务或文档链接，便于运维查询。
/// - `migration_hint`：补充迁移建议，在日志中帮助一线同学决策。
/// - `emitted`：内部状态位，记录是否已输出过告警。
#[derive(Debug)]
pub struct DeprecationNotice {
    symbol: &'static str,
    since: &'static str,
    removal: &'static str,
    tracking: Option<&'static str>,
    migration_hint: Option<&'static str>,
    emitted: AtomicBool,
}

impl DeprecationNotice {
    /// 构造新的弃用告警描述。
    ///
    /// # 参数约定（What）
    /// - `symbol`：目标弃用符号全名。
    /// - `since` / `removal`：使用 `MAJOR.MINOR.PATCH` 语义化版本字符串。
    /// - `tracking`：可选的任务编号、链接或文档锚点。
    /// - `migration_hint`：补充迁移策略简介，可为空。
    ///
    /// # 前置条件
    /// - 所有字符串必须是 `'static` 生命周期，保证全局常量可安全引用。
    ///
    /// # 后置条件
    /// - 返回的结构体初始 `emitted = false`，尚未触发告警。
    pub const fn new(
        symbol: &'static str,
        since: &'static str,
        removal: &'static str,
        tracking: Option<&'static str>,
        migration_hint: Option<&'static str>,
    ) -> Self {
        Self {
            symbol,
            since,
            removal,
            tracking,
            migration_hint,
            emitted: AtomicBool::new(false),
        }
    }

    /// 在运行时触发弃用告警。
    ///
    /// # 执行流程（How）
    /// 1. 使用 `compare_exchange` 检查告警是否已经发送；若已发送则直接返回。
    /// 2. 构造结构化字段集合（`OwnedAttributeSet`），携带符号、版本、追踪链接等信息。
    /// 3. 若提供日志句柄则输出 WARN 日志；否则在 `std` 环境回退到 `eprintln!`。
    ///
    /// # 契约约束（What）
    /// - **输入**：`logger` 可为空，表示宿主暂未注入结构化日志。
    /// - **前置条件**：调用方需保证该函数只在真正进入弃用路径时触发，避免误报。
    /// - **后置条件**：首次调用后 `emitted == true`，后续调用将静默。
    ///
    /// # 风险提示（Trade-offs）
    /// - 未提供日志且运行在 `no_std` 环境时不会输出任何告警，需要在宿主层补偿。
    pub fn emit(&self, logger: Option<&dyn Logger>) {
        if self
            .emitted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let mut fields = OwnedAttributeSet::new();
        fields.push_owned(deprecation_fields::FIELD_SYMBOL, self.symbol);
        fields.push_owned(deprecation_fields::FIELD_SINCE, self.since);
        fields.push_owned(deprecation_fields::FIELD_REMOVAL, self.removal);
        if let Some(tracking) = self.tracking {
            fields.push_owned(deprecation_fields::FIELD_TRACKING, tracking);
        }
        if let Some(hint) = self.migration_hint {
            fields.push_owned(deprecation_fields::FIELD_MIGRATION, hint);
        }

        if let Some(logger) = logger {
            logger.warn_with_fields(
                "调用了已弃用 API：请按照迁移指引尽快完成升级。",
                fields.as_slice(),
                None,
            );
        } else {
            #[cfg(feature = "std")]
            {
                use std::io::Write;
                let mut stderr = std::io::stderr();
                let _ = writeln!(
                    stderr,
                    "[spark-core][deprecation] symbol={symbol} since={since} removal={removal} tracking={tracking:?} migration={migration:?}",
                    symbol = self.symbol,
                    since = self.since,
                    removal = self.removal,
                    tracking = self.tracking,
                    migration = self.migration_hint,
                );
            }
        }
    }

    /// 返回当前告警是否已经触发，主要用于测试断言。
    pub fn has_emitted(&self) -> bool {
        self.emitted.load(Ordering::SeqCst)
    }

    /// **仅用于测试：** 重置内部状态，便于多轮调用。
    #[cfg(test)]
    pub fn reset_for_test(&self) {
        self.emitted.store(false, Ordering::SeqCst);
    }
}

/// `common::legacy_loopback_outbound` 的弃用元数据示例。
///
/// # 设计说明（Why）
/// - 作为策略文档与 CI 检查的参考实现，展示如何填写完整元信息。
///
/// # 契约（What）
/// - `tracking` 链接指向治理文档锚点，方便开发者快速阅读迁移方案。
/// - 计划在 `0.3.0` 版本移除，满足“两版过渡”要求。
pub static LEGACY_LOOPBACK_OUTBOUND: DeprecationNotice = DeprecationNotice::new(
    "spark_core::common::legacy_loopback_outbound",
    "0.1.0",
    "0.3.0",
    Some("docs/governance/deprecation.md#示例弃用commonlegacy_loopback_outbound"),
    Some("改用 Loopback::fire_loopback_outbound 以保留主路逻辑"),
);

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{string::String, vec::Vec};
    use std::sync::Mutex;

    use crate::observability::LogRecord;

    /// 记录日志调用的简易 Logger，用于验证告警字段。
    #[derive(Default)]
    struct RecordingLogger {
        records: Mutex<Vec<CapturedRecord>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CapturedRecord {
        message: String,
        attributes: Vec<(String, String)>,
    }

    impl Logger for RecordingLogger {
        fn log(&self, record: &LogRecord<'_>) {
            let mut attributes = Vec::new();
            for entry in record.attributes {
                attributes.push((
                    entry.key.to_string(),
                    match &entry.value {
                        crate::observability::MetricAttributeValue::Text(text) => text.to_string(),
                        crate::observability::MetricAttributeValue::Bool(value) => {
                            value.to_string()
                        }
                        crate::observability::MetricAttributeValue::F64(value) => value.to_string(),
                        crate::observability::MetricAttributeValue::I64(value) => value.to_string(),
                    },
                ));
            }
            self.records.lock().unwrap().push(CapturedRecord {
                message: record.message.to_string(),
                attributes,
            });
        }
    }

    impl RecordingLogger {
        fn last(&self) -> Option<CapturedRecord> {
            self.records.lock().unwrap().last().cloned()
        }
    }

    #[test]
    fn emit_only_once() {
        let logger = RecordingLogger::default();
        let notice = DeprecationNotice::new(
            "demo::symbol",
            "0.1.0",
            "0.3.0",
            Some("tracking"),
            Some("migrate"),
        );

        notice.emit(Some(&logger));
        assert!(notice.has_emitted());
        assert_eq!(logger.records.lock().unwrap().len(), 1);

        notice.emit(Some(&logger));
        assert_eq!(logger.records.lock().unwrap().len(), 1);
    }

    #[test]
    fn emit_with_fields() {
        let logger = RecordingLogger::default();
        let notice = DeprecationNotice::new(
            "demo::symbol",
            "0.1.0",
            "0.3.0",
            Some("T-link"),
            Some("please migrate"),
        );

        notice.emit(Some(&logger));
        let record = logger.last().expect("应写入一条日志");
        assert_eq!(
            record.message,
            "调用了已弃用 API：请按照迁移指引尽快完成升级。"
        );
        assert!(
            record
                .attributes
                .iter()
                .any(|(k, v)| k == deprecation_fields::FIELD_SYMBOL && v == "demo::symbol")
        );
        assert!(
            record
                .attributes
                .iter()
                .any(|(k, v)| k == deprecation_fields::FIELD_REMOVAL && v == "0.3.0")
        );
    }

    #[test]
    fn has_emitted_tracks_state() {
        let notice = DeprecationNotice::new("demo", "0.1.0", "0.3.0", None, None);
        assert!(!notice.has_emitted());
        notice.emit(None);
        assert!(notice.has_emitted());
    }
}
