use crate::{
    CoreError,
    error::{self, ErrorCategory},
    observability::{
        OwnedAttributeSet,
        keys::{
            attributes::error as attr_keys, labels::error_kind as error_kind_labels,
            metrics::service as service_labels,
        },
    },
};
use alloc::borrow::Cow;
use core::time::Duration;

/// 错误观测视图：将 [`CoreError`] 的分类信息转换为结构化标签集合。
///
/// # 教案式说明（Why）
/// - 统一产出 `error.*` 属性，避免各调用方手写字符串导致可观测性维度漂移；
/// - 将错误分类、重试建议、安全类型与预算语义集中映射，便于指标、日志与追踪复用；
/// - 提供可测试的中间表示，契约测试可直接断言字段而无需解析 `OwnedAttributeSet`。
///
/// # 契约定义（What）
/// - `code`：稳定错误码；
/// - `category`：`ErrorCategory` 的蛇形命名字符串；
/// - `kind`：跨域统一的错误类别标签（用于 `error.kind`）；
/// - `retryable`：布尔标记；
/// - `retry_wait_ms` / `retry_reason`：仅在 `Retryable` 时存在；
/// - `security_class`：安全事件的分类代码；
/// - `budget_kind`：资源耗尽时的预算标签；
/// - `busy_direction`：默认自动响应中声明的繁忙方向（上游/下游）。
///
/// # 风险提示（Trade-offs & Gotchas）
/// - `busy_direction` 来自错误矩阵的默认策略，若业务显式覆写分类且未在矩阵登记，字段将缺失；
/// - `budget_kind` 对 `Custom` 预算会克隆字符串，建议在热路径复用 `OwnedAttributeSet` 以摊销分配成本。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorTelemetryView {
    pub code: &'static str,
    pub category: &'static str,
    pub kind: &'static str,
    pub retryable: bool,
    pub retry_wait_ms: Option<u64>,
    pub retry_reason: Option<Cow<'static, str>>,
    pub security_class: Option<&'static str>,
    pub budget_kind: Option<Cow<'static, str>>,
    pub busy_direction: Option<&'static str>,
}

impl ErrorTelemetryView {
    /// 根据 [`CoreError`] 构造观测视图。
    ///
    /// # 实现逻辑（How）
    /// 1. 通过 [`CoreError::category`] 获取结构化分类；
    /// 2. 解析 `RetryAdvice`、`SecurityClass`、`BudgetKind` 等细节生成可选字段；
    /// 3. 查询错误矩阵的默认自动响应，补充繁忙方向；
    /// 4. 生成 `ErrorTelemetryView`，供调用方注入属性集合或用于测试断言。
    pub fn from_error(error: &CoreError) -> Self {
        let category = error.category();
        let kind = error_kind_label(&category);
        let category_label = category_label(&category);
        let retryable = matches!(category, ErrorCategory::Retryable(_));

        let (retry_wait_ms, retry_reason) = retry_metadata(&category);
        let security_class = security_class_code(&category);
        let budget_kind = budget_label(&category);
        let busy_direction = busy_direction_label(error.code(), &category);

        ErrorTelemetryView {
            code: error.code(),
            category: category_label,
            kind,
            retryable,
            retry_wait_ms,
            retry_reason,
            security_class,
            budget_kind,
            busy_direction,
        }
    }

    /// 将视图写入 [`OwnedAttributeSet`]。
    ///
    /// # 合同说明（What）
    /// - `target`：调用方提供的可变属性集合；现有条目会保留，仅在末尾追加新的键值；
    /// - 追加键遵循 `contracts/observability_keys.toml` 中的 `attributes::error` 契约；
    /// - 若某字段为 `None`，将跳过对应键，避免产生空值。
    pub fn apply_to(&self, target: &mut OwnedAttributeSet) {
        target.push_owned(attr_keys::ATTR_CODE, self.code);
        target.push_owned(attr_keys::ATTR_CATEGORY, self.category);
        target.push_owned(attr_keys::ATTR_KIND, self.kind);
        target.push_owned(attr_keys::ATTR_RETRYABLE, self.retryable);

        if let Some(wait) = self.retry_wait_ms {
            target.push_owned(attr_keys::ATTR_RETRY_WAIT_MS, wait);
        }
        if let Some(reason) = self.retry_reason.clone() {
            target.push_owned(attr_keys::ATTR_RETRY_REASON, reason);
        }
        if let Some(class) = self.security_class {
            target.push_owned(attr_keys::ATTR_SECURITY_CLASS, class);
        }
        if let Some(kind) = self.budget_kind.clone() {
            target.push_owned(attr_keys::ATTR_BUDGET_KIND, kind);
        }
        if let Some(direction) = self.busy_direction {
            target.push_owned(attr_keys::ATTR_BUSY_DIRECTION, direction);
        }
    }
}

fn category_label(category: &ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::Retryable(_) => "retryable",
        ErrorCategory::NonRetryable => "non_retryable",
        ErrorCategory::ResourceExhausted(_) => "resource_exhausted",
        ErrorCategory::Security(_) => "security",
        ErrorCategory::ProtocolViolation => "protocol_violation",
        ErrorCategory::Cancelled => "cancelled",
        ErrorCategory::Timeout => "timeout",
        #[allow(unreachable_patterns)]
        _ => "unknown",
    }
}

fn error_kind_label(category: &ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::Retryable(_) => error_kind_labels::RETRYABLE,
        ErrorCategory::NonRetryable => error_kind_labels::NON_RETRYABLE,
        ErrorCategory::ResourceExhausted(_) => error_kind_labels::RESOURCE_EXHAUSTED,
        ErrorCategory::Security(_) => error_kind_labels::SECURITY,
        ErrorCategory::ProtocolViolation => error_kind_labels::PROTOCOL_VIOLATION,
        ErrorCategory::Cancelled => error_kind_labels::CANCELLED,
        ErrorCategory::Timeout => error_kind_labels::TIMEOUT,
        #[allow(unreachable_patterns)]
        _ => error_kind_labels::UNKNOWN,
    }
}

fn retry_metadata(category: &ErrorCategory) -> (Option<u64>, Option<Cow<'static, str>>) {
    match category {
        ErrorCategory::Retryable(advice) => {
            (Some(duration_to_millis(advice.wait)), advice.reason.clone())
        }
        _ => (None, None),
    }
}

fn security_class_code(category: &ErrorCategory) -> Option<&'static str> {
    match category {
        ErrorCategory::Security(class) => Some(class.code()),
        _ => None,
    }
}

fn budget_label(category: &ErrorCategory) -> Option<Cow<'static, str>> {
    match category {
        ErrorCategory::ResourceExhausted(kind) => Some(kind.observability_label()),
        _ => None,
    }
}

fn busy_direction_label(code: &str, category: &ErrorCategory) -> Option<&'static str> {
    if !matches!(category, ErrorCategory::Retryable(_)) {
        return None;
    }

    error::category_matrix::default_autoresponse(code)
        .and_then(|response| response.retry())
        .and_then(|(_, _, busy)| busy)
        .map(|disposition| match disposition {
            error::category_matrix::BusyDisposition::Upstream => {
                service_labels::READY_DETAIL_UPSTREAM
            }
            error::category_matrix::BusyDisposition::Downstream => {
                service_labels::READY_DETAIL_DOWNSTREAM
            }
        })
}

fn duration_to_millis(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        u64::MAX
    } else {
        millis as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::codes, observability::OwnedAttributeSet, security::SecurityClass,
        status::RetryAdvice, types::BudgetKind,
    };
    use alloc::collections::BTreeMap;
    use core::time::Duration;

    fn attrs_to_map(set: &OwnedAttributeSet) -> BTreeMap<&str, String> {
        let mut map = BTreeMap::new();
        for kv in set.as_slice() {
            let value = match &kv.value {
                crate::observability::MetricAttributeValue::Text(text) => text.as_ref().to_string(),
                crate::observability::MetricAttributeValue::Bool(flag) => flag.to_string(),
                crate::observability::MetricAttributeValue::F64(number) => number.to_string(),
                crate::observability::MetricAttributeValue::I64(number) => number.to_string(),
            };
            map.insert(kv.key.as_ref(), value);
        }
        map
    }

    #[test]
    fn retryable_error_emits_retry_labels() {
        let error = CoreError::new(codes::APP_BACKPRESSURE_APPLIED, "backpressure");
        let view = ErrorTelemetryView::from_error(&error);

        assert_eq!(view.code, codes::APP_BACKPRESSURE_APPLIED);
        assert_eq!(view.kind, error_kind_labels::RETRYABLE);
        assert!(view.retryable);
        assert!(view.retry_wait_ms.is_some());
        assert_eq!(
            view.busy_direction,
            Some(service_labels::READY_DETAIL_DOWNSTREAM)
        );

        let mut attrs = OwnedAttributeSet::new();
        view.apply_to(&mut attrs);
        let map = attrs_to_map(&attrs);

        assert_eq!(
            map.get(attr_keys::ATTR_CODE).unwrap(),
            codes::APP_BACKPRESSURE_APPLIED
        );
        assert_eq!(
            map.get(attr_keys::ATTR_KIND).unwrap(),
            error_kind_labels::RETRYABLE
        );
        assert_eq!(map.get(attr_keys::ATTR_RETRYABLE).unwrap(), "true");
        assert_eq!(
            map.get(attr_keys::ATTR_BUSY_DIRECTION).unwrap(),
            service_labels::READY_DETAIL_DOWNSTREAM
        );
    }

    #[test]
    fn explicit_retryable_overrides_reason() {
        let advice = RetryAdvice::after(Duration::from_millis(500)).with_reason("custom");
        let error =
            CoreError::new("test.retry", "retry").with_category(ErrorCategory::Retryable(advice));
        let view = ErrorTelemetryView::from_error(&error);

        assert_eq!(view.retry_wait_ms, Some(500));
        assert_eq!(view.retry_reason.as_deref(), Some("custom"));

        let mut attrs = OwnedAttributeSet::new();
        view.apply_to(&mut attrs);
        let map = attrs_to_map(&attrs);
        assert_eq!(map.get(attr_keys::ATTR_RETRY_WAIT_MS).unwrap(), "500");
        assert_eq!(map.get(attr_keys::ATTR_RETRY_REASON).unwrap(), "custom");
    }

    #[test]
    fn security_and_budget_information_present() {
        let security = CoreError::new("app.unauthorized", "denied")
            .with_category(ErrorCategory::Security(SecurityClass::Authorization));
        let mut security_attrs = OwnedAttributeSet::new();
        ErrorTelemetryView::from_error(&security).apply_to(&mut security_attrs);
        let security_map = attrs_to_map(&security_attrs);
        assert_eq!(
            security_map.get(attr_keys::ATTR_SECURITY_CLASS).unwrap(),
            SecurityClass::Authorization.code()
        );

        let exhausted = CoreError::new("protocol.budget_exceeded", "decode")
            .with_category(ErrorCategory::ResourceExhausted(BudgetKind::Decode));
        let mut exhausted_attrs = OwnedAttributeSet::new();
        ErrorTelemetryView::from_error(&exhausted).apply_to(&mut exhausted_attrs);
        let exhausted_map = attrs_to_map(&exhausted_attrs);
        assert_eq!(
            exhausted_map.get(attr_keys::ATTR_BUDGET_KIND).unwrap(),
            "decode"
        );
    }
}
