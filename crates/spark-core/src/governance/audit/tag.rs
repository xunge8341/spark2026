use alloc::sync::Arc;
use core::ops::Deref;

/// 审计标签的不可变封装，为事件和记录器提供统一命名空间。
///
/// # 教案式说明
/// - **意图（Why）**：各个子系统在记录审计事件时，需要引用“变更来源”“治理策略”等标签；使用强类型避免重复声明字面量。
/// - **逻辑（How）**：内部以 `Arc<str>` 存储标签文本，保证克隆成本仅为原子引用计数，自身不暴露可变接口。
/// - **契约（What）**：通过 [`AuditTag::new`] 构造；调用 [`AuditTag::as_str`] 读取；如需共享所有权，可调用 [`AuditTag::into_arc`]
///   获取内部 `Arc<str>`。
/// - **风险提示（Trade-offs）**：类型不会去重或校验命名，若标签数量巨大需在上层自行做控制。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AuditTag(Arc<str>);

impl AuditTag {
    /// 根据输入字符串构造新的审计标签。
    ///
    /// # 契约说明
    /// - **输入参数**：任意可转换为 `Arc<str>` 的值（`&'static str`、`String`、`Arc<str>` 等）。
    /// - **后置条件**：返回值持有标签文本的共享引用，多次克隆不会复制底层字符串。
    pub fn new(tag: impl Into<Arc<str>>) -> Self {
        Self(tag.into())
    }

    /// 以 `&str` 形式读取标签内容，便于写入日志或序列化。
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 将标签转换为 `Arc<str>`，用于跨线程共享或缓存。
    pub fn into_arc(self) -> Arc<str> {
        self.0
    }
}

impl Deref for AuditTag {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
