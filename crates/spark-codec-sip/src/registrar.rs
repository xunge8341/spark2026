use alloc::string::ToString;
use alloc::{string::String, sync::Arc};
use core::{fmt, ops::Deref};

use crate::types::{NameAddr, SipScheme, SipUri};

/// 基于零拷贝视角的 AOR 轻量引用。
///
/// # 教案式说明
/// - **意图（Why）**：在协议解析阶段，很多场景仅需要读取 AOR 以执行路由/鉴权判定，不必立即分配所有权字符串。
///   通过持有 [`SipUri`] 切片，可让编解码阶段与业务阶段解耦内存策略，避免无谓堆分配。
/// - **定位（Where）**：位于 `spark-codec-sip::registrar`，供 Registrar、Transaction 等上层在需要时再延迟获取所有权。
/// - **契约（What）**：
///   - 输入：必须基于解析完成的 [`SipUri`] 或 [`NameAddr`] 构造，隐含前置条件为底层缓冲仍然有效；
///   - 输出：`into_owned` 可生成有所有权的 [`Aor`]，满足跨线程存储要求；
///   - 后置：不在本类型上做额外的字符串校验，保证零拷贝读取路径轻量。
/// - **权衡与注意事项（Trade-offs & Gotchas）**：
///   - 该类型仅持引用，生命周期绑定输入缓冲；若需跨请求存储必须调用 [`into_owned`](Self::into_owned)。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AorRef<'a> {
    uri: SipUri<'a>,
}

impl<'a> AorRef<'a> {
    /// 将零拷贝引用转换为具备所有权的 [`Aor`]。
    ///
    /// - **输入/前置**：调用方需确保底层 URI 已验证合法；
    /// - **输出/后置**：返回的 `Aor` 拥有独立存储，可安全写入 `LocationStore`。
    #[must_use]
    pub fn into_owned(self) -> Aor {
        Aor::from_uri(&self.uri)
    }

    /// 直接读取底层 URI 引用，便于在只读判定场景下避免分配。
    #[must_use]
    pub fn as_uri(&self) -> &SipUri<'a> {
        &self.uri
    }
}

impl<'a> From<&NameAddr<'a>> for AorRef<'a> {
    fn from(addr: &NameAddr<'a>) -> Self {
        Self { uri: addr.uri }
    }
}

impl<'a> From<&SipUri<'a>> for AorRef<'a> {
    fn from(uri: &SipUri<'a>) -> Self {
        Self { uri: *uri }
    }
}

/// 终端 Contact 地址的零拷贝视图。
///
/// # 设计摘要
/// - **Why**：避免在解析阶段即创建拥有所有权的 `String`/`Arc<str>`，对齐 REGISTER 高频但体积小的场景，降低 GC/分配压力。
/// - **How**：内部仅保存 [`SipUri`] 的切片，提供 `into_owned` 将其升级为持久化存储所需的 [`ContactUri`]。
/// - **What**：调用方需保证输入 URI 合法且缓冲未被回收；输出所有权对象满足线程安全使用。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ContactUriRef<'a> {
    uri: SipUri<'a>,
}

impl<'a> ContactUriRef<'a> {
    /// 获取对底层 URI 的只读访问。
    #[must_use]
    pub fn as_uri(&self) -> &SipUri<'a> {
        &self.uri
    }

    /// 升级为拥有所有权的 [`ContactUri`]。
    ///
    /// - **输入**：零拷贝引用；需确保生命周期尚未结束。
    /// - **输出**：可跨线程安全共享的 `ContactUri`。
    #[must_use]
    pub fn into_owned(self) -> ContactUri {
        ContactUri::from_uri(&self.uri)
    }
}

impl<'a> From<&NameAddr<'a>> for ContactUriRef<'a> {
    fn from(addr: &NameAddr<'a>) -> Self {
        Self { uri: addr.uri }
    }
}

impl<'a> From<&SipUri<'a>> for ContactUriRef<'a> {
    fn from(uri: &SipUri<'a>) -> Self {
        Self { uri: *uri }
    }
}

/// `Aor`（Address of Record）提供 SIP 注册表中的主键表示。
///
/// # 教案式速览
/// - **意图（Why）**：
///   - Registrar 需要使用地址（通常来自 `To` 头或请求 URI）作为逻辑主键，以便在 Location Store 中快速定位终端。
///   - 直接存储字符串存在重复拼接、大小写不一致的问题，因此使用专用类型统一格式与比较规则。
/// - **定位（Where）**：
///   - 位于 `spark-codec-sip`，与解析结果保持紧密耦合，便于后续扩展（例如支持 `gruu`、`sip.instance`）。
///   - 被 `spark-switch` 等上层组件作为哈希表键值使用。
/// - **结构（What）**：内部以 `Arc<str>` 承载规范化后的 URI，既保证所有权，又允许在多个组件间零拷贝共享。
///
/// # 合同说明
/// - **前置条件**：调用方必须提供合法的 SIP URI；可通过 [`SipUri`] 或 [`NameAddr`] 解析结果构造。
/// - **后置条件**：生成的 `Aor` 提供 `Eq`/`Hash` 语义，可直接作为 `DashMap` 键；`Display`/`Deref` 暴露原始字符串。
/// - **边界情况**：若 URI 中包含参数或 header，均保持原样存储；后续业务可根据具体需要进一步解析。
///
/// # 实现要点
/// - `Arc<str>` 保证克隆仅增加引用计数，适合在多线程 Registrar 中共享。
/// - 通过 [`Self::from_uri`]、[`Self::from_name_addr`] 辅助函数封装格式化逻辑，避免重复实现字符串拼接。
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Aor(Arc<str>);

impl Aor {
    /// 使用已经归一化的字符串构造 `Aor`。
    ///
    /// - **输入**：任意可转换为 `Arc<str>` 的所有权数据，例如 `String` 或 `Arc<str>`。
    /// - **输出**：新的 `Aor` 实例，内部以原样存储字符串。
    /// - **注意事项**：调用前应确保字符串已经满足 SIP URI 语法；本方法不会再次验证合法性。
    #[must_use]
    pub fn new(raw: impl Into<Arc<str>>) -> Self {
        Self(raw.into())
    }

    /// 从零拷贝的 [`SipUri`] 构造地址主键。
    ///
    /// # 逻辑（How）
    /// 1. 将 `scheme`、`userinfo`、`host`、`port`、`params`、`headers` 重新组合为文本。
    /// 2. 复用 `Arc<str>` 存储结果，确保在 Location Store 中克隆时只有原子加减操作开销。
    #[must_use]
    pub fn from_uri(uri: &SipUri<'_>) -> Self {
        Self::new(format_sip_uri(uri))
    }

    /// 从 [`NameAddr`] 提取 URI 并构造 `Aor`。
    #[must_use]
    pub fn from_name_addr(addr: &NameAddr<'_>) -> Self {
        Self::from_uri(&addr.uri)
    }
}

impl Deref for Aor {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for Aor {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Aor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// `ContactUri` 代表终端在 `Contact` 头中声明的可达地址。
///
/// # 角色定位
/// - **Why**：Registrar 需要记录终端的实时接入点（可能包含传输参数、实例标识等），以便 INVITE、MESSAGE 等请求能被路由到最新位置。
/// - **How**：复用 `Arc<str>` 持有格式化后的 URI 字符串；提供从 `SipUri`/`NameAddr` 的转换接口，确保解析层与注册层保持一致。
/// - **What**：实现 `Clone`/`Eq`/`Hash`，便于在缓存、断言和测试场景中直接比较。
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ContactUri(Arc<str>);

impl ContactUri {
    /// 直接以字符串构造 `ContactUri`。
    #[must_use]
    pub fn new(raw: impl Into<Arc<str>>) -> Self {
        Self(raw.into())
    }

    /// 根据 [`SipUri`] 格式化为注册表可存储的地址。
    #[must_use]
    pub fn from_uri(uri: &SipUri<'_>) -> Self {
        Self::new(format_sip_uri(uri))
    }

    /// 从 name-addr 结构体提取 URI。
    #[must_use]
    pub fn from_name_addr(addr: &NameAddr<'_>) -> Self {
        Self::from_uri(&addr.uri)
    }
}

impl Deref for ContactUri {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for ContactUri {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ContactUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn format_sip_uri(uri: &SipUri<'_>) -> String {
    let mut text = String::new();
    match uri.scheme {
        SipScheme::Sip => text.push_str("sip:"),
        SipScheme::Sips => text.push_str("sips:"),
    }
    if let Some(userinfo) = uri.userinfo {
        text.push_str(userinfo);
        text.push('@');
    }
    text.push_str(uri.host);
    if let Some(port) = uri.port {
        text.push(':');
        text.push_str(&port.to_string());
    }
    if let Some(params) = uri.params {
        text.push_str(params);
    }
    if let Some(headers) = uri.headers {
        text.push('?');
        text.push_str(headers);
    }
    text
}
