use alloc::sync::Arc;

use dashmap::DashMap;
use spark_codec_sip::{Aor, ContactUri};

/// `LocationStore` 充当内存中的 SIP Registrar 表。
///
/// # 教案式解读
/// - **意图（Why）**：
///   - Registrar 服务需保存最新的 `Aor -> ContactUri` 映射，以便 INVITE、MESSAGE 等请求能够路由到终端的最新接入点；
///   - 使用专用结构封装 `DashMap`，统一对外暴露的注册/查询接口，避免业务代码直接依赖底层并发映射实现细节。
/// - **作用域（Where）**：
///   - 隶属于 `spark-switch::applications` 模块，被 Registrar 以及未来的会话控制服务共享；
///   - 采用 `Arc` 以便多个 Service 实例或线程安全地共享同一张注册表。
/// - **实现策略（How）**：
///   - 内部持有 `Arc<DashMap<Aor, ContactUri>>`：`DashMap` 提供无锁读、多写安全特性，契合 REGISTER 高频读写场景；
///   - 对外提供 `register` 与 `lookup`，并保留对 `DashMap` 原语的封装空间，后续可追加 `remove`、`expire` 等高级操作。
///
/// # 契约说明
/// - **前置条件**：调用方需保证传入的 `Aor` 与 `ContactUri` 均已通过 `spark-codec-sip` 正规化，便于哈希命中。
/// - **后置条件**：
///   - `register` 会覆盖同名条目并返回旧值，方便上层判断是否发生终端迁移；
///   - `lookup` 返回最新的可达地址克隆，不会暴露内部可变引用，确保线程安全。
/// - **风险提示**：当前未实现逐出与 TTL 策略，若需要与 SIP `Expires` 头对齐，可在后续迭代中扩展。
#[derive(Debug, Clone)]
pub struct LocationStore {
    inner: Arc<DashMap<Aor, ContactUri>>,
}

impl Default for LocationStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LocationStore {
    /// 构造空的注册表。
    ///
    /// - **输入**：无；内部直接初始化空的 `DashMap`。
    /// - **输出**：新的 `LocationStore`，可被 `Arc` 克隆并跨线程共享。
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// 注册或更新终端地址。
    ///
    /// - **参数**：
    ///   - `aor`：Address of Record，通常来自 REGISTER 请求的 `To` 头或请求 URI；
    ///   - `contact`：终端声明的可达地址，可能携带传输参数。
    /// - **前置条件**：调用方应确保 `aor`、`contact` 已按业务规则通过验证。
    /// - **返回值**：若先前存在条目，则返回旧的 `ContactUri`，便于调用方感知迁移；否则返回 `None`。
    pub fn register(&self, aor: Aor, contact: ContactUri) -> Option<ContactUri> {
        self.inner.insert(aor, contact)
    }

    /// 按 AOR 查询最新的 Contact。
    ///
    /// - **输入**：待查询的 `Aor` 引用，避免不必要的克隆。
    /// - **输出**：命中时返回克隆后的 `ContactUri`，便于后续在其它线程继续使用；未命中返回 `None`。
    pub fn lookup(&self, aor: &Aor) -> Option<ContactUri> {
        self.inner.get(aor).map(|entry| entry.value().clone())
    }

    /// 返回内部并发映射的共享引用，供需要更细粒度操作的上层在受控范围内使用。
    #[must_use]
    pub fn map(&self) -> &Arc<DashMap<Aor, ContactUri>> {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_register_and_lookup_roundtrip() {
        let store = LocationStore::new();
        let aor = Aor::new("sip:alice@example.com");
        let contact = ContactUri::new("sip:alice@client.invalid");
        assert!(store.register(aor.clone(), contact.clone()).is_none());
        assert_eq!(store.lookup(&aor), Some(contact));
    }
}
