//! SIP 主题测试入口。
//!
//! # 教案式定位
//! - **Why**：`spark-tck` 需要为 SIP INVITE/CANCEL 事务建立统一的竞态回归测试，确保实现者
//!   在多线程环境下仍能遵循 RFC 3261 的状态机语义。
//! - **How**：模块内部按专题拆分，当前仅包含 `cancel_race`，覆盖 CANCEL 与最终响应竞速的核心
//!   分支；后续可以在此扩展注册路由、分叉呼叫等专题。
//! - **What**：对外公开子模块，方便 `cargo test -p spark-tck -- sip::cancel_race::*` 精确执行
//!   CANCEL 竞态相关用例。

pub mod cancel_race;
