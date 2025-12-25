/// # 教案级说明：为何直接重导出？
///
/// ## 背景（Why）
/// - `spark-codec-line` 不定义独立的旧版就绪枚举，只需要对外暴露与核心库一致的迁移工具。
/// - 通过重导出避免重复维护文档与实现，确保所有兼容策略由 `spark-core` 统一演进。
///
/// ## 架构位置（Where）
/// - 该文件位于扩展 crate 的 `compat::flow_ready` 子模块，为需要自行迁移的扩展调用者提供稳定入口。
///
/// ## 契约说明（What）
/// - `to_ready_state` 与 `to_poll_ready` 的完整契约详见 `spark-codecs::compat::flow_ready` 的注释，本模块保证语义不发生任何更改。
/// - 使用前需要启用 `compat_v0` 功能，以便自动开启核心库的同名特性。
///
/// ## 风险与注意事项（Trade-offs）
/// - 若未来扩展 crate 引入自有的旧状态类型，可在此文件新增包裹逻辑；当前保持简单以降低维护开销。
pub use spark_codecs::compat::flow_ready::{to_poll_ready, to_ready_state};
