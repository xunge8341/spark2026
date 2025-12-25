# spark-macros

## 职责边界
- 提供 `#[spark::service]` 等过程宏，生成符合 `spark-core` 契约的 `Service` 实现，统一调度、背压与关闭语义。
- 承担跨 crate 的 API 稳定层：通过在 `spark-core` 中的 re-export，保障业务方只依赖一个命名空间即可升级。
- 在编译期对 Service 声明进行语义校验，补充文档中定义的调用约束，避免运行期才暴露契约违规。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：暴露 `service` 过程宏，并内联 `expand_service`、`extract_result_types` 等辅助逻辑以生成符合契约的 Service 模板。

## 状态机与错误域
- 宏生成的 `poll_ready` 会自动映射业务返回值到 `ReadyState`，并补充 `BusyReason`，对应 [`docs/graceful-shutdown-contract.md`](../../docs/graceful-shutdown-contract.md) 对半关闭顺序的要求。
- 编译期校验捕获的非法声明（缺少 `use spark_core as spark;`、异步签名错误等）会产生 `syn::Error`，最终落在 `ErrorCategory::ImplementationError`。
- 运行期错误保持业务返回的 `CoreError` 或 `DomainError`，宏不会包裹或改写错误分类，以免破坏 [`crates/spark-contract-tests`](../spark-contract-tests) 的断言。

## 关联契约与测试
- 与 [`crates/spark-core`](../spark-core) 紧耦合：任何宏语义变更需要同时更新 core 中的 re-export 与文档示例。
- [`crates/spark-contract-tests-macros`](../spark-contract-tests-macros) 提供针对过程宏的编译期测试，覆盖 Service 生命周期、背压信号与错误透传。
- 示例服务定义可参考 [`docs/getting-started.md`](../../docs/getting-started.md) 与 [`docs/context-sugar.md`](../../docs/context-sugar.md)，确保宏使用姿势一致。

## 集成注意事项
- 宏默认依赖调用方在模块顶层执行 `use spark_core as spark;`，否则路径无法解析；该约束在编译期会给出指向性提示。
- 生成代码假定调用方在 `no_std + alloc` 环境下可用，如需纯 `no_std` 支持需关注 [`docs/no-std-compatibility-report.md`](../../docs/no-std-compatibility-report.md) 的后续迭代。
- 若需要自定义过程宏或扩展属性，请在 issue 中描述预期状态机与错误分类，避免破坏既有契约。
