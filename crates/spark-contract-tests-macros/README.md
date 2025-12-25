# spark-contract-tests-macros

## 职责边界
- 提供 `#[spark_tck]` 属性宏，为测试模块自动注册 `crates/spark-contract-tests` 中定义的标准套件，减少重复样板。
- 负责在编译期校验套件声明与参数，防止遗漏关键契约主题（半关闭、背压、错误分类等）。
- 作为合规模板的一部分，被 `make ci-lints` 与 `make ci-doc-warning` 等流程调用，保证所有实现共享同一测试入口。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：实现 `spark_tck` 属性宏，内联包含默认套件解析、AST 注入与代码生成逻辑（`parse_suites`、`inject_tests` 等）。

## 状态机与错误域
- 默认套件涵盖 ReadyState、错误分类、安全事件等主题，具体断言由 `spark-contract-tests` 落地；宏需确保所有主题均被注册。
- 编译期错误统一通过 `syn::Error` 报告，提醒开发者修正参数或引用路径，归类为实现错误。
- 若需要扩展错误域，例如新增 `observability` 分类，需同步更新 `defaults.rs` 与相关文档链接。

## 关联契约与测试
- 依赖 [`crates/spark-contract-tests`](../spark-contract-tests) 暴露的 Runner API；升级核心测试后必须同步调整宏生成的函数签名。
- 与 [`crates/spark-tck`](../spark-tck) 协同：impl TCK 可选择附加套件，宏负责保证最小集合始终包含核心契约。
- 使用示例可参考 [`docs/ci/redline.md`](../../docs/ci/redline.md) 所描述的 CI 红线流程，确保在本地执行到相同的测试矩阵。

## 集成注意事项
- 属性宏默认以模块命名生成函数，需确保测试模块名称在同一 crate 内唯一，避免符号冲突。
- 若在工作区外部使用，需要在 `Cargo.toml` 中将本 crate 标记为 `dev-dependencies` 并启用 `proc-macro` 支持。
- 扩展自定义套件时，请确认目标主题已经在 README 与根索引中登记，避免文档与实际测试集脱节。
