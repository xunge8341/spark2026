# ADR 0001: crate 结构与治理策略

- 状态：提议（Proposed）
- 日期：2024-05-20

## 背景

为了在 Spark2026 项目中保持 crate 结构、依赖关系及特性管理的一致性，需要制定统一的治理策略。此前的贡献中缺乏明确的边界约束，导致难以追踪 crate 之间的职责划分和依赖约定。本 ADR 针对 crate 命名、目录放置、依赖准入及 feature 策略进行统一约束，以支撑未来的模块扩展与协同开发。

## 决策

1. **crate 命名规范**
   - 所有第一方 crate 必须使用 `spark_` 或 `spark2026_` 前缀，后缀根据领域功能进行命名（例如 `spark_maths`、`spark2026_executor`）。
   - 跨语言接口或 FFI 相关 crate 使用 `spark_ffi_*` 命名，确保搜索时易于辨识。
   - 原型或实验性功能必须以 `spark_experimental_*` 命名并在 `Cargo.toml` 中明确标注 `publish = false`。

2. **目录放置与边界**
   - 可重用核心逻辑和平台服务放置于 `crates/` 目录，按领域或层次划分子目录；原则上同一层次的 crate 不得跨目录放置。
   - 面向示例或教程的 crate 应置于 `examples/`，与生产 crate 分离。
   - 面向 SDK 的输出放置于 `sdk/`，与内核实现隔离，避免双向依赖。
   - 所有 crate 必须在 README 或模块级文档中说明边界和与其他 crate 的交互方式。

3. **依赖管理策略**
   - 引入新三方依赖必须在 PR 中说明用途、替代方案及安全评估，优先使用已有依赖。
   - 同一功能域内优先通过内部 crate 复用，禁止因重复实现而增加新的三方库。
   - `default-features = false` 为默认要求，仅在确实需要时启用上游特性，并在 `Cargo.toml` 中说明原因。
   - 对于 `std`、`alloc`、`no_std` 等环境差异，必须明确标注依赖的可用目标平台。

4. **feature 策略**
   - 面向运行时行为切换的 feature 必须提供文档，解释 feature 的责任边界与兼容性。
   - Feature 应互斥或组合时，需要在 `cfg` 检查中添加编译期断言，避免非法组合。
   - 功能默认保持最小特性集（`default = []`），通过显式启用组合实现扩展；禁止将实验性功能设为默认。
   - 不同层级的 feature 需通过命名层级体现（例如 `storage-s3`, `storage-gcs`），确保可搜索性。

## 影响

- 新增 crate 或修改依赖时必须先校验是否符合上述命名及目录策略，否则无法通过审查。
- PR 模板与贡献指南需同步更新，对提交者给出明确操作指引。
- 违反上述规范的既有 crate 在后续迭代中需要逐步重构以对齐本 ADR。

## 命名映射与缩写白名单

| 历史命名 | 当前命名 | 适用层级 | 调整理由 |
| --- | --- | --- | --- |
| `spark-impl-tck` | `spark-tck` | 传输实现兼容性测试 | 去除 `impl` 非共识缩写，使 crate 名与职责直接对应。 |
| `spark-ember` | `spark-examples` | Pipeline 运行时示例宿主 | 统一 `spark-*` 前缀并显式标注示例用途，避免抽象名词歧义。 |

> 缩写白名单：`io`、`ip`、`tcp`、`udp`、`tls`、`mtls`、`http`、`quic`、`rtp`、`sdp`、`sip`、`dns`、`ssh`、`wasm`、`ffi`、`abi`、`uuid`、`jwt`、`otel`。

以上白名单用于约束 crate、模块与特性中的缩写使用；新增缩写需在 ADR 更新后方可引入。

## 备选方案

- 继续沿用无统一标准的方式：被否决，原因是长期演进中会导致模块间耦合不清。
- 仅依赖口头约定：被否决，原因是难以在新成员加入或异步协作时传达。

## 执行计划

1. 发布本 ADR，并在仓库根目录引导贡献者阅读。
2. 更新 CONTRIBUTING.md，收敛提交流程与质量门槛。
3. 对现有 crate 做快速审计，列出后续需要整改的项目，排进技术债计划。

## 参考

- Rust RFC: [RFC 1122 Cargo Features](https://rust-lang.github.io/rfcs/1122-cargo-features.html)
- Rust 官方 book: [Package Layout](https://doc.rust-lang.org/cargo/reference/manifest.html#the-package-section)
