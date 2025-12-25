# Contributing to Spark2026

欢迎来到 Spark2026！为了保持高质量的代码与文档，请在贡献前仔细阅读本指南。

## 准备工作

1. **环境要求**
   - Rust 稳定版（遵循 `rust-toolchain.toml`）
   - `cargo`、`make`、`just`（如需）等工具链
   - 对智能合约/零知识背景的基本了解

2. **分支策略**
   - 使用 `main` 作为长期稳定分支。
   - 功能开发、修复或文档更新请创建主题分支，命名建议：`feature/<summary>`、`fix/<issue-id>`、`docs/<topic>`。

3. **Issue 与任务管理**
   - 在提交 PR 前，请先确认是否已有对应 Issue。
   - 对应任务的依赖链（如 T-001~T-006）需在描述中明确，避免重复劳动。

## 仓库结构约定

本项目采用 [ADR 0001](docs/adr/0001-crate-structure-and-governance.md) 与 [ADR 0002](docs/adr/0002-codecs-transport-layering.md) 统一管理 crate 结构与边界：

- 新建 crate 需位于 `crates/`、`examples/` 或 `sdk/` 等约定目录，并遵守命名规范。
- 引入或调整依赖前必须检查现有实现，避免重复引入第三方库。
- Feature 必须提供文档并默认保持最小特性集。

## 契约引入矩阵与黑名单

- 任何调整契约的变更都必须对照 [`docs/ARCH-LAYERS.md`](docs/ARCH-LAYERS.md) 的分层矩阵执行自检。
- **黑名单速记**：
  - F-01：除 `spark-core` 外禁止声明 `CoreError`、`RequestId`、`Budget` 等契约类型；
  - F-02：业务代码不得 `pub use spark_core::contract::*`，需改用 `spark_core::prelude` 或显式列出导出符号；
  - F-03：传输实现必须复用 `spark_core::protocol::{Message, Frame, Event}`；
  - F-04：配置解析必须通过 `spark_core::config` 构造 `Timeout` 与 `TimeoutProfile`；
- CI 会通过 `rg` 和契约测试验证以上规则，请在提交前自行确认。

## 开发流程

1. Fork 仓库并创建主题分支。
2. 在对应目录中进行修改，遵循项目的编码规范和文档结构。
3. 提交前执行完整检查（见下文“质量门槛”）。
4. 提交 PR，模板中应包含：
   - 变更摘要
   - 关联 Issue 或任务
   - 是否引入新依赖及理由
   - 测试与验证结果

## 代码与文档要求

- 严格遵守“教案级”注释标准：
  - 说明意图、逻辑、契约、设计权衡和潜在风险。
  - 注释需保证独立可理解性并与代码保持同步。
- 保持命名、模块划分与项目现有风格一致。
- 文档更新需提供足够背景信息，确保新贡献者可快速理解。

## 质量门槛

在提交 PR 前，必须确保以下命令全部通过（若因平台限制无法执行，请在 PR 中说明原因及替代验证方式）：

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace --no-default-features --features alloc
cargo doc --workspace
cargo bench --workspace -- --quick
cargo deny check advisories bans licenses sources
python3 tools/ci/check_docs_links.py
make ci-lints
make ci-zc-asm
make ci-no-std-alloc
make ci-doc-warning
make ci-bench-smoke
```

## 审查与合并

- 代码审查要求至少一位维护者批准。
- PR 必须引用相关 ADR 或设计文档，如涉及架构调整需先更新或新增 ADR。
- 审查期间可能要求补充测试、文档或调整实现，请保持响应。

## 发布与回滚

- 发布流程由维护团队执行，请勿在未经批准的情况下直接推送到 `main`。
- 如遇紧急问题，需要回滚时务必在 PR 中说明影响范围与后续补救计划。

感谢你的贡献！
