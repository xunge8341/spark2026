# BOOT-1 基线冻结追踪 Issue

> **状态**：进行中（初始基线已冻结，等待后续架构清理任务接入）

## 任务清单
- [x] 从 `main` 切出工作分支 `feat/arch-cleanup-p0`。
- [x] 生成 cargo metadata 快照并入库：`docs/reports/baseline/cargo-metadata.json`。
- [x] 生成依赖关系（cargo tree）快照并入库：`docs/reports/baseline/cargo-tree.txt`。
- [x] 生成公共 API 快照（含失败诊断占位符）并入库：`docs/reports/baseline/public-api/`。
  - 当前 `spark-router`（原 `spark-pipeline`）、`spark-transport-tcp` 与 `spark-transport-tls` 编译失败，已以 `FAILED` 占位并保留 stderr 日志供后续分析。
- [ ] 整理失败 crate 的修复计划（待后续架构清理任务接入）。

## 参考资料
- 基线产物目录：`docs/reports/baseline/`
- 生成命令：
  - `cargo metadata --format-version 1 --all-features`
  - `cargo tree --workspace --all-features`
  - `cargo +nightly public-api -p <crate> --simplified`

> 建议在 GitHub 上同步创建对应 issue，引用本文件作为任务描述与进展记录。
