# Spark 2026 十大常见陷阱

> 将真实排障经验凝练为十条“避坑卡片”，帮助团队在最短时间内对齐心智模型。每条卡片都包含现象、根因、快速自检与进阶建议。

## 1. 忽视 Rust 工具链版本
- **现象：** `cargo check` 提示语法错误或编译器特性不可用。
- **根因：** 系统默认 Rust 版本低于仓库要求的 `1.89.0`。
- **自检：** 执行 `rustc --version`，确认版本与 `rust-toolchain.toml` 一致。
- **进阶建议：** 使用 `rustup override set stable` 将工作目录锁定到稳定版本。

## 2. 未启用 `alloc` 功能
- **现象：** `cargo build --no-default-features` 时出现大量 `alloc` 相关类型缺失。
- **根因：** `spark-core` 默认依赖 `alloc`；关闭默认特性后需手动开启。
- **自检：** 运行 `cargo build --workspace --no-default-features --features alloc` 验证。
- **进阶建议：** 在 CI 中保留该命令，提前发现 `no_std` 场景兼容性问题。

## 3. 忽略缓冲池契约
- **现象：** 自研缓冲池在高并发下出现越界或未释放内存。
- **根因：** 未完整实现 `WritableBuffer`/`ReadableBuffer` 的约束（例如 `split_to` 未更新指针）。
- **自检：** 对照 `crates/spark-codec-line/examples/minimal.rs` 中的 Mock 实现，检查每个接口的前置/后置条件。
- **进阶建议：** 在单元测试中引入 `DecodeContext::check_frame_constraints`，模拟预算压力。

## 4. 漏跑核心质量检查
- **现象：** 本地调试通过，CI 因格式或 Lint 失败。
- **根因：** 未运行 `cargo fmt`/`cargo clippy`/`make ci-*` 等命令。
- **自检：** 在提交前执行 `make ci-lints`，快速聚合格式化与 Clippy 检查。
- **进阶建议：** 配置 Git Hooks（如 `pre-commit`）自动触发。

## 5. 配置源循环依赖
- **现象：** `ConfigurationBuilder::build` 返回 `BuildErrorKind::Validation`。
- **根因：** `ProfileDescriptor` 的 `extends` 字段存在自引用或重复条目。
- **自检：** 查看 `BuildReport` 中的 `profile.extends` 检查记录。
- **进阶建议：** 在配置治理平台接入拓扑检测，提前阻断循环。

## 6. 审计流水线未注入
- **现象：** 运行期无法追踪配置变更来源。
- **根因：** 构建 `ConfigurationBuilder` 时遗漏 `with_audit_pipeline`。
- **自检：** 打印 `BuildOutcome.report`，确认 `audit` 条目存在。
- **进阶建议：** 将 `AuditPipeline` 的配置作为必填项写入团队上手 checklist。

## 7. 编解码预算未配置
- **现象：** 遇到特定输入时 `codec.decode` 返回 `protocol.budget_exceeded`。
- **根因：** 未根据实际业务帧长设置 `EncodeContext`/`DecodeContext` 的 `max_frame_size` 或绑定 `Budget`。
- **自检：** 查阅 `crates/spark-codec-line/examples/minimal.rs`，确认样例如何设置预算并回滚消费。
- **进阶建议：** 在生产环境将预算与 `SLO` 指标联动，实现动态调节。

## 8. 忽视运行时观测性
- **现象：** 发生背压或降级时无指标、无日志可查。
- **根因：** 未通过 `CoreServices::observability_facade` 暴露统一观测接口。
- **自检：** 检查各 Handler 是否注入 `spark_otel::facade::DefaultObservabilityFacade`。
- **进阶建议：** 在集成测试中模拟限流事件，确认指标与日志完整。

## 9. 传输协商矩阵未同步
- **现象：** 新增传输实现后握手失败。
- **根因：** 未更新 `docs/dual-layer-interface-matrix.md` 与 `transport::Capability` 组合。
- **自检：** 对照矩阵中的能力位，确认双方约定一致。
- **进阶建议：** 在 PR 模板中加入“是否更新矩阵”检查项。

## 10. 忘记同步文档与样例
- **现象：** 新人按照 README 或教程操作仍然失败。
- **根因：** 文档未覆盖最新接口或样例未更新。
- **自检：** 在每次迭代前手动跑一遍 `docs/getting-started.md` 的步骤。
- **进阶建议：** 将文档检查纳入版本发布清单，并在 CI 中加上示例运行脚本。

> 建议在团队 Onboarding 培训中直接演示一次「最小例 + 十大陷阱」，帮助新成员建立正确心智模型。
