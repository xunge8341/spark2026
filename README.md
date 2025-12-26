# Spark 2026

[📘 最小上手路径](docs/getting-started.md) · [⚠️ 十大常见陷阱](docs/pitfalls.md)

Spark 2026 是面向高性能、协议无关场景的异步通信契约集合，覆盖核心接口、传输实现、编解码器与观测插件。通过统一的状态机与错误分类，我们可以在 `no_std + alloc` 环境下构建跨协议的可靠通信栈。

## 仓库索引

| 层级 | 入口 | 说明 |
| --- | --- | --- |
| **核心契约** | [`crates/spark-core`](crates/spark-core/README.md) | 定义 `CallContext`、`ReadyState`、`CoreError` 等基础类型，并仅保留 `TraceId`/`SpanId`/`AuditTag`/`ResourceAttr` 等纯观测契约，是所有实现与测试的锚点。 |
| **过程宏** | [`crates/spark-macros`](crates/spark-macros/README.md) | 提供 `#[spark::service]` 等宏，自动生成符合契约的 `Service` 实现。 |
| **契约测试** | [`crates/spark-contract-tests`](crates/spark-contract-tests/README.md) | 汇总 ReadyState、错误分类、半关闭等主题化测试；[`crates/spark-contract-tests-macros`](crates/spark-contract-tests-macros/README.md) 提供编译期套件注册。 |
| **实现 TCK** | [`crates/spark-tck`](crates/spark-tck/README.md) | 在真实 TCP/TLS/QUIC/UDP 环境执行合规检查，验证传输实现遵守契约。 |
| **可观测性** | [`crates/spark-otel`](crates/spark-otel/README.md) | 将状态机与错误域映射到 OpenTelemetry，提供链路追踪与指标输出。 |
| **传输层** | [`crates/spark-transport-tcp`](crates/spark-transport-tcp/README.md) · [`tls`](crates/spark-transport-tls/README.md) · [`quic`](crates/spark-transport-quic/README.md) · [`udp`](crates/spark-transport-udp/README.md) | 提供不同网络介质上的通道实现，统一 ReadyState 与关闭语义。 |
| **编解码器** | [`crates/spark-codec-line`](crates/spark-codec-line/README.md) · [`rtp`](crates/spark-codec-rtp/README.md) · [`rtcp`](crates/spark-codec-rtcp/README.md) · [`sdp`](crates/spark-codec-sdp/README.md) · [`sip`](crates/spark-codec-sip/README.md) | 演示文本、媒体与信令协议在统一契约下的实现方式。 |

更多设计背景与治理策略可参考：

- 架构综述：[docs/global-architecture.md](docs/global-architecture.md)
- 状态机矩阵：[docs/state_machines.md](docs/state_machines.md)
- 错误分类矩阵：[docs/error-category-matrix.md](docs/error-category-matrix.md)
- 优雅关闭契约：[docs/graceful-shutdown-contract.md](docs/graceful-shutdown-contract.md)
- 观测与指标：[docs/observability](docs/observability)
- Feature 策略：[docs/feature-policy.md](docs/feature-policy.md)

## 快速开始

1. 安装 Rust 1.89（`rustup` 将根据 `rust-toolchain.toml` 自动选择版本）。
2. 运行 `cargo check --workspace` 确认依赖齐全。
3. 按照 [最小上手路径](docs/getting-started.md) 执行 `cargo run -p spark-codec-line --example minimal`，体验端到端往返流程。
4. 阅读 [十大常见陷阱](docs/pitfalls.md)，避免在生产化过程中踩坑。

## 贡献指南

- 在提交前确保本地通过以下命令：
  ```bash
  cargo fmt --all --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  ```
- 若涉及文档或契约更新，请同步维护 `docs/` 目录与相关样例。
- 遇到问题可在 Issue 中附带复现步骤、命令输出与环境信息，便于定位。

欢迎贡献新扩展、运行时实现或经验总结！
