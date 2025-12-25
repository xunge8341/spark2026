# Feature 策略与命名规范

> 适用范围：`spark-core`、`crates/spark-transport-*` 以及依赖这些契约的实现层 crate。

## 背景（Why）
- 框架需要同时覆盖 `std` 与 `no_std + alloc` 场景，若默认启用重量级运行时会造成最小化构建失败。
- 传输实现未来可能扩展至多种执行环境（Tokio、Mio、自研 runtime 等），统一的 feature 命名便于选择实现层。
- 采用“默认安全 / 最小化”策略能够缩短 CI 组合矩阵的构建时间，并让使用方明确开启哪些可选能力。

## 统一约定（What）
1. **最小默认集**：除实现层必须的依赖外，所有 crate 默认只启用维持契约所需的最小功能。
2. **运行时命名**：涉及异步运行时的实现层统一使用 `runtime-*` 前缀，例如 `runtime-tokio`。
3. **向上游透传**：当实现层需要依赖下游 crate 的运行时特性时，feature 必须显式透传（例如 TLS 对 TCP 的转发）。
4. **文档说明**：每个实现层 crate 顶部文档需说明禁用 `runtime-*` 后的行为（仅保留契约、跳过实际实现）。

## spark-core 的 feature 集（How）
| Feature | 默认 | 作用 | 说明 |
| --- | --- | --- | --- |
| `alloc` | ✅ | 启用 `Box`/`Arc`/`Vec` 等堆分配类型 | 必选；禁用会触发编译期错误，提醒调用方显式启用 |
| `std` | ⛔ | 提供 `std` 环境下的时间、IO、线程等扩展 | 自动依赖 `alloc`，并 gate 所有仅能在 `std` 上工作的模块 |
| `loom-model` | ⛔ | 启用 `loom` 并行模型测试 | 依赖 `std`，仅在并发模型验证时打开 |
| `std_json` / `error_contract_doc` 等 | ⛔ | 生成契约文档所需的 JSON / TOML 支持 | 统一依赖 `std` 与相应可选依赖 |

## 传输实现层（Transport）的 feature 集
| Crate | 默认 feature | 说明 |
| --- | --- | --- |
| `spark-transport-tcp` | `runtime-tokio` | Tokio TCP 通道；禁用后仅保留文档与类型别名 |
| `spark-transport-tls` | `runtime-tokio` | Tokio + rustls 封装，并透传至 TCP 的 Tokio 实现 |
| `spark-transport-quic` | `runtime-tokio` | 基于 Quinn 的 QUIC 通道；禁用后停用全部实现代码 |
| `spark-transport-udp` | `runtime-tokio` | Tokio UDP 与可选 `batch-udp-unix` 优化 |

所有运行时实现均将 Tokio 相关依赖标记为可选，`runtime-tokio` 关闭时不会触发 Tokio/Quinn 等构建，便于执行 `--no-default-features` 的最小化编译。

## 使用示例（Contract）
- **最小 `no_std + alloc`**：`cargo build --workspace --no-default-features --features alloc`
- **启用特定实现**：
  ```toml
  spark-transport-tcp = { version = "0.1", default-features = false, features = ["runtime-tokio"] }
  ```
- **组合优化特性**：
  ```toml
  spark-transport-udp = { version = "0.1", default-features = false, features = ["runtime-tokio", "batch-udp-unix"] }
  ```

## 风险与后续
- 当前仅提供 Tokio 实现；未来增加 Mio/自研运行时时，需要复用 `runtime-*` 前缀并补充互斥关系。
- 当 feature 组合变化时，需同步更新 CI 构建矩阵，确保 `--no-default-features`、`alloc`、`runtime-*` 等组合全部通过。
