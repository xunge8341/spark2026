# ADR 0002: Codecs 与 Transport 分层治理

## 背景与问题陈述
- **业务目标**：Spark 2026 需要在多种信令协议（SIP/SDP/RTP/RTCP/自研线路协议）与多种传输通道（TCP/UDP/TLS/QUIC）之间保持解耦，以支撑未来的网元扩展与安全合规审计。
- **历史包袱**：早期 PoC 阶段曾出现 Codec 与 Transport 交叉引用的尝试，导致协议解析层不可单独测试，也破坏了 `no_std + alloc` 的最小化构建。
- **风险**：
  1. 传输实现若直接依赖具体 Codec，将使得新协议上线必须改动底层网络栈，触发额外的安全审计。
  2. Codec 层若感知传输细节，则无法在 fuzz / contract 测试中进行独立验证，破坏“协议逻辑可单测”的原则。

## 决策
- **分层边界**：
  1. `crates/spark-codec-*` 仅依赖 `spark-core` 提供的公共契约，不得引用任何 `crates/spark-transport-*`。
  2. `crates/spark-transport-*` 仅与 `spark-core` 和运行时相关依赖（Tokio/Quinn/Rustls 等）耦合，禁止回指 codec 实现。
- **Feature 策略**：
  - Codec 层使用 `alloc/std/compat_v0` 等统一特性，覆盖 `no_std + alloc` 与兼容模式。
  - Transport 层仅通过 `runtime-*` 暴露运行时实现，关闭后应保留空壳契约，确保 `cargo build --no-default-features --features alloc` 可通过。
- **依赖治理**：
  - Codec 与 Transport 的跨层通信必须通过 `spark-core` 中定义的 trait/enum/typed event 完成。
  - 若未来需要共享工具（例如统计计数器），必须放入新的 `crates/shared/*` 并通过 ADR 更新治理边界。

## 推理过程（Why & How）
1. **契约统一**：所有协议解析结果都会落地到 `spark-core` 的 `Frame`/`Envelope` 等抽象类型。由 Codec 侧输出，Transport 侧消费，可保证双方独立演进。
2. **编译矩阵**：分层后我们可以在 CI 中执行：
   - `cargo build --workspace --no-default-features --features alloc`：验证 Codec 可在 `alloc` 场景单独编译；
   - `cargo build --workspace`：Transport 默认启用 Tokio/Quinn；
   - 通过禁用 Transport 的 `runtime-*` 特性，可在最小化环境下进行 Codec fuzzing。
3. **安全审计**：传输层常涉及加密库（Rustls、aws-lc-rs）。将其隔离后，Codec 模块在安全评估时无需重复审查 TLS/QUIC 依赖。

## 影响与权衡
- **收益**：
  - Contract Tests 可以对 Codec 做纯内存级的 roundtrip 验证，无需启动网络堆栈。
  - Transport 可以针对不同运行时（Tokio/Mio/自研）实现替换，而无需担心协议解析器的耦合。
- **成本**：
  - 部分共享工具（如统计标签、错误分类）必须上移到 `spark-core`，需要额外的封装工作。
  - 开发者在调试跨层问题时需要通过 `spark-core` 的类型追踪上下文，学习成本略有增加。

## 落地动作
1. 审查所有 `crates/spark-codec-*` 与 `crates/spark-transport-*` 的 `Cargo.toml`，确保依赖列表符合上述规则。
2. 在 `CONTRIBUTING.md` 中强调新增模块需遵循本 ADR；发现例外时需提交新的 ADR 说明理由。
3. 将分层检查纳入 `tools/ci/check_contract_sync.sh` 的扩展计划，后续通过脚本自动比对。

## 后续观察指标
- 每次新增 Codec 或 Transport 模块时，应在 PR 模板中引用本 ADR。
- CI 中的 `make ci-no-std-alloc` 与 `make ci-zc-asm` 必须持续通过，作为分层有效性的回归验证。

本 ADR 在 Gate-2 阶段正式生效，后续若需打破边界（例如引入零拷贝共享缓冲区），必须更新本文件并获得架构评审批准。
