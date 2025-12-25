# LineDelimitedCodec 扩展示例

> 目标：演示如何在不修改 `spark-core` 的前提下实现一个行分隔文本编解码扩展，并通过合约测试验证接口兼容性。

## 背景与定位

- **零核心改动**：本示例完全位于 `crates/spark-codec-line` 新增 crate 内，仅依赖 `spark-core` 公开 API；
- **学习模板**：涵盖 `Codec` trait 实现、预算控制、错误码选型与缓冲池交互，为后续扩展（如自定义 Transport/Middleware）提供参照；
- **部署场景**：适用于日志流、命令行协议等“行文本”模型，可直接注册至 `CodecRegistry`。

## 编解码流程

1. **编码阶段**
   - 从 `EncodeContext` 租借缓冲，校验 `max_frame_size` 预算；
   - 写入业务字符串与换行符，冻结为 `EncodedPayload` 返回；
   - 超出预算时抛出 `protocol.budget_exceeded`，以便调用方执行降级或背压。
2. **解码阶段**
   - 检查首个连续缓冲块，查找换行符；
   - 命中时通过 `split_to` + `String::from_utf8` 生成业务字符串；
   - 缺失换行符返回 `DecodeOutcome::Incomplete`，等待更多字节；
   - UTF-8 解析失败返回 `protocol.decode`，带出详细原因。

## 合约测试

`cargo test -p spark-codec-line` 将运行以下关键用例：

- **编码换行**：确保编码结果以 `\n` 结尾且缓冲正确冻结；
- **解码成功**：验证能从连续缓冲中解析首行并保留剩余数据；
- **缺行等待**：在无换行符时返回 `Incomplete` 且不消费缓冲；
- **预算约束**：模拟帧长度超限，断言错误码为 `protocol.budget_exceeded`。

## 与核心契约的衔接

- 描述符采用 `text/plain; charset=utf-8` + `identity`，可直接参与运行时协商；
- 错误码遵循 `spark_core::error::codes` 约定，保证日志与监控体系识别；
- 依赖 `alloc` 即可构建，满足 `cargo build --workspace --no-default-features --features alloc` 要求。

## 后续拓展建议

- 若需要支持多行或二进制负载，可在本示例基础上引入长度前缀或转义机制；
- 可结合 `CodecMetricsHook` 打点，衡量不同消息长度下的吞吐与错误率；
- 与 `Transport` 扩展组合时，建议新增集成测试验证从字节流到业务对象的端到端路径。
