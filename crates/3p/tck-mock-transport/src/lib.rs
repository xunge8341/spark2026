//! 模拟传输层实现：示范如何搭建最低可行的 `spark-core` 集成环境。
//!
//! # 教案式综述
//! - **意图 (Why)**：为第三方团队提供最小示例，说明在不接入真实网络协议的情况下，如何组合 `spark-core`
//!   的构件并运行官方 TCK，验证内建契约。此示例强调“架构骨架”而非性能细节。
//! - **结构 (How)**：
//!   1. `MockTransport` 仅实现极简的请求-响应循环，通过 `Vec` 缓存请求以便测试注入断言；
//!   2. `run_roundtrip` 暴露可编程入口，将 `spark-core` 的服务抽象绑定到模拟传输层，允许 TCK 案例驱动；
//!   3. 所有状态均驻留在 `Arc` 管理的共享结构，便于并发测试通过。
//! - **契约 (What)**：
//!   - **前置条件**：调用方须在标准库环境下构建（`std` 特性）；
//!   - **输入**：一组实现 `MockService` 的业务处理器，用于响应框架生成的 [`PipelineMessage`] 请求；
//!   - **输出**：`MockTransport` 返回成功发送/接收的次数，用于断言交互完整性；
//!   - **后置条件**：所有暂存请求会在函数返回前清理，避免跨测试污染。
//! - **设计考量 (Trade-offs)**：示例刻意保持朴素实现，牺牲了吞吐与容错，但换来易读性与易于在课堂演示中扩展；
//!   若要演进为真实传输层，可替换队列实现并接入异步网络堆栈。

use spark_core::buffer::PipelineMessage;
use std::sync::{Arc, Mutex};

/// `MockService` 定义：抽象出 TCK 所需的最小请求处理接口。
///
/// # 教案式说明
/// - **意图 (Why)**：在演示项目中将业务逻辑与传输层解耦，便于直接替换为真实服务实现。
/// - **逻辑 (How)**：使用一个方法 `handle` 获取完整的 [`PipelineMessage`] 所有权并返回响应；可由闭包或结构体实现。
/// - **契约 (What)**：
///   - **输入**：[`PipelineMessage`] 为 `spark-core` 统一封装的缓冲/业务消息体，可携带字节或用户对象；
///   - **输出**：返回一个新的 [`PipelineMessage`]，描述应答内容；
///   - **前置条件**：调用方需保证请求已通过框架校验；
///   - **后置条件**：响应应遵循协议约束（如序列号、上下文字段保持一致）。
pub trait MockService: Send + Sync + 'static {
    fn handle(&self, request: PipelineMessage) -> PipelineMessage;
}

/// `MockTransport`：以内存缓冲模拟网络双工通道。
///
/// # 教案式说明
/// - **意图 (Why)**：为 TCK 提供可控的消息往返环境，不依赖操作系统套接字即可重放复杂场景。
/// - **逻辑 (How)**：内部维护一个互斥保护的 `Vec` 作为待处理请求队列，`send` 负责入队，`drain` 负责与服务交互并
///   生成响应集。
/// - **契约 (What)**：
///   - `send` 的前置条件是调用线程持有要发送的合法消息；后置条件是队列长度增加；
///   - `drain` 的前置条件是服务实现无 `panic`；后置条件是返回的响应数量与已出队请求一致。
/// - **风险提示**：互斥锁策略简单直接，但在高并发情况下存在抢占开销；示例场景中权衡易懂性优先。
#[derive(Default, Clone)]
pub struct MockTransport {
    inbox: Arc<Mutex<Vec<PipelineMessage>>>,
}

impl MockTransport {
    /// 将请求推入模拟传输层。
    ///
    /// # 契约说明
    /// - **输入**：外部构造的 [`PipelineMessage`]；
    /// - **前置条件**：消息必须符合框架的序列化规则；
    /// - **后置条件**：消息会被缓存等待 `drain` 消费。
    pub fn send(&self, message: PipelineMessage) {
        let mut inbox = self.inbox.lock().expect("锁应当可用");
        inbox.push(message);
    }

    /// 消费所有缓存请求并通过服务实现生成响应。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：将收集到的消息交给外部服务处理，并生成 TCK 可断言的响应集。
    /// - **逻辑 (How)**：
    ///   1. 获取队列所有权并逐条出队；
    ///   2. 调用服务的 `handle` 方法生成响应；
    ///   3. 将响应汇总返回，供测试进一步验证。
    /// - **契约 (What)**：
    ///   - **输入**：实现 `MockService` 的处理器；
    ///   - **输出**：成功响应的向量；
    ///   - **前置条件**：处理器不能返回非法消息（否则 TCK 断言会失败）；
    ///   - **后置条件**：内部队列被清空。
    pub fn drain<S>(&self, service: &S) -> Vec<PipelineMessage>
    where
        S: MockService,
    {
        let mut inbox = self.inbox.lock().expect("锁应当可用");
        let mut responses = Vec::with_capacity(inbox.len());
        for request in inbox.drain(..) {
            responses.push(service.handle(request));
        }
        responses
    }
}

/// 运行一次简单的“请求-响应”循环，辅助课堂演示。
///
/// # 教案式说明
/// - **意图 (Why)**：将 `MockTransport` 的使用步骤封装为单个函数，降低示例调用复杂度。
/// - **逻辑 (How)**：
///   1. 对输入的可迭代请求逐条调用 `send`；
///   2. 调用 `drain` 获取响应；
///   3. 返回响应数量，便于 TCK 或演示代码快速断言。
/// - **契约 (What)**：
///   - **输入**：传输层实例、服务实现、可迭代的 [`PipelineMessage`] 集合；
///   - **输出**：响应条目数；
///   - **前置条件**：迭代集合中的消息必须满足协议；
///   - **后置条件**：所有请求都已被处理并返回结果。
/// - **风险提示**：若服务实现内部 `panic`，函数会直接传播；课堂示例可借此说明如何调试失败用例。
pub fn run_roundtrip<S, I>(transport: &MockTransport, service: &S, requests: I) -> usize
where
    S: MockService,
    I: IntoIterator<Item = PipelineMessage>,
{
    for request in requests {
        transport.send(request);
    }
    transport.drain(service).len()
}
