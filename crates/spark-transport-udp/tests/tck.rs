//! spark-transport-udp 通过 `spark-tck` 断言自身实现是否满足核心契约。
//!
//! # 教案式说明
//! - **Why**：一旦 UDP 传输层实现发生变更，该测试可在 CI 中自动重放 TCK，阻止违反绑定、返回路由
//!   或批量 IO 契约的回归。
//! - **How**：直接调用 `spark-tck` 暴露的断言函数，让 TCK 维护复杂逻辑，本地仅需提供 Tokio 运行时。
//! - **What**：每个测试返回 `()`；若断言失败则 panic 并附带阶段性上下文信息。

use spark_tck::udp;

/// 验证 `UdpEndpoint` 的最小收发闭环是否可用。
///
/// - **Why**：保证绑定与返回路由逻辑未被破坏。
/// - **How**：复用 `spark-tck` 的 `assert_bind_send_recv` 执行端到端 ping/pong。
/// - **What**：若断言失败，`expect` 将直接显示 TCK 给出的上下文信息。
#[tokio::test(flavor = "multi_thread")]
async fn tck_udp_bind_send_recv() {
    udp::assert_bind_send_recv()
        .await
        .expect("UDP 烟囱断言失败：请检查绑定/收发逻辑");
}

/// 校验 `Via;rport` 回写契约，覆盖 NAT 穿越场景。
///
/// - **Why**：任何错误都会导致终端因响应不可达而掉线。
/// - **How**：TCK 内部模拟 `REGISTER` 请求并检查返回路由。
/// - **What**：失败时 panic，提示实现者校正 `return_route` 行为。
#[tokio::test(flavor = "multi_thread")]
async fn tck_udp_rport_round_trip() {
    udp::assert_rport_round_trip()
        .await
        .expect("Via;rport 未正确写回客户端端口");
}

/// 批量 IO 闭环验证，确保 scatter/gather 契约仍然成立。
///
/// - **Why**：批量路径是高并发场景的性能关键，回归容易导致丢包或顺序错误。
/// - **How**：调用 TCK 的 `assert_round_trip` 在批量接口上完成请求/响应回环。
/// - **What**：若 panic，请检查 `batch::recv_from`/`batch::send_to` 的实现与契约。
#[tokio::test(flavor = "multi_thread")]
async fn tck_udp_batch_round_trip() {
    udp::assert_batch_round_trip()
        .await
        .expect("UDP 批量 IO 套件断言失败");
}
