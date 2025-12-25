//! 使用 `spark-tck` 对 SDP Offer/Answer 协商进行行为与负例回归。
//!
//! # 教案式说明
//! - **Why**：确保 `spark-codec-sdp` 在核心协商分支（PCMU/PCMA、DTMF）上的行为与负例契约不会被回归。
//! - **How**：直接调用 `spark-tck::sdp::offer_answer` 暴露的断言函数，由 TCK 维护详细校验逻辑。
//! - **What**：若断言失败，将 panic 并给出具体字段对比，提示实现者修复。

/// 当 offer 同时提供 PCMU/PCMA 时，应优先返回本地偏好的 PCMU。
#[test]
fn tck_offer_answer_basic_pcm() {
    spark_tck::sdp::offer_answer::assert_basic_pcm();
}

/// 当本地开启 DTMF 能力时，应成功协商 `telephone-event` 相关属性。
#[test]
fn tck_offer_answer_negotiate_dtmf() {
    spark_tck::sdp::offer_answer::assert_negotiate_dtmf();
}
