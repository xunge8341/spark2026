//! SDP 协商相关的单元测试集合。
//!
//! ## 模块定位（Why）
//! - 作为 `spark-tck` 的内部测试入口，确保编解码协商能力在代码库内部即可验证；
//! - 与集成测试相比，单元测试能更快反馈 Offer/Answer 逻辑缺陷。

pub mod offer_answer;
