//! RTCP 相关的契约测试入口。
//!
//! # 教案定位（Why）
//! - 将 RTCP 解析的场景测试集中在独立模块，便于与传输、SIP 等子系统解耦。
//! - 通过子模块划分不同专题（例如复合包解析、错误处理），保持测试代码结构清晰。
//!
//! # 结构说明（What）
//! - `parse_compound`：验证 `spark-codec-rtcp` 对复合报文的解析能力与错误分类。
//! - `stats`：覆盖 Sender Report / Receiver Report 生成流程，锁定时间映射与异常分支。
//!
//! # 维护提示（How）
//! - 新增测试专题时，请同步在此处 `pub mod`，并为 README/文档补充上下文说明。

pub mod parse_compound;
pub mod stats;
