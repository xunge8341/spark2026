//! 取消/截止传播 SLO 基准二进制入口。
//!
//! # 说明
//! - 主体逻辑位于 `tools/bench/cancel_deadline_slo.rs`，此处仅做入口与错误处理；
//! - 之所以放在 `spark-core` 二进制中，是为了复用 crate 内部类型与语法糖实现。

#[path = "../../../../tools/bench/cancel_deadline_slo.rs"]
mod cancel_deadline_slo;

fn main() {
    if let Err(error) = cancel_deadline_slo::run() {
        eprintln!("取消/截止 SLO 基准失败: {error}");
        std::process::exit(1);
    }
}
