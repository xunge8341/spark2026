//! 回归测试：逐一重放 `protocol_regression` 语料，确保极限样本可被稳定解析。
//!
//! - **Why**：CI 中运行常规 `cargo test` 即可验证 fuzz 语料，无需依赖 libFuzzer 运行时。
//! - **How**：遍历语料目录，读取每个样本并调用 [`spark2026_fuzz::execute_protocol_script`]
//!   完成解码，若出现 panic 即视为回归。
//! - **What**：测试无返回值，只要执行完成即表示当前语料未触发崩溃。

use std::fs;
use std::path::PathBuf;

#[test]
fn replay_protocol_corpus() {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dir.push("corpus/protocol_regression");
    let entries = fs::read_dir(&dir).expect("protocol_regression corpus 应存在");
    for entry in entries {
        let entry = entry.expect("读取语料目录失败");
        if !entry.file_type().map(|kind| kind.is_file()).unwrap_or(false) {
            continue;
        }
        let path = entry.path();
        let data = fs::read(&path).expect("读取语料失败");
        spark2026_fuzz::execute_protocol_script(&data);
    }
}
