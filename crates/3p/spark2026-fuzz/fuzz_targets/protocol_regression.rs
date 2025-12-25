#![no_main]

use libfuzzer_sys::fuzz_target;
use spark2026_fuzz::execute_protocol_script;

// 针对 LINE/RTP/SIP/SDP 四类协议的极限样本回归入口。
//
// - **Why**：统一在同一 target 内执行手工维护的极限脚本，确保 fuzz 回归与 CI 验证共享逻辑。
// - **How**：LibFuzzer 传入的字节流会被解析为脚本文本，再调用 `execute_protocol_script` 完成实际解码。
// - **What**：输入任意字节即可，无法解析的脚本会被忽略，从而允许 fuzzer 扩大搜索空间。
fuzz_target!(|data: &[u8]| {
    execute_protocol_script(data);
});
