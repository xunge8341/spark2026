//! 审计回放工具：读取 `AuditEventV1` JSON 日志，验证哈希链并重建配置快照。
//!
//! # 使用方法
//! ```bash
//! cargo run --bin audit_replay -- audit.log --baseline baseline.json --output final.json \
//!     --gap-report replay-gap.json
//! ```
//! - `audit.log`：逐行存放的 `AuditEventV1` JSON（通常来自生产环境导出）。
//! - `--baseline`：可选，初始状态（JSON 数组，元素为 `AuditChangeEntry`）。
//! - `--output`：可选，回放后的最终状态写入目标文件。
//! - `--gap-report`：可选，当发现哈希链断裂时生成重放请求描述文件，便于向上游触发重发。
//!
//! # 设计要点（Why）
//! - 与库内 [`AuditStateHasher`] 保持一致的哈希算法，确保链路校验一致。
//! - 回放过程中一旦发现 `state_prev_hash` 不匹配，立即终止并输出 gap 报告，满足“检测与重发策略”的验收要求。
//! - 依赖 `serde_json` 实现脚本化调用，可方便集成到 CI/CD 或应急演练脚本中。

use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;

use serde_json::json;

use spark_core::{
    AuditChangeEntry, AuditChangeSet, AuditEventV1, AuditStateHasher, ConfigKey, ConfigValue,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("审计回放失败: {error}");
        std::process::exit(1);
    }
}

fn run() -> spark_core::Result<(), String> {
    let mut raw_args: Vec<String> = env::args().skip(1).collect();
    raw_args.retain(|arg| arg.trim_start_matches('-') != "quick");
    let mut args = raw_args.into_iter();

    let log_path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| usage("缺少审计日志路径"))?;

    let mut baseline_path = None;
    let mut output_path = None;
    let mut gap_report_path = None;

    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--baseline" => {
                let value = args
                    .next()
                    .ok_or_else(|| usage("--baseline 之后必须提供文件路径"))?;
                baseline_path = Some(PathBuf::from(value));
            }
            "--output" => {
                let value = args
                    .next()
                    .ok_or_else(|| usage("--output 之后必须提供文件路径"))?;
                output_path = Some(PathBuf::from(value));
            }
            "--gap-report" => {
                let value = args
                    .next()
                    .ok_or_else(|| usage("--gap-report 之后必须提供文件路径"))?;
                gap_report_path = Some(PathBuf::from(value));
            }
            "quick" => continue,
            flag if flag.trim_start_matches('-') == "quick" => continue,
            unknown => {
                return Err(usage(&format!("未知参数: {unknown}")));
            }
        }
    }

    let mut state = if let Some(path) = baseline_path {
        load_state(&path).map_err(|error| format!("读取基线失败: {error}"))?
    } else {
        BTreeMap::new()
    };

    replay_log(&log_path, &mut state, gap_report_path.as_ref())?;

    if let Some(path) = output_path {
        write_state(&path, &state).map_err(|error| format!("写入输出失败: {error}"))?;
    }

    Ok(())
}

fn replay_log(
    log_path: &PathBuf,
    state: &mut BTreeMap<ConfigKey, ConfigValue>,
    gap_report: Option<&PathBuf>,
) -> spark_core::Result<(), String> {
    let file = File::open(log_path).map_err(|error| format!("打开日志失败: {error}"))?;
    let reader = BufReader::new(file);

    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|error| format!("读取日志第 {} 行失败: {error}", index + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let event: AuditEventV1 = serde_json::from_str(trimmed)
            .map_err(|error| format!("解析第 {} 行 JSON 失败: {error}", index + 1))?;

        let current_hash = AuditStateHasher::hash_configuration(state.iter());
        if current_hash != event.state_prev_hash {
            if let Some(path) = gap_report {
                write_gap_report(path, &current_hash, &event)
                    .map_err(|error| format!("写入 gap 报告失败: {error}"))?;
            }
            return Err(format!(
                "第 {} 行检测到哈希链断裂：期望 {}, 实际 {}",
                index + 1,
                current_hash,
                event.state_prev_hash
            ));
        }

        apply_changes(state, &event.changes);
        let new_hash = AuditStateHasher::hash_configuration(state.iter());
        if new_hash != event.state_curr_hash {
            return Err(format!(
                "事件 {} 回放后哈希不匹配：生成 {}，日志记录 {}",
                event.event_id, new_hash, event.state_curr_hash
            ));
        }
    }

    Ok(())
}

fn apply_changes(state: &mut BTreeMap<ConfigKey, ConfigValue>, diff: &AuditChangeSet) {
    for entry in &diff.created {
        state.insert(entry.key.clone(), entry.value.clone());
    }
    for entry in &diff.updated {
        state.insert(entry.key.clone(), entry.value.clone());
    }
    for entry in &diff.deleted {
        state.remove(&entry.key);
    }
}

fn load_state(path: &PathBuf) -> spark_core::Result<BTreeMap<ConfigKey, ConfigValue>, io::Error> {
    let file = File::open(path)?;
    let entries: Vec<AuditChangeEntry> = serde_json::from_reader(file)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut state = BTreeMap::new();
    for entry in entries {
        state.insert(entry.key, entry.value);
    }
    Ok(state)
}

fn write_state(
    path: &PathBuf,
    state: &BTreeMap<ConfigKey, ConfigValue>,
) -> spark_core::Result<(), io::Error> {
    let entries: Vec<AuditChangeEntry> = state
        .iter()
        .map(|(key, value)| AuditChangeEntry {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();
    let mut file = File::create(path)?;
    let payload = serde_json::to_string_pretty(&entries)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    file.write_all(payload.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

fn write_gap_report(
    path: &PathBuf,
    expected_hash: &str,
    event: &AuditEventV1,
) -> spark_core::Result<(), io::Error> {
    let report = json!({
        "expected_prev_hash": expected_hash,
        "incoming_prev_hash": event.state_prev_hash,
        "event_id": event.event_id,
        "sequence": event.sequence,
        "occurred_at": event.occurred_at,
    });
    let mut file = File::create(path)?;
    let payload = serde_json::to_string_pretty(&report)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    file.write_all(payload.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

fn usage(detail: &str) -> String {
    format!(
        "{detail}\n用法: audit_replay <audit.log> [--baseline baseline.json] [--output final.json] [--gap-report gap.json]"
    )
}
