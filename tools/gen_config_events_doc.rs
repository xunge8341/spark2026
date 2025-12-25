//! 配置事件契约文档生成器：根据 `contracts/config_events.toml` 产出 Markdown 说明。
//!
//! # 教案式说明（Why）
//! - 将事件契约的单一事实来源（SOT）转换为可阅读的文档，确保控制面、运行面与审计面在评审时拥有同一份权威资料；
//! - 杜绝手写文档与代码漂移，通过自动化生成保持字段、审计映射与演练用例的一致性。
//!
//! # 契约定义（What）
//! - 输入：仓库根目录下的 `contracts/config_events.toml`；
//! - 输出：`docs/configuration-events.md`，涵盖事件列表、字段说明、审计映射与演练步骤；
//! - 特性开关：需启用 `std` 与 `configuration_event_doc` 以访问文件系统与 `toml` 解析。
//!
//! # 实现方式（How）
//! - 使用 [`serde`] 解析合约结构；
//! - 根据事件与结构体声明构建 Markdown 表格与段落；
//! - 写入目标文档时覆盖旧内容，保证每次执行后文档与合约完全同步。

use std::{env, fs, path::PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ConfigEventsContract {
    version: String,
    summary: String,
    #[serde(default)]
    structs: Vec<EventStructSpec>,
    events: Vec<ConfigEventSpec>,
}

#[derive(Debug, Deserialize)]
struct EventStructSpec {
    ident: String,
    summary: String,
    rationale: String,
    description: String,
    #[serde(default)]
    fields: Vec<EventFieldSpec>,
}

#[derive(Debug, Deserialize)]
struct EventFieldSpec {
    name: String,
    required: bool,
    #[serde(rename = "type")]
    ty: EventFieldTypeSpec,
    doc: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum EventFieldTypeSpec {
    String,
    U64,
    Bool,
    Struct { ident: String },
    List { item: Box<EventFieldTypeSpec> },
}

#[derive(Debug, Deserialize)]
struct ConfigEventSpec {
    ident: String,
    code: String,
    family: String,
    name: String,
    severity: String,
    summary: String,
    description: String,
    audit: EventAuditSpec,
    #[serde(default)]
    fields: Vec<EventFieldSpec>,
    #[serde(default)]
    drills: Vec<EventDrillSpec>,
}

#[derive(Debug, Deserialize)]
struct EventAuditSpec {
    action: String,
    entity_kind: String,
    entity_id_field: String,
}

#[derive(Debug, Deserialize)]
struct EventDrillSpec {
    title: String,
    goal: String,
    #[serde(default)]
    setup: Vec<String>,
    #[serde(default)]
    steps: Vec<String>,
    #[serde(default)]
    expectations: Vec<String>,
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let contract_path = manifest_dir.join("../../contracts/config_events.toml");
    let doc_path = manifest_dir.join("../../docs/configuration-events.md");

    let raw = fs::read_to_string(&contract_path)
        .unwrap_or_else(|err| panic!("读取 {:?} 失败: {}", contract_path, err));
    let contract: ConfigEventsContract =
        toml::from_str(&raw).unwrap_or_else(|err| panic!("解析 {:?} 失败: {}", contract_path, err));
    let markdown = render_markdown(&contract);
    fs::write(&doc_path, markdown).unwrap_or_else(|err| {
        panic!("写入 {:?} 失败: {}", doc_path, err);
    });
}

fn render_markdown(contract: &ConfigEventsContract) -> String {
    let mut buf = String::new();
    buf.push_str("# 配置事件契约\n\n");
    buf.push_str(&format!("- 合约版本：`{}`。\n", contract.version));
    buf.push_str(&format!("- 总览：{}\n", contract.summary));
    buf.push_str("- 单一事实来源（SOT）：`contracts/config_events.toml`。\n");
    buf.push_str("- 生成产物：`crates/spark-core/src/governance/configuration/events.rs`、`docs/configuration-events.md`。\n\n");

    buf.push_str("## 事件总览\n\n");
    buf.push_str("| 事件代码 | 名称 | 家族 | 严重性 | 结构体 |\n");
    buf.push_str("| --- | --- | --- | --- | --- |\n");
    for event in &contract.events {
        buf.push_str(&format!(
            "| `{}` | {} | `{}` | `{}` | `{}` |\n",
            event.code, event.name, event.family, event.severity, event.ident
        ));
    }
    buf.push('\n');

    if !contract.structs.is_empty() {
        buf.push_str("## 复用结构体\n\n");
        for st in &contract.structs {
            buf.push_str(&format!("### `{}`\n", st.ident));
            buf.push_str(&format!("- **Why**：{}\n", st.summary));
            buf.push_str(&format!("- **Rationale**：{}\n", st.rationale));
            buf.push_str(&format!("- **How**：{}\n", st.description));
            buf.push_str("- **字段**：\n");
            buf.push_str("  | 字段 | 类型 | 必填 | 说明 |\n");
            buf.push_str("  | --- | --- | --- | --- |\n");
            for field in &st.fields {
                buf.push_str(&format!(
                    "  | `{}` | `{}` | `{}` | {} |\n",
                    field.name,
                    field_type_name(&field.ty),
                    if field.required { "是" } else { "否" },
                    field.doc
                ));
            }
            buf.push('\n');
        }
    }

    buf.push_str("## 事件详情\n\n");
    for event in &contract.events {
        buf.push_str(&format!("### `{}` — {}\n\n", event.code, event.name));
        buf.push_str(&format!("**Why**：{}\n\n", event.summary));
        buf.push_str(&format!("**What**：{}\n\n", event.description));
        buf.push_str("**契约要点**：\n");
        buf.push_str(&format!("- 结构体：`{}`。\n", event.ident));
        buf.push_str(&format!(
            "- 审计映射：action=`{}`，entity_kind=`{}`，entity_id_field=`{}`。\n",
            event.audit.action, event.audit.entity_kind, event.audit.entity_id_field
        ));
        buf.push_str("- 字段列表：\n");
        buf.push_str("  | 字段 | 类型 | 必填 | 说明 |\n");
        buf.push_str("  | --- | --- | --- | --- |\n");
        for field in &event.fields {
            buf.push_str(&format!(
                "  | `{}` | `{}` | `{}` | {} |\n",
                field.name,
                field_type_name(&field.ty),
                if field.required { "是" } else { "否" },
                field.doc
            ));
        }
        buf.push('\n');

        if !event.drills.is_empty() {
            buf.push_str("**演练用例**：\n\n");
            for drill in &event.drills {
                buf.push_str(&format!("- 用例：{}\n", drill.title));
                buf.push_str(&format!("  - 目标：{}\n", drill.goal));
                if !drill.setup.is_empty() {
                    buf.push_str("  - 准备：\n");
                    for step in &drill.setup {
                        buf.push_str(&format!("    - {}\n", step));
                    }
                }
                if !drill.steps.is_empty() {
                    buf.push_str("  - 步骤：\n");
                    for step in &drill.steps {
                        buf.push_str(&format!("    1. {}\n", step));
                    }
                }
                if !drill.expectations.is_empty() {
                    buf.push_str("  - 验收：\n");
                    for exp in &drill.expectations {
                        buf.push_str(&format!("    - {}\n", exp));
                    }
                }
                buf.push('\n');
            }
        }
    }

    buf
}

fn field_type_name(spec: &EventFieldTypeSpec) -> String {
    match spec {
        EventFieldTypeSpec::String => "String".to_string(),
        EventFieldTypeSpec::U64 => "u64".to_string(),
        EventFieldTypeSpec::Bool => "bool".to_string(),
        EventFieldTypeSpec::Struct { ident } => ident.clone(),
        EventFieldTypeSpec::List { item } => format!("Vec<{}>", field_type_name(item)),
    }
}
