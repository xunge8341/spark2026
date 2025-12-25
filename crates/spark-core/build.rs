use std::{
    collections::BTreeMap,
    env,
    fmt::Write,
    fs,
    path::{Path, PathBuf},
};

use heck::ToShoutySnakeCase;
use serde::Deserialize;

#[path = "../../tools/error_matrix_contract.rs"]
mod error_matrix_contract;
use error_matrix_contract::{
    BudgetDispositionSpec, BusyDispositionSpec, CategoryTemplateSpec, ExpandedEntry,
    SecurityClassSpec, expand_entries, read_error_matrix_contract,
};

/// 构建脚本入口：读取 TOML 契约，生成 `crates/spark-core/src/error/generated/category_matrix.rs`。
///
/// # 教案式说明（Why）
/// - 避免手写静态矩阵导致的代码/文档漂移，将错误分类声明集中在 `contracts/error_matrix.toml`；
/// - 在编译阶段自动生成矩阵，实现“契约即代码”。
///
/// # 契约定义（What）
/// - 输入：仓库根目录下的 `contracts/error_matrix.toml`；
/// - 前置条件：文件内容符合 [`ErrorMatrixContract`] 的结构约束；
/// - 后置条件：在源码目录输出生成文件，供 `spark_core::error::category_matrix` 模块直接编译。
///
/// # 逻辑解析（How）
/// 1. 解析合约文件，展开多错误码行；
/// 2. 按错误码排序，确保 diff 稳定；
/// 3. 渲染 Rust 模块模板并写回磁盘。
fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let contract_path = manifest_dir.join("../../contracts/error_matrix.toml");
    println!("cargo:rerun-if-changed={}", contract_path.display());
    let contract = read_error_matrix_contract(&contract_path);
    let entries = expand_entries(&contract);
    let generated = render_category_matrix(&entries);
    let generated_dir = manifest_dir.join("src/error/generated");
    fs::create_dir_all(&generated_dir).expect("创建 error/generated 目录");
    let output_path = generated_dir.join("category_matrix.rs");
    fs::write(&output_path, generated).expect("写入 generated/category_matrix.rs");

    let observability_contract_path = manifest_dir.join("../../contracts/observability_keys.toml");
    println!(
        "cargo:rerun-if-changed={}",
        observability_contract_path.display()
    );
    let observability_contract = read_observability_keys_contract(&observability_contract_path);
    let observability_generated = render_observability_keys(&observability_contract);
    let observability_output_path = manifest_dir.join("src/governance/observability/keys.rs");
    fs::write(&observability_output_path, observability_generated)
        .expect("写入 governance/observability/keys.rs");

    let config_events_contract_path = manifest_dir.join("../../contracts/config_events.toml");
    println!(
        "cargo:rerun-if-changed={}",
        config_events_contract_path.display()
    );
    let config_events_contract = read_config_events_contract(&config_events_contract_path);
    let config_events_generated = render_config_events(&config_events_contract);
    let config_events_output_path = manifest_dir.join("src/governance/configuration/events.rs");
    fs::write(&config_events_output_path, config_events_generated)
        .expect("写入 governance/configuration/events.rs");
}

/// 可观测性键名合约的顶层结构：按分组列出键及其元数据。
#[derive(Debug, Deserialize)]
struct ObservabilityKeysContract {
    groups: Vec<KeyGroupSpec>,
}

/// 描述某一键名分组及其模块层级信息。
#[derive(Debug, Deserialize, Clone)]
struct KeyGroupSpec {
    path: Vec<String>,
    title: String,
    description: String,
    #[serde(default)]
    items: Vec<KeySpec>,
}

/// 单个键或标签枚举值的声明。
#[derive(Debug, Deserialize, Clone)]
struct KeySpec {
    ident: String,
    value: String,
    kind: KeyKind,
    #[serde(default)]
    usage: Vec<UsageDomain>,
    doc: String,
}

/// 键名的分类类型。
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum KeyKind {
    AttributeKey,
    LabelValue,
    LogField,
    TraceField,
}

impl KeyKind {
    /// 返回可读的中文标签，用于文档与注释输出。
    fn label(self) -> &'static str {
        match self {
            KeyKind::AttributeKey => "指标/日志键",
            KeyKind::LabelValue => "标签枚举值",
            KeyKind::LogField => "日志字段",
            KeyKind::TraceField => "追踪字段",
        }
    }
}

/// 键名适用的可观测性维度。
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum UsageDomain {
    Metrics,
    Logs,
    Tracing,
    Events,
}

impl UsageDomain {
    /// 返回中文标签，辅助输出“适用范围”。
    fn label(self) -> &'static str {
        match self {
            UsageDomain::Metrics => "指标",
            UsageDomain::Logs => "日志",
            UsageDomain::Tracing => "追踪",
            UsageDomain::Events => "运维事件",
        }
    }
}

/// 构建模块树时使用的中间结构。
#[derive(Default)]
struct ModuleNode {
    title: Option<String>,
    description: Option<String>,
    items: Vec<KeySpec>,
    children: BTreeMap<String, ModuleNode>,
}

/// 读取并解析可观测性键名合约。
///
/// # Why
/// - 集中读取 SOT 合约，避免在多个生成器之间重复解析逻辑；
/// - 解析失败时立即给出上下文，帮助开发者定位格式错误。
///
/// # What
/// - 输入：TOML 文件路径；输出：结构化的 [`ObservabilityKeysContract`]。
fn read_observability_keys_contract(path: &Path) -> ObservabilityKeysContract {
    let raw = fs::read_to_string(path).unwrap_or_else(|err| {
        panic!("读取 {path:?} 失败: {err}");
    });
    toml::from_str(&raw).unwrap_or_else(|err| {
        panic!("解析 {path:?} 失败: {err}");
    })
}

/// 生成 `observability::keys` 模块源码。
///
/// # 教案式说明
/// - **意图（Why）**：构建时一次生成全部指标/日志/追踪键，杜绝人为拼写错误；
/// - **逻辑（How）**：构造模块树、逐层写入模块注释与常量定义；
/// - **契约（What）**：返回完整的 Rust 源码字符串，供写入 `crates/spark-core/src/governance/observability/keys.rs`。
fn render_observability_keys(contract: &ObservabilityKeysContract) -> String {
    let mut groups = contract.groups.clone();
    groups.sort_by(|a, b| a.path.cmp(&b.path));

    let mut root = ModuleNode::default();
    for group in &groups {
        insert_group(&mut root, group, &group.path);
    }

    let mut buffer = String::new();
    buffer.push_str("// @generated 自动生成文件，请勿手工修改。\n");
    buffer.push_str(
        "// 由 crates/spark-core/build.rs 根据 contracts/observability_keys.toml 生成。\n\n",
    );
    buffer.push_str("//! 可观测性键名契约：统一指标、日志与追踪键名的单一事实来源。\n");
    buffer.push_str("//!\n");
    buffer.push_str(
        "//! 教案式说明（Why）：数据来自 `contracts/observability_keys.toml`，构建脚本与工具据此生成代码与文档，避免多处漂移。\n",
    );
    buffer.push_str(
        "//! 契约定义（What）：各子模块（如 `metrics::service`）提供只读常量，供指标、日志与追踪统一引用。\n",
    );
    buffer.push_str(
        "//! 实现细节（How）：构建阶段展开模块树并写入稳定的 Rust 源文件，每个常量附带类型与适用范围说明。\n\n",
    );

    for (name, child) in &root.children {
        let path = vec![name.clone()];
        render_module(&mut buffer, name, child, 0, &path);
        buffer.push('\n');
    }

    buffer
}

/// 将分组信息插入模块树，缺失的中间节点会自动创建。
fn insert_group(current: &mut ModuleNode, group: &KeyGroupSpec, path: &[String]) {
    if path.is_empty() {
        current.title = Some(group.title.clone());
        current.description = Some(group.description.clone());
        current.items = group.items.clone();
        return;
    }

    let (head, tail) = path.split_first().expect("路径非空");
    let child = current.children.entry(head.clone()).or_default();
    insert_group(child, group, tail);
}

/// 递归渲染模块与其子项。
fn render_module(
    buffer: &mut String,
    name: &str,
    node: &ModuleNode,
    indent: usize,
    path: &[String],
) {
    write_module_doc(buffer, indent, name, node, path);
    indent_with(buffer, indent);
    writeln!(buffer, "pub mod {name} {{").expect("写入模块头部");

    if !node.children.is_empty() {
        buffer.push('\n');
    }

    for (child_name, child_node) in &node.children {
        let mut child_path = path.to_vec();
        child_path.push(child_name.clone());
        render_module(buffer, child_name, child_node, indent + 1, &child_path);
        buffer.push('\n');
    }

    if !node.children.is_empty() && !node.items.is_empty() {
        buffer.push('\n');
    }

    for (index, item) in node.items.iter().enumerate() {
        render_item(buffer, item, indent + 1);
        if index + 1 < node.items.len() {
            buffer.push('\n');
        }
    }

    indent_with(buffer, indent);
    buffer.push_str("}\n");
}

/// 写入模块级别的注释，优先使用合约中的标题与描述。
fn write_module_doc(
    buffer: &mut String,
    indent: usize,
    name: &str,
    node: &ModuleNode,
    path: &[String],
) {
    let mut lines = Vec::new();
    if let Some(title) = &node.title {
        lines.push(title.clone());
        if let Some(description) = &node.description {
            lines.push(String::new());
            lines.extend(description.lines().map(|line| line.to_owned()));
        }
    } else {
        let scope = path.join("::");
        lines.push(format!("{scope} 键名分组（模块标识：{name}）"));
        lines.push(String::new());
        lines.push("该分组由 contracts/observability_keys.toml 自动生成。".to_string());
    }

    for line in lines {
        indent_with(buffer, indent);
        if line.is_empty() {
            buffer.push_str("///\n");
        } else {
            writeln!(buffer, "/// {line}").expect("写入模块注释");
        }
    }
}

/// 渲染单个键常量及其注释。
fn render_item(buffer: &mut String, item: &KeySpec, indent: usize) {
    let mut usage = item.usage.clone();
    usage.sort();
    usage.dedup();

    let usage_text = if usage.is_empty() {
        "适用范围：-".to_string()
    } else {
        let joined = usage
            .into_iter()
            .map(UsageDomain::label)
            .collect::<Vec<_>>()
            .join("、");
        format!("适用范围：{joined}。")
    };

    write_doc_attr(buffer, indent, &format!("类型：{}。", item.kind.label()));
    write_doc_attr(buffer, indent, &usage_text);
    write_doc_attr(buffer, indent, "");
    for line in item.doc.lines() {
        write_doc_attr(buffer, indent, line);
    }
    indent_with(buffer, indent);
    writeln!(
        buffer,
        "pub const {}: &str = \"{}\";",
        item.ident,
        escape_rust_string(&item.value)
    )
    .expect("写入常量");
}

/// 配置事件合约顶层结构。
#[derive(Debug, Deserialize)]
struct ConfigEventsContract {
    version: String,
    summary: String,
    #[serde(default)]
    structs: Vec<EventStructSpec>,
    events: Vec<ConfigEventSpec>,
}

/// 事件使用的嵌套结构体声明。
#[derive(Debug, Deserialize, Clone)]
struct EventStructSpec {
    ident: String,
    summary: String,
    rationale: String,
    description: String,
    #[serde(default)]
    fields: Vec<EventFieldSpec>,
}

/// 事件字段声明。
#[derive(Debug, Deserialize, Clone)]
struct EventFieldSpec {
    name: String,
    required: bool,
    #[serde(rename = "type")]
    ty: EventFieldTypeSpec,
    doc: String,
}

/// 字段类型枚举。
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum EventFieldTypeSpec {
    String,
    U64,
    Bool,
    Struct { ident: String },
    List { item: Box<EventFieldTypeSpec> },
}

/// 单个事件的完整声明。
#[derive(Debug, Deserialize, Clone)]
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

/// 审计映射声明。
#[derive(Debug, Deserialize, Clone)]
struct EventAuditSpec {
    action: String,
    entity_kind: String,
    entity_id_field: String,
}

/// 演练用例声明。
#[derive(Debug, Deserialize, Clone)]
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

/// 读取配置事件合约。
fn read_config_events_contract(path: &Path) -> ConfigEventsContract {
    let raw = fs::read_to_string(path).unwrap_or_else(|err| {
        panic!("读取 {path:?} 失败: {err}");
    });
    toml::from_str(&raw).unwrap_or_else(|err| {
        panic!("解析 {path:?} 失败: {err}");
    })
}

/// 渲染配置事件模块源码。
fn render_config_events(contract: &ConfigEventsContract) -> String {
    let mut buffer = String::new();
    buffer.push_str("// @generated 自动生成文件，请勿手工修改。\n");
    buffer
        .push_str("// 由 crates/spark-core/build.rs 根据 contracts/config_events.toml 生成。\n\n");
    write_doc_attr(
        &mut buffer,
        0,
        "配置事件契约：由 contracts/config_events.toml 自动生成，统一控制面、运行面与审计面共享的事件语义。",
    );
    write_doc_attr(&mut buffer, 0, "");
    write_doc_attr(
        &mut buffer,
        0,
        &format!("合约版本（version）：{}。", escape_doc(&contract.version)),
    );
    write_doc_attr(
        &mut buffer,
        0,
        &format!("总体说明（summary）：{}。", escape_doc(&contract.summary)),
    );
    write_doc_attr(
        &mut buffer,
        0,
        "本模块与 docs/configuration-events.md 同步生成，如需调整字段请修改合约后重新生成。",
    );
    buffer.push_str("use alloc::{string::String, vec::Vec};\n\n");

    render_event_severity(&mut buffer);
    render_event_descriptor_types(&mut buffer);
    render_event_structs(&mut buffer, &contract.structs);
    render_event_payloads(&mut buffer, &contract.events);
    render_event_descriptors(&mut buffer, contract);

    buffer
}

fn render_event_severity(buffer: &mut String) {
    write_doc_attr(
        buffer,
        0,
        "配置事件严重性等级，映射合约中的 `severity` 字段，供告警与审计标签复用。",
    );
    write_doc_attr(buffer, 0, "");
    write_doc_attr(buffer, 0, "# 教案式说明（Why）");
    write_doc_attr(
        buffer,
        0,
        "- 统一控制面与审计面对于事件严重性的理解，避免多处硬编码。",
    );
    write_doc_attr(buffer, 0, "# 契约定义（What）");
    write_doc_attr(buffer, 0, "- 取值范围：info/warning/critical；");
    write_doc_attr(buffer, 0, "# 实现提示（How）");
    write_doc_attr(
        buffer,
        0,
        "- 生成代码中提供 `as_str` 帮助转换为稳定字符串标签。",
    );
    buffer.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq)]\n");
    buffer.push_str("pub enum EventSeverity {\n");
    buffer.push_str("    Info,\n");
    buffer.push_str("    Warning,\n");
    buffer.push_str("    Critical,\n");
    buffer.push_str("}\n\n");

    write_doc_attr(
        buffer,
        0,
        "严重性到字符串的稳定映射，用于事件标签与审计实体标签。",
    );
    write_doc_attr(buffer, 0, "");
    write_doc_attr(buffer, 0, "# 合同说明（What）");
    write_doc_attr(
        buffer,
        0,
        "- 输入：严重性枚举；输出：`info`/`warning`/`critical`。",
    );
    write_doc_attr(buffer, 0, "# 风险提示（Trade-offs）");
    write_doc_attr(
        buffer,
        0,
        "- 若合约新增等级需同步扩展匹配分支，否则构建时会 panic。",
    );
    buffer.push_str("impl EventSeverity {\n");
    buffer.push_str("    pub const fn as_str(self) -> &'static str {\n");
    buffer.push_str("        match self {\n");
    buffer.push_str("            Self::Info => \"info\",\n");
    buffer.push_str("            Self::Warning => \"warning\",\n");
    buffer.push_str("            Self::Critical => \"critical\",\n");
    buffer.push_str("        }\n");
    buffer.push_str("    }\n");
    buffer.push_str("}\n\n");
}

fn render_event_descriptor_types(buffer: &mut String) {
    write_doc_attr(
        buffer,
        0,
        "事件元数据描述符，集中存放代码、名称、审计映射与演练用例。",
    );
    write_doc_attr(buffer, 0, "");
    write_doc_attr(buffer, 0, "# Why");
    write_doc_attr(
        buffer,
        0,
        "- 运行面与工具链可以读取该描述符生成文档或校验事件负载。",
    );
    write_doc_attr(buffer, 0, "# What");
    write_doc_attr(
        buffer,
        0,
        "- 包含事件识别码、所属家族、严重性、摘要、详细说明、审计映射、字段列表与演练用例。",
    );
    write_doc_attr(buffer, 0, "# How");
    write_doc_attr(
        buffer,
        0,
        "- 构建脚本直接写出 `'static` 常量，确保运行期零成本访问。",
    );
    buffer.push_str("#[derive(Clone, Debug, PartialEq)]\n");
    buffer.push_str("pub struct ConfigurationEventDescriptor {\n");
    buffer.push_str("    pub ident: &'static str,\n");
    buffer.push_str("    pub code: &'static str,\n");
    buffer.push_str("    pub family: &'static str,\n");
    buffer.push_str("    pub name: &'static str,\n");
    buffer.push_str("    pub severity: EventSeverity,\n");
    buffer.push_str("    pub summary: &'static str,\n");
    buffer.push_str("    pub description: &'static str,\n");
    buffer.push_str("    pub audit: AuditDescriptor,\n");
    buffer.push_str("    pub fields: &'static [EventFieldDescriptor],\n");
    buffer.push_str("    pub drills: &'static [DrillDescriptor],\n");
    buffer.push_str("}\n\n");

    write_doc_attr(
        buffer,
        0,
        "描述事件与审计系统的映射关系，包括动作与实体标识。",
    );
    buffer.push_str("#[derive(Clone, Debug, PartialEq, Eq)]\n");
    buffer.push_str("pub struct AuditDescriptor {\n");
    buffer.push_str("    pub action: &'static str,\n");
    buffer.push_str("    pub entity_kind: &'static str,\n");
    buffer.push_str("    pub entity_id_field: &'static str,\n");
    buffer.push_str("}\n\n");

    write_doc_attr(
        buffer,
        0,
        "字段描述符，记录字段类型、是否必填以及教案级说明。",
    );
    buffer.push_str("#[derive(Clone, Debug, PartialEq, Eq)]\n");
    buffer.push_str("pub struct EventFieldDescriptor {\n");
    buffer.push_str("    pub name: &'static str,\n");
    buffer.push_str("    pub type_name: &'static str,\n");
    buffer.push_str("    pub required: bool,\n");
    buffer.push_str("    pub doc: &'static str,\n");
    buffer.push_str("}\n\n");

    write_doc_attr(
        buffer,
        0,
        "演练用例描述符，记录目标、准备步骤、执行步骤与验收标准。",
    );
    buffer.push_str("#[derive(Clone, Debug, PartialEq, Eq)]\n");
    buffer.push_str("pub struct DrillDescriptor {\n");
    buffer.push_str("    pub title: &'static str,\n");
    buffer.push_str("    pub goal: &'static str,\n");
    buffer.push_str("    pub setup: &'static [&'static str],\n");
    buffer.push_str("    pub steps: &'static [&'static str],\n");
    buffer.push_str("    pub expectations: &'static [&'static str],\n");
    buffer.push_str("}\n\n");
}

fn render_event_structs(buffer: &mut String, structs: &[EventStructSpec]) {
    for spec in structs {
        write_doc_attr(
            buffer,
            0,
            &format!("结构体 `{}`：{}。", spec.ident, escape_doc(&spec.summary)),
        );
        write_doc_attr(buffer, 0, "");
        write_doc_attr(buffer, 0, "# 教案式说明（Why）");
        write_doc_attr(buffer, 0, &format!("- {}", escape_doc(&spec.rationale)));
        write_doc_attr(buffer, 0, "# 契约定义（What）");
        write_doc_attr(
            buffer,
            0,
            "- 字段映射遵循合约声明顺序，所有字段均参与事件序列化。",
        );
        write_doc_attr(buffer, 0, "# 实现提示（How）");
        write_doc_attr(buffer, 0, &format!("- {}", escape_doc(&spec.description)));
        write_doc_attr(buffer, 0, "# 风险与注意事项（Trade-offs）");
        write_doc_attr(
            buffer,
            0,
            "- 修改字段时务必同步更新合约与演练文档，避免 SOT 漂移。",
        );
        buffer.push_str("#[derive(Clone, Debug, PartialEq)]\n");
        buffer.push_str(&format!("pub struct {} {{\n", spec.ident));
        for field in &spec.fields {
            let field_type = render_field_type(&field.ty, field.required);
            let type_doc = render_field_type_doc(&field.ty, field.required);
            write_doc_attr(
                buffer,
                1,
                &format!(
                    "字段 `{}`（Why/What/How）：{}",
                    field.name,
                    escape_doc(&field.doc)
                ),
            );
            write_doc_attr(buffer, 1, &format!("- 类型：{}。", escape_doc(&type_doc)));
            write_doc_attr(buffer, 1, "- 前置条件：调用方需提供符合命名规范的数据；");
            write_doc_attr(buffer, 1, "- 后置条件：事件序列化后保持原样，供审计回放。");
            indent_with(buffer, 1);
            writeln!(buffer, "pub {}: {},", field.name, field_type).expect("写入结构体字段");
            buffer.push('\n');
        }
        buffer.push_str("}\n\n");
    }
}

fn render_event_payloads(buffer: &mut String, events: &[ConfigEventSpec]) {
    for event in events {
        write_doc_attr(
            buffer,
            0,
            &format!(
                "事件 `{}`（代码：`{}`，家族：`{}`）。",
                escape_doc(&event.name),
                escape_doc(&event.code),
                escape_doc(&event.family)
            ),
        );
        write_doc_attr(buffer, 0, "");
        write_doc_attr(buffer, 0, "# 教案式说明（Why）");
        write_doc_attr(buffer, 0, &format!("- {}", escape_doc(&event.summary)));
        write_doc_attr(buffer, 0, "# 契约定义（What）");
        write_doc_attr(buffer, 0, &format!("- {}", escape_doc(&event.description)));
        write_doc_attr(
            buffer,
            0,
            &format!(
                "- 审计映射：action=`{}`，entity_kind=`{}`，entity_id_field=`{}`。",
                escape_doc(&event.audit.action),
                escape_doc(&event.audit.entity_kind),
                escape_doc(&event.audit.entity_id_field)
            ),
        );
        write_doc_attr(buffer, 0, "# 实现提示（How）");
        write_doc_attr(
            buffer,
            0,
            "- 该结构体字段顺序与合约保持一致，便于聚合器直接构造；",
        );
        write_doc_attr(
            buffer,
            0,
            "- 由生成器保证字段文档嵌入，提醒调用者关注前置/后置条件。",
        );
        write_doc_attr(buffer, 0, "# 风险与注意事项（Trade-offs）");
        write_doc_attr(
            buffer,
            0,
            "- 修改字段前需评估审计与演练文档影响，避免链路断裂。",
        );
        buffer.push_str("#[derive(Clone, Debug, PartialEq)]\n");
        buffer.push_str(&format!("pub struct {} {{\n", event.ident));
        for field in &event.fields {
            let field_type = render_field_type(&field.ty, field.required);
            let type_doc = render_field_type_doc(&field.ty, field.required);
            write_doc_attr(
                buffer,
                1,
                &format!(
                    "字段 `{}`（Why/What/How）：{}",
                    field.name,
                    escape_doc(&field.doc)
                ),
            );
            write_doc_attr(buffer, 1, &format!("- 类型：{}。", escape_doc(&type_doc)));
            if field.required {
                write_doc_attr(buffer, 1, "- 前置条件：调用方必须提供该字段；");
            } else {
                write_doc_attr(buffer, 1, "- 前置条件：可选字段，缺省表示信息未知；");
            }
            write_doc_attr(buffer, 1, "- 后置条件：事件序列化后用于审计与演练校验。");
            indent_with(buffer, 1);
            writeln!(buffer, "pub {}: {},", field.name, field_type).expect("写入事件字段");
            buffer.push('\n');
        }
        buffer.push_str("}\n\n");
    }
}

fn render_event_descriptors(buffer: &mut String, contract: &ConfigEventsContract) {
    write_doc_attr(
        buffer,
        0,
        "配置事件合约版本常量，用于运行时快速校验生成产物。",
    );
    buffer.push_str(&format!(
        "pub const CONFIGURATION_EVENTS_VERSION: &str = \"{}\";\n\n",
        escape_rust_string(&contract.version)
    ));
    write_doc_attr(buffer, 0, "配置事件合约摘要，便于 UI 或 CLI 展示总体说明。");
    buffer.push_str(&format!(
        "pub const CONFIGURATION_EVENTS_SUMMARY: &str = \"{}\";\n\n",
        escape_rust_string(&contract.summary)
    ));

    for event in &contract.events {
        let const_prefix = event.ident.to_shouty_snake_case();
        let fields_const = format!("{}_FIELDS", const_prefix);
        let drills_const = format!("{}_DRILLS", const_prefix);

        buffer.push_str(&format!(
            "pub const {}: &[EventFieldDescriptor] = &[\n",
            fields_const
        ));
        for field in &event.fields {
            let field_type = render_field_type(&field.ty, field.required);
            buffer.push_str("    EventFieldDescriptor {\n");
            buffer.push_str(&format!(
                "        name: \"{}\",\n",
                escape_rust_string(&field.name)
            ));
            buffer.push_str(&format!(
                "        type_name: \"{}\",\n",
                escape_rust_string(&field_type)
            ));
            buffer.push_str(&format!(
                "        required: {},\n",
                if field.required { "true" } else { "false" }
            ));
            buffer.push_str(&format!(
                "        doc: \"{}\",\n",
                escape_rust_string(&field.doc)
            ));
            buffer.push_str("    },\n");
        }
        buffer.push_str("];\n\n");

        buffer.push_str(&format!(
            "pub const {}: &[DrillDescriptor] = &[\n",
            drills_const
        ));
        for drill in &event.drills {
            buffer.push_str("    DrillDescriptor {\n");
            buffer.push_str(&format!(
                "        title: \"{}\",\n",
                escape_rust_string(&drill.title)
            ));
            buffer.push_str(&format!(
                "        goal: \"{}\",\n",
                escape_rust_string(&drill.goal)
            ));
            buffer.push_str(&format!(
                "        setup: {},\n",
                render_string_slice(&drill.setup)
            ));
            buffer.push_str(&format!(
                "        steps: {},\n",
                render_string_slice(&drill.steps)
            ));
            buffer.push_str(&format!(
                "        expectations: {},\n",
                render_string_slice(&drill.expectations)
            ));
            buffer.push_str("    },\n");
        }
        buffer.push_str("];\n\n");

        buffer.push_str(&format!(
            "pub const {}: ConfigurationEventDescriptor = ConfigurationEventDescriptor {{\n",
            const_prefix
        ));
        buffer.push_str(&format!(
            "    ident: \"{}\",\n",
            escape_rust_string(&event.ident)
        ));
        buffer.push_str(&format!(
            "    code: \"{}\",\n",
            escape_rust_string(&event.code)
        ));
        buffer.push_str(&format!(
            "    family: \"{}\",\n",
            escape_rust_string(&event.family)
        ));
        buffer.push_str(&format!(
            "    name: \"{}\",\n",
            escape_rust_string(&event.name)
        ));
        buffer.push_str(&format!(
            "    severity: {},\n",
            severity_variant(&event.severity)
        ));
        buffer.push_str(&format!(
            "    summary: \"{}\",\n",
            escape_rust_string(&event.summary)
        ));
        buffer.push_str(&format!(
            "    description: \"{}\",\n",
            escape_rust_string(&event.description)
        ));
        buffer.push_str("    audit: AuditDescriptor {\n");
        buffer.push_str(&format!(
            "        action: \"{}\",\n",
            escape_rust_string(&event.audit.action)
        ));
        buffer.push_str(&format!(
            "        entity_kind: \"{}\",\n",
            escape_rust_string(&event.audit.entity_kind)
        ));
        buffer.push_str(&format!(
            "        entity_id_field: \"{}\",\n",
            escape_rust_string(&event.audit.entity_id_field)
        ));
        buffer.push_str("    },\n");
        buffer.push_str(&format!("    fields: {},\n", fields_const));
        buffer.push_str(&format!("    drills: {},\n", drills_const));
        buffer.push_str("};\n\n");
    }

    buffer.push_str("pub const CONFIGURATION_EVENTS: &[ConfigurationEventDescriptor] = &[\n");
    for event in &contract.events {
        buffer.push_str(&format!("    {},\n", event.ident.to_shouty_snake_case()));
    }
    buffer.push_str("];\n");
}

fn render_field_type(ty: &EventFieldTypeSpec, required: bool) -> String {
    let base = match ty {
        EventFieldTypeSpec::String => "String".to_string(),
        EventFieldTypeSpec::U64 => "u64".to_string(),
        EventFieldTypeSpec::Bool => "bool".to_string(),
        EventFieldTypeSpec::Struct { ident } => ident.clone(),
        EventFieldTypeSpec::List { item } => {
            let inner = render_field_type(item, true);
            format!("Vec<{}>", inner)
        }
    };
    if required {
        base
    } else {
        format!("Option<{}>", base)
    }
}

fn render_field_type_doc(ty: &EventFieldTypeSpec, required: bool) -> String {
    let raw = render_field_type(ty, required);
    format!("`{}`", raw)
}

fn render_string_slice(items: &[String]) -> String {
    if items.is_empty() {
        "&[]".to_string()
    } else {
        let mut result = String::from("&[");
        for (idx, item) in items.iter().enumerate() {
            if idx > 0 {
                result.push_str(", ");
            }
            result.push('"');
            result.push_str(&escape_rust_string(item));
            result.push('"');
        }
        result.push(']');
        result
    }
}

fn severity_variant(value: &str) -> &'static str {
    match value.to_ascii_lowercase().as_str() {
        "info" => "EventSeverity::Info",
        "warning" => "EventSeverity::Warning",
        "critical" => "EventSeverity::Critical",
        other => panic!("未知的事件严重性：{other}"),
    }
}

/// 写入单行 `#[doc = "..."]` 属性。
fn write_doc_attr(buffer: &mut String, indent: usize, line: &str) {
    indent_with(buffer, indent);
    buffer.push_str("#[doc = \"");
    buffer.push_str(&escape_doc(line));
    buffer.push_str("\"]\n");
}

/// 按缩进写入四个空格乘以层级。
fn indent_with(buffer: &mut String, indent: usize) {
    for _ in 0..indent {
        buffer.push_str("    ");
    }
}

/// 转义 doc 属性中的特殊字符。
fn escape_doc(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

/// 转义字符串字面量。
fn escape_rust_string(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

/// 渲染最终的 Rust 模块文本。
///
/// # Why
/// - 保留原有手写模块的教案级注释与类型定义，降低后续维护成本；
/// - 将矩阵常量的数据段自动拼装，杜绝人工疏漏。
///
/// # How
/// - 使用字符串构建：先写入固定模板，再逐条追加矩阵条目；
/// - 通过 [`render_template`] 将声明式模板转换为具体的 Rust 表达式。
///
/// # What
/// - 输入：展开后的条目；输出：完整的模块源码字符串。
fn render_category_matrix(entries: &[ExpandedEntry]) -> String {
    let mut buffer = String::new();
    buffer.push_str("// @generated 自动生成文件，请勿手工修改。\n");
    buffer.push_str("// 由 crates/spark-core/build.rs 根据 contracts/error_matrix.toml 生成。\n\n");
    buffer.push_str("use crate::{\n");
    buffer.push_str("    error::ErrorCategory,\n");
    buffer.push_str("    security::SecurityClass,\n");
    buffer.push_str("    status::{BusyReason, RetryAdvice},\n");
    buffer.push_str("    types::BudgetKind,\n");
    buffer.push_str("};\n");
    buffer.push_str("use core::time::Duration;\n\n");
    buffer.push_str(r#"/// 默认错误分类矩阵的只读数据源，集中声明“错误码 → ErrorCategory → 自动响应动作”的三段映射。
///
/// # 教案式背景说明（Why）
/// - 构建脚本从 `contracts/error_matrix.toml` 读取声明式数据，统一驱动代码、文档与测试；
/// - 该模块作为框架内的单一事实来源（Single Source of Truth），确保新增错误码时无需手动同步多处文件。
///
/// # 契约定义（What）
/// - 对外暴露 [`entries`]、[`entry_for_code`]、[`default_autoresponse`] 三个只读查询接口；
/// - **输入约束**：调用方必须传入在文档中登记的稳定错误码；
/// - **返回承诺**：表中提供对应的 [`ErrorCategory`] 与默认动作（重试/背压/关闭/取消/无动作）。
///
/// # 实现思路（How）
/// - 使用 [`CategoryMatrixEntry`] 承载错误码与 [`CategoryTemplate`]；
/// - `CategoryTemplate` 负责按需构造 [`ErrorCategory`] 与 [`DefaultAutoResponse`]，避免在静态表中直接存放运行期对象；
/// - 静态常量 `MATRIX` 由构建脚本生成，确保条目顺序稳定；
/// - 通过辅助枚举 [`BusyDisposition`] 与 [`BudgetDisposition`] 描述“繁忙主语义”与“预算类型”，避免跨模块直接依赖内部细节。
///
/// # 风险与权衡（Trade-offs & Gotchas）
/// - 若未来扩展新的自动响应动作，需同步扩展 [`DefaultAutoResponse`] 及默认处理器；
/// - 表项按字母顺序维护，便于审查；测试会确保文档同步，避免遗漏更新。
"#);
    buffer.push_str("pub mod matrix {\n");
    buffer.push_str("    use super::{CategoryMatrixEntry, DefaultAutoResponse};\n\n");
    buffer.push_str(
        r#"    /// 暴露内部静态矩阵，供调用方遍历但禁止修改。
    pub const fn entries() -> &'static [CategoryMatrixEntry] {
        super::MATRIX
    }

    /// 按错误码查找矩阵条目。
    ///
    /// # 契约说明
    /// - **输入**：遵循 `<域>.<语义>` 规则的稳定错误码；
    /// - **前置条件**：错误码必须事先收录于矩阵中；
    /// - **返回值**：若存在条目返回引用，否则 `None`（表示需显式指定分类）。
    pub fn entry_for_code(code: &str) -> Option<&'static CategoryMatrixEntry> {
        entries().iter().find(|entry| entry.code == code)
    }

    /// 提供默认的自动响应动作，供 Pipeline 或测试驱动行为一致性。
    pub fn default_autoresponse(code: &str) -> Option<DefaultAutoResponse> {
        entry_for_code(code).map(CategoryMatrixEntry::default_response)
    }

    /// 内部使用：根据错误码返回默认 [`ErrorCategory`]。
    pub(crate) fn lookup_default_category(code: &str) -> Option<crate::error::ErrorCategory> {
        entry_for_code(code).map(CategoryMatrixEntry::category)
    }
"#,
    );
    buffer.push_str("}\n\n");
    buffer.push_str("pub use matrix::{default_autoresponse, entries, entry_for_code};\n\n");
    buffer.push_str(
        r#"/// 描述单条“错误码 → 分类模板”的映射关系。
///
/// # 字段契约
/// - `code`：稳定错误码，匹配 `docs/error-category-matrix.md` 中的第一列；
/// - `template`：分类模板，封装 `ErrorCategory` 的构造逻辑与默认动作定义。
#[derive(Clone, Copy)]
pub struct CategoryMatrixEntry {
    code: &'static str,
    template: CategoryTemplate,
}

impl CategoryMatrixEntry {
    /// 返回错误码。
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// 根据模板构造默认分类。
    pub fn category(&self) -> ErrorCategory {
        self.template.instantiate()
    }

    /// 计算默认自动响应动作。
    pub fn default_response(&self) -> DefaultAutoResponse {
        self.template.default_response()
    }
}

/// 表达如何从静态表生成 `ErrorCategory` 及自动响应动作的模板。
#[derive(Clone, Copy)]
pub enum CategoryTemplate {
    /// 对应 `Retryable` 分类，携带等待窗口、原因描述与繁忙主语义。
    Retryable {
        wait_ms: u64,
        reason: &'static str,
        busy: Option<BusyDisposition>,
    },
    /// `Timeout` 分类。
    Timeout,
    /// 协议违规。
    ProtocolViolation { close_message: &'static str },
    /// 预算耗尽（区分预算类型）。
    ResourceExhausted { budget: BudgetDisposition },
    /// 运行时取消。
    Cancelled,
    /// 非重试错误。
    NonRetryable,
    /// 安全事件，需要携带具体安全分类以生成关闭文案。
    Security { class: SecurityClass },
}

impl CategoryTemplate {
    /// 按模板实例化 [`ErrorCategory`]。
    pub fn instantiate(&self) -> ErrorCategory {
        match self {
            CategoryTemplate::Retryable {
                wait_ms, reason, ..
            } => {
                let advice =
                    RetryAdvice::after(Duration::from_millis(*wait_ms)).with_reason(*reason);
                ErrorCategory::Retryable(advice)
            }
            CategoryTemplate::Timeout => ErrorCategory::Timeout,
            CategoryTemplate::ProtocolViolation { .. } => ErrorCategory::ProtocolViolation,
            CategoryTemplate::ResourceExhausted { budget } => {
                ErrorCategory::ResourceExhausted(budget.to_budget_kind())
            }
            CategoryTemplate::Cancelled => ErrorCategory::Cancelled,
            CategoryTemplate::NonRetryable => ErrorCategory::NonRetryable,
            CategoryTemplate::Security { class } => ErrorCategory::Security(*class),
        }
    }

    /// 计算默认自动响应动作。
    pub fn default_response(&self) -> DefaultAutoResponse {
        match self {
            CategoryTemplate::Retryable {
                wait_ms,
                reason,
                busy,
            } => DefaultAutoResponse::RetryAfter {
                wait_ms: *wait_ms,
                reason,
                busy: *busy,
            },
            CategoryTemplate::Timeout | CategoryTemplate::Cancelled => DefaultAutoResponse::Cancel,
            CategoryTemplate::ProtocolViolation { close_message } => DefaultAutoResponse::Close {
                reason_code: "protocol.violation",
                message: close_message,
            },
            CategoryTemplate::ResourceExhausted { budget } => {
                DefaultAutoResponse::BudgetExhausted { budget: *budget }
            }
            CategoryTemplate::NonRetryable => DefaultAutoResponse::None,
            CategoryTemplate::Security { class } => DefaultAutoResponse::Close {
                reason_code: "security.violation",
                message: class.summary(),
            },
        }
    }
}

/// 默认自动响应动作的精简表达，用于测试与 Pipeline 复用。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultAutoResponse {
    /// 广播 `Busy`（可选）随后发送 `RetryAfter`。
    RetryAfter {
        wait_ms: u64,
        reason: &'static str,
        busy: Option<BusyDisposition>,
    },
    /// 广播 `BudgetExhausted`。
    BudgetExhausted { budget: BudgetDisposition },
    /// 触发优雅关闭。
    Close {
        reason_code: &'static str,
        message: &'static str,
    },
    /// 标记取消令牌。
    Cancel,
    /// 默认不产生额外动作。
    None,
}

impl DefaultAutoResponse {
    /// 若为背压分支，返回对应的预算类型。
    pub const fn budget(&self) -> Option<BudgetDisposition> {
        match self {
            DefaultAutoResponse::BudgetExhausted { budget } => Some(*budget),
            _ => None,
        }
    }

    /// 若为关闭分支，返回关闭原因元组。
    pub const fn close_reason(&self) -> Option<(&'static str, &'static str)> {
        match self {
            DefaultAutoResponse::Close {
                reason_code,
                message,
            } => Some((*reason_code, *message)),
            _ => None,
        }
    }

    /// 若为重试分支，返回等待窗口与繁忙主语义。
    pub const fn retry(&self) -> Option<(u64, &'static str, Option<BusyDisposition>)> {
        match self {
            DefaultAutoResponse::RetryAfter {
                wait_ms,
                reason,
                busy,
            } => Some((*wait_ms, *reason, *busy)),
            _ => None,
        }
    }
}

/// 描述在退避信号之前广播的繁忙主语义（上游/下游）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusyDisposition {
    Upstream,
    Downstream,
}

impl BusyDisposition {
    /// 转换为 [`BusyReason`]，供 Pipeline 默认处理器复用。
    pub fn to_busy_reason(self) -> BusyReason {
        match self {
            BusyDisposition::Upstream => BusyReason::upstream(),
            BusyDisposition::Downstream => BusyReason::downstream(),
        }
    }
}

/// 描述预算耗尽场景的预算类型，避免在常量表中直接引用 `BudgetKind`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetDisposition {
    Decode,
    Flow,
}

impl BudgetDisposition {
    /// 转换为框架实际使用的 [`BudgetKind`]。
    pub fn to_budget_kind(self) -> BudgetKind {
        match self {
            BudgetDisposition::Decode => BudgetKind::Decode,
            BudgetDisposition::Flow => BudgetKind::Flow,
        }
    }
}
"#,
    );
    buffer.push_str("\n/// 静态矩阵：保持按错误码字典序排列，方便审查与 diff。\nconst MATRIX: &[CategoryMatrixEntry] = &[\n");
    for entry in entries {
        let const_name = code_constant(&entry.code);
        let template_expr = render_template(&entry.template);
        writeln!(
            buffer,
            "    CategoryMatrixEntry {{\n        code: crate::error::codes::{const_name},\n        template: {template_expr},\n    }},"
        )
        .expect("写入矩阵条目");
    }
    buffer.push_str("];\n");
    buffer
}

/// 将错误码转为常量名称，例如 `transport.io` → `TRANSPORT_IO`。
fn code_constant(code: &str) -> String {
    let mut name = String::with_capacity(code.len());
    for ch in code.chars() {
        match ch {
            '.' | '-' => name.push('_'),
            _ => name.push(ch),
        }
    }
    name.to_ascii_uppercase()
}

/// 渲染分类模板的 Rust 表达式。
fn render_template(template: &CategoryTemplateSpec) -> String {
    match template {
        CategoryTemplateSpec::Retryable {
            wait_ms,
            reason,
            busy,
        } => {
            let reason_literal = to_rust_string(reason);
            let busy_expr = match busy {
                Some(BusyDispositionSpec::Upstream) => {
                    "Some(BusyDisposition::Upstream)".to_string()
                }
                Some(BusyDispositionSpec::Downstream) => {
                    "Some(BusyDisposition::Downstream)".to_string()
                }
                None => "None".to_string(),
            };
            format!(
                "CategoryTemplate::Retryable {{\n            wait_ms: {wait_ms},\n            reason: {reason_literal},\n            busy: {busy_expr},\n        }}"
            )
        }
        CategoryTemplateSpec::Timeout => "CategoryTemplate::Timeout".to_string(),
        CategoryTemplateSpec::ProtocolViolation { close_message } => {
            let literal = to_rust_string(close_message);
            format!(
                "CategoryTemplate::ProtocolViolation {{\n            close_message: {literal},\n        }}"
            )
        }
        CategoryTemplateSpec::ResourceExhausted { budget } => {
            let budget_expr = match budget {
                BudgetDispositionSpec::Decode => "BudgetDisposition::Decode",
                BudgetDispositionSpec::Flow => "BudgetDisposition::Flow",
            };
            format!(
                "CategoryTemplate::ResourceExhausted {{\n            budget: {budget_expr},\n        }}"
            )
        }
        CategoryTemplateSpec::Cancelled => "CategoryTemplate::Cancelled".to_string(),
        CategoryTemplateSpec::NonRetryable => "CategoryTemplate::NonRetryable".to_string(),
        CategoryTemplateSpec::Security { class } => {
            let class_expr = match class {
                SecurityClassSpec::Authentication => "SecurityClass::Authentication",
                SecurityClassSpec::Authorization => "SecurityClass::Authorization",
                SecurityClassSpec::Confidentiality => "SecurityClass::Confidentiality",
                SecurityClassSpec::Integrity => "SecurityClass::Integrity",
                SecurityClassSpec::Audit => "SecurityClass::Audit",
                SecurityClassSpec::Unknown => "SecurityClass::Unknown",
            };
            format!("CategoryTemplate::Security {{\n            class: {class_expr},\n        }}")
        }
    }
}

/// 将任意字符串转换为合法的 Rust 字面量，处理转义字符。
fn to_rust_string(value: &str) -> String {
    let mut literal = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => literal.push_str("\\\\"),
            '"' => literal.push_str("\\\""),
            '\n' => literal.push_str("\\n"),
            '\r' => literal.push_str("\\r"),
            '\t' => literal.push_str("\\t"),
            c if c.is_control() => literal.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => literal.push(c),
        }
    }
    literal.push('"');
    literal
}
