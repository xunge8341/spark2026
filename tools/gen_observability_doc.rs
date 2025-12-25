use std::{
    env,
    fmt::Write,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

/// 文档生成入口：根据 `contracts/observability_keys.toml` 产出 Markdown。
///
/// # 教案式说明（Why）
/// - 保障指标/日志/追踪键名的知识库与代码保持同步；
/// - 提供面向 SRE 的汇总文档，便于查找字段含义与适用范围；
/// - 与构建脚本共享同一合约，形成单一事实来源（SOT）。
///
/// # 契约定义（What）
/// - 输入：仓库根目录下的 `contracts/observability_keys.toml`；
/// - 前置条件：启用 `std` 与 `observability_contract_doc` feature 以访问文件系统与 `toml` 解析；
/// - 后置条件：覆盖写入 `docs/observability-contract.md`，生成包含表格的说明文档。
///
/// # 运行流程（How）
/// 1. 读取合约文件并解析为强类型结构；
/// 2. 按路径排序各分组，逐一渲染 Markdown 小节；
/// 3. 写入目标文档，供 CI 与人工评审校验。
fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let contract_path = manifest_dir.join("../../contracts/observability_keys.toml");
    let contract = read_contract(&contract_path);
    let markdown = render_markdown(&contract);
    let doc_path = manifest_dir.join("../../docs/observability-contract.md");
    fs::write(&doc_path, markdown).expect("写入 docs/observability-contract.md");
}

/// 与构建脚本共用的顶层结构。
#[derive(Debug, Deserialize)]
struct ObservabilityKeysContract {
    groups: Vec<KeyGroupSpec>,
}

/// 单个模块分组的描述。
#[derive(Debug, Deserialize, Clone)]
struct KeyGroupSpec {
    path: Vec<String>,
    title: String,
    description: String,
    #[serde(default)]
    items: Vec<KeySpec>,
}

/// 具体键名或枚举值的元信息。
#[derive(Debug, Deserialize, Clone)]
struct KeySpec {
    ident: String,
    value: String,
    kind: KeyKind,
    #[serde(default)]
    usage: Vec<UsageDomain>,
    doc: String,
}

/// 键名类型分类。
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum KeyKind {
    AttributeKey,
    LabelValue,
    LogField,
    TraceField,
}

impl KeyKind {
    fn label(self) -> &'static str {
        match self {
            KeyKind::AttributeKey => "指标/日志键",
            KeyKind::LabelValue => "标签枚举值",
            KeyKind::LogField => "日志字段",
            KeyKind::TraceField => "追踪字段",
        }
    }
}

/// 可观测性维度。
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum UsageDomain {
    Metrics,
    Logs,
    Tracing,
    Events,
}

impl UsageDomain {
    fn label(self) -> &'static str {
        match self {
            UsageDomain::Metrics => "指标",
            UsageDomain::Logs => "日志",
            UsageDomain::Tracing => "追踪",
            UsageDomain::Events => "运维事件",
        }
    }
}

/// 读取合约文件。
fn read_contract(path: &Path) -> ObservabilityKeysContract {
    let raw = fs::read_to_string(path).unwrap_or_else(|err| {
        panic!("读取 {path:?} 失败: {err}");
    });
    toml::from_str(&raw).unwrap_or_else(|err| {
        panic!("解析 {path:?} 失败: {err}");
    })
}

/// 渲染整份 Markdown 文档。
fn render_markdown(contract: &ObservabilityKeysContract) -> String {
    let mut groups = contract.groups.clone();
    groups.sort_by(|a, b| a.path.cmp(&b.path));

    let mut buf = String::new();
    buf.push_str("<!-- @generated 自动生成，请勿手工编辑 -->\n");
    buf.push_str("# 可观测性键名契约（Observability Keys Contract）\n\n");
    buf.push_str("> 目标：指标/日志/追踪键名统一管理，避免代码与仪表盘漂移。\n");
    buf.push_str("> 来源：`contracts/observability_keys.toml`（单一事实来源）。\n");
    buf.push_str(
        "> 产物：生成 `crates/spark-core/src/governance/observability/keys.rs` 与本文档。\n\n",
    );

    buf.push_str("## 阅读指引\n\n");
    buf.push_str("- **键类型**：区分指标/日志键、标签枚举值、日志字段与追踪字段；\n");
    buf.push_str("- **适用范围**：标记该键应用于指标、日志、追踪或运维事件；\n");
    buf.push_str("- **更新流程**：修改合约后同步运行生成器，并提交代码与文档。\n\n");

    for group in groups {
        render_group(&mut buf, &group);
    }

    buf
}

/// 渲染单个分组的小节。
fn render_group(buf: &mut String, group: &KeyGroupSpec) {
    let heading = format!("{} — {}", group.path.join("."), group.title);
    writeln!(buf, "## {heading}").expect("写入标题");
    buf.push('\n');
    for line in group.description.lines() {
        writeln!(buf, "> {line}").expect("写入描述");
    }
    buf.push('\n');
    buf.push_str("| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |\n");
    buf.push_str("| --- | --- | --- | --- | --- |\n");

    let mut items = group.items.clone();
    items.sort_by(|a, b| a.ident.cmp(&b.ident));
    for item in items {
        let kind = item.kind.label();
        let usage = format_usage(&item.usage);
        let value = escape_markdown(&item.value);
        let doc = escape_markdown(&item.doc);
        writeln!(
            buf,
            "| `{}` | {} | {} | `{}` | {} |",
            item.ident, kind, usage, value, doc
        )
        .expect("写入表格行");
    }
    buf.push('\n');
}

/// 将适用范围渲染为 Markdown 字符串。
fn format_usage(usage: &[UsageDomain]) -> String {
    if usage.is_empty() {
        "-".to_string()
    } else {
        let mut domains = usage.to_vec();
        domains.sort();
        domains
            .into_iter()
            .map(UsageDomain::label)
            .collect::<Vec<_>>()
            .join("、")
    }
}

/// 转义 Markdown 表格中的特殊字符。
fn escape_markdown(input: &str) -> String {
    let replaced = input.replace('|', "\\|").replace('`', "\\`");
    replaced.replace('\n', "<br />")
}
