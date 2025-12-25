//! 跨语言配置事件契约生成器：导出 JSON Schema / AsyncAPI，并生成 Python & Java SDK。
//!
//! # 教案式动机（Why）
//! - 以落盘的 JSON Schema 与 AsyncAPI 文档作为跨语言单一事实来源（SOT），反向驱动 SDK 代码生成，确保不同语言基于完全一致的契约；
//! - 通过自动化生成，避免 Rust、Python、Java 在事件字段、审计映射及错误矩阵上的手工维护与漂移；
//! - 为客户端 TCK 提供稳定输入，确保多语言实现以同一份契约驱动验证。
//!
//! # 契约声明（What）
//! - 输入：仓库根目录下的 `contracts/config_events.toml` 与 `contracts/error_matrix.toml`（用于生成/校验 SOT），以及既有的 Schema/AsyncAPI；
//! - 输出：
//!   * `schemas/configuration-events.schema.json`：JSON Schema Draft 2020-12；
//!   * `schemas/configuration-events.asyncapi.json`：AsyncAPI 2.6.0 文档；
//!   * `sdk/python/` 下的教学级 Python SDK（数据模型 + Schema/错误映射）；
//!   * `sdk/java/` 下的教学级 Java SDK（不可变数据模型 + 元数据访问器）。
//! - 错误处理：任何读写或解析失败均直接 panic，并携带文件路径上下文，便于 CI 快速定位。
//!
//! # 实现总览（How）
//! 1. 使用 [`serde`] 解析 TOML 契约为强类型结构，复用构建脚本同款字段枚举；
//! 2. 将结构体/事件字段映射为 JSON Schema 组件，并结合错误矩阵生成 `x-errorMatrix` 扩展；
//! 3. 基于 Schema 渲染 Python `dataclass` 与 Java 不可变 POJO，附带富含 Why/How/What 的文档字符串；
//! 4. 写入生成产物时使用 `fs::create_dir_all` 保证目录存在，同时统一采用 UTF-8 与 pretty JSON，便于审查 diff。

use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::fmt::Write as _;

/// 配置事件合约的顶层结构定义。
#[derive(Debug, Deserialize)]
struct ConfigEventsContract {
    version: String,
    summary: String,
    #[serde(default)]
    structs: Vec<EventStructSpec>,
    events: Vec<ConfigEventSpec>,
}

/// 嵌套结构体声明。
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

/// 单个事件的声明。
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

/// 错误矩阵合约：顶层为行集合。
#[derive(Debug, Deserialize)]
struct ErrorMatrixContract {
    rows: Vec<ErrorMatrixRow>,
}

/// 单行可能包含多个错误码及共享分类。
#[derive(Debug, Deserialize)]
struct ErrorMatrixRow {
    codes: Vec<String>,
    category: CategoryTemplateSpec,
    doc: ErrorDocSpec,
}

/// 错误矩阵的文档说明。
#[derive(Debug, Deserialize)]
struct ErrorDocSpec {
    rationale: String,
    tuning: String,
}

/// 错误分类模板声明式表示。
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CategoryTemplateSpec {
    Retryable {
        wait_ms: u64,
        reason: String,
        #[serde(default)]
        busy: Option<BusyDispositionSpec>,
    },
    Timeout,
    ProtocolViolation {
        close_message: String,
    },
    ResourceExhausted {
        budget: BudgetDispositionSpec,
    },
    Cancelled,
    NonRetryable,
    Security {
        class: SecurityClassSpec,
    },
}

/// Busy 方向声明。
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum BusyDispositionSpec {
    Upstream,
    Downstream,
}

/// 预算类型声明。
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum BudgetDispositionSpec {
    Decode,
    Flow,
}

/// 安全分类声明。
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum SecurityClassSpec {
    Authentication,
    Authorization,
    Confidentiality,
    Integrity,
    Audit,
    Unknown,
}

/// 展开后的错误矩阵条目：每个错误码独立存储。
#[derive(Debug, Clone)]
struct ErrorMatrixEntry {
    code: String,
    category: CategoryTemplateSpec,
    rationale: String,
    tuning: String,
}

/// Schema / AsyncAPI 自描述元信息：用于在 JSON Schema 中保留摘要、细节与设计缘由。
struct SchemaDocMeta<'a> {
    /// 摘要标题：供 JSON Schema `title` 与扩展字段复用。
    title: &'a str,
    /// 细化描述：映射到 JSON Schema `description`，便于生成 SDK 注释。
    detail: Option<&'a str>,
    /// 设计缘由/教学动机：以 `x-rationale` 保存，帮助跨语言团队理解设计取舍。
    rationale: Option<&'a str>,
}

fn main() {
    // 1. 解析输入路径：以 `CARGO_MANIFEST_DIR`（即 spark-core crate 根目录）为锚点，定位仓库根。
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .expect("spark-core crate 位于仓库子目录内")
        .to_path_buf();

    // 2. 加载配置事件与错误矩阵的契约，若失败立即 panic 并包含路径上下文。
    let config_contract: ConfigEventsContract =
        read_toml(&repo_root.join("contracts/config_events.toml"));
    let error_contract: ErrorMatrixContract =
        read_toml(&repo_root.join("contracts/error_matrix.toml"));
    let error_entries = expand_error_entries(&error_contract);

    // 3. 生成 JSON Schema 与 AsyncAPI。
    let schema_json = render_json_schema(&config_contract, &error_entries);
    let asyncapi_json = render_asyncapi(&config_contract, &error_entries);

    let schema_dir = repo_root.join("schemas");
    ensure_dir(&schema_dir);
    write_pretty_json(
        &schema_dir.join("configuration-events.schema.json"),
        &schema_json,
    );
    write_pretty_json(
        &schema_dir.join("configuration-events.asyncapi.json"),
        &asyncapi_json,
    );

    // 3.1 以 Schema/AsyncAPI 作为 SOT，重建契约供跨语言 SDK 使用。
    let sdk_contract = reconstruct_contract_from_sot(&schema_json, &asyncapi_json);

    // 4. 生成 Python SDK（dataclass + 元数据 + Schema 访问器）。
    let python_dir = repo_root.join("sdk/python/spark_config_sdk");
    ensure_dir(&python_dir);
    let python_init = render_python_init();
    let python_events = render_python_events(&sdk_contract);
    let python_errors = render_python_errors(&error_entries);
    let python_schemas = render_python_schemas(&schema_json, &asyncapi_json);
    fs::write(python_dir.join("__init__.py"), python_init).expect("写入 Python __init__.py 失败");
    fs::write(python_dir.join("events.py"), python_events).expect("写入 Python events.py 失败");
    fs::write(python_dir.join("errors.py"), python_errors).expect("写入 Python errors.py 失败");
    fs::write(python_dir.join("schemas.py"), python_schemas).expect("写入 Python schemas.py 失败");

    // 4.1 Python 项目元数据：生成简洁 pyproject，便于在 TCK 中以 editable 方式引入。
    let python_project_dir = repo_root.join("sdk/python");
    ensure_dir(&python_project_dir);
    fs::write(
        python_project_dir.join("pyproject.toml"),
        render_python_pyproject(&sdk_contract.version),
    )
    .expect("写入 Python pyproject.toml 失败");

    // 5. 生成 Java SDK：主类 + 构建脚手架。
    let java_src_dir = repo_root.join("sdk/java/src/main/java/com/spark/config");
    ensure_dir(&java_src_dir);
    fs::write(
        java_src_dir.join("ConfigurationEvents.java"),
        render_java_configuration_events(
            &sdk_contract,
            &error_entries,
            &schema_json,
            &asyncapi_json,
        ),
    )
    .expect("写入 Java ConfigurationEvents.java 失败");

    let java_project_dir = repo_root.join("sdk/java");
    ensure_dir(&java_project_dir.join("src/main/java"));
    fs::write(
        java_project_dir.join("build.gradle"),
        render_java_build_gradle(),
    )
    .expect("写入 Java build.gradle 失败");
    fs::write(
        java_project_dir.join("settings.gradle"),
        render_java_settings_gradle(),
    )
    .expect("写入 Java settings.gradle 失败");
}

/// 读取指定路径的 TOML 文档。
///
/// # Why
/// - 统一错误处理，避免在多个调用点重复书写 `fs::read_to_string` + `toml::from_str`。
///
/// # What
/// - 输入：目标文件路径；返回：类型参数 `T` 对应的结构。
/// - 前置条件：文件存在且内容符合 `T` 的反序列化约束。
/// - 后置条件：若解析成功返回结构体；失败则 panic 并输出路径与错误信息。
fn read_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    let raw = fs::read_to_string(path).unwrap_or_else(|err| {
        panic!("读取 {:?} 失败: {}", path, err);
    });
    toml::from_str(&raw).unwrap_or_else(|err| panic!("解析 {:?} 失败: {}", path, err))
}

/// 展开错误矩阵：将共享分类的多错误码拆分为独立条目。
///
/// # How
/// - 遍历每一行，复制 `category` 与文档信息到每个错误码；
/// - 使用 `BTreeMap` 进行排序，确保输出稳定，便于生成器产生可重复的 diff。
fn expand_error_entries(contract: &ErrorMatrixContract) -> Vec<ErrorMatrixEntry> {
    let mut entries = Vec::new();
    for row in &contract.rows {
        for code in &row.codes {
            entries.push(ErrorMatrixEntry {
                code: code.clone(),
                category: row.category.clone(),
                rationale: row.doc.rationale.clone(),
                tuning: row.doc.tuning.clone(),
            });
        }
    }
    entries.sort_by(|a, b| a.code.cmp(&b.code));
    entries
}

/// 根据契约渲染 JSON Schema。
fn render_json_schema(contract: &ConfigEventsContract, errors: &[ErrorMatrixEntry]) -> Value {
    let mut defs = Map::new();

    // 先写入结构体定义，保留摘要/细节/缘由，供事件引用与跨语言 SDK 注释复用。
    for st in &contract.structs {
        defs.insert(
            st.ident.clone(),
            schema_for_fields(
                &st.fields,
                Some(SchemaDocMeta {
                    title: &st.summary,
                    detail: Some(&st.description),
                    rationale: Some(&st.rationale),
                }),
            ),
        );
    }

    // 事件定义同样落入 $defs，确保 Schema 自描述足以驱动 SDK 生成。
    for event in &contract.events {
        defs.insert(
            event.ident.clone(),
            schema_for_fields(
                &event.fields,
                Some(SchemaDocMeta {
                    title: &event.summary,
                    detail: Some(&event.description),
                    rationale: None,
                }),
            ),
        );
    }

    let mut event_payloads = Map::new();
    let mut event_required = Vec::new();
    for event in &contract.events {
        event_payloads.insert(
            event.ident.clone(),
            json!({
                "$ref": format!("#/$defs/{}", event.ident),
                "description": event.summary,
            }),
        );
        event_required.push(Value::String(event.ident.clone()));
    }

    // 错误矩阵扩展，以对象形式提供详尽说明。
    let mut error_map = Map::new();
    for entry in errors {
        error_map.insert(
            entry.code.clone(),
            category_to_json(&entry.category, &entry.rationale, &entry.tuning),
        );
    }

    let mut events_obj = Map::new();
    events_obj.insert("type".to_string(), Value::String("object".to_string()));
    events_obj.insert(
        "description".to_string(),
        Value::String("事件 payload 的定义集合，键为事件结构体 ident。".to_string()),
    );
    events_obj.insert("properties".to_string(), Value::Object(event_payloads));
    events_obj.insert("required".to_string(), Value::Array(event_required));
    events_obj.insert("additionalProperties".to_string(), Value::Bool(false));
    events_obj.insert(
        "x-eventOrder".to_string(),
        Value::Array(
            contract
                .events
                .iter()
                .map(|event| Value::String(event.ident.clone()))
                .collect(),
        ),
    );

    let mut properties = Map::new();
    properties.insert(
        "version".to_string(),
        json!({"type": "string", "const": contract.version}),
    );
    properties.insert("events".to_string(), Value::Object(events_obj));

    let mut root = Map::new();
    root.insert(
        "$schema".to_string(),
        Value::String("https://json-schema.org/draft/2020-12/schema".to_string()),
    );
    root.insert(
        "$id".to_string(),
        Value::String(
            "https://github.com/spark-rs/spark2026/configuration-events.schema.json".to_string(),
        ),
    );
    root.insert(
        "title".to_string(),
        Value::String("Spark Configuration Events".to_string()),
    );
    root.insert(
        "description".to_string(),
        Value::String(contract.summary.clone()),
    );
    root.insert("$defs".to_string(), Value::Object(defs));
    root.insert("type".to_string(), Value::String("object".to_string()));
    root.insert("properties".to_string(), Value::Object(properties));
    root.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("version".to_string()),
            Value::String("events".to_string()),
        ]),
    );
    root.insert("x-errorMatrix".to_string(), Value::Object(error_map));
    root.insert(
        "x-structOrder".to_string(),
        Value::Array(
            contract
                .structs
                .iter()
                .map(|st| Value::String(st.ident.clone()))
                .collect(),
        ),
    );

    Value::Object(root)
}

/// 将字段集合转换为 JSON Schema。
fn schema_for_fields(fields: &[EventFieldSpec], doc: Option<SchemaDocMeta<'_>>) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();
    let mut order = Vec::new();
    for field in fields {
        order.push(Value::String(field.name.clone()));
        properties.insert(
            field.name.clone(),
            field_type_to_schema(&field.ty, &field.doc),
        );
        if field.required {
            required.push(Value::String(field.name.clone()));
        }
    }

    let mut object = Map::new();
    object.insert("type".to_string(), Value::String("object".to_string()));
    object.insert("properties".to_string(), Value::Object(properties));
    object.insert("additionalProperties".to_string(), Value::Bool(false));
    object.insert("required".to_string(), Value::Array(required));
    object.insert("x-fieldOrder".to_string(), Value::Array(order));

    if let Some(doc) = doc {
        object.insert("title".to_string(), Value::String(doc.title.to_string()));
        object.insert(
            "x-summary".to_string(),
            Value::String(doc.title.to_string()),
        );
        let detail = doc.detail.unwrap_or(doc.title);
        object.insert("description".to_string(), Value::String(detail.to_string()));
        if let Some(detail) = doc.detail {
            object.insert("x-detail".to_string(), Value::String(detail.to_string()));
        }
        if let Some(rationale) = doc.rationale {
            object.insert(
                "x-rationale".to_string(),
                Value::String(rationale.to_string()),
            );
        }
    }

    Value::Object(object)
}

/// 将字段类型映射为 JSON Schema 片段。
fn field_type_to_schema(ty: &EventFieldTypeSpec, doc: &str) -> Value {
    match ty {
        EventFieldTypeSpec::String => json!({"type": "string", "description": doc}),
        EventFieldTypeSpec::U64 => {
            json!({"type": "integer", "format": "uint64", "description": doc})
        }
        EventFieldTypeSpec::Bool => json!({"type": "boolean", "description": doc}),
        EventFieldTypeSpec::Struct { ident } => {
            json!({"$ref": format!("#/$defs/{}", ident), "description": doc})
        }
        EventFieldTypeSpec::List { item } => json!({
            "type": "array",
            "description": doc,
            "items": field_type_to_schema(item, doc),
        }),
    }
}

/// 将分类模板转换为 JSON 对象，并附带文档说明。
fn category_to_json(category: &CategoryTemplateSpec, rationale: &str, tuning: &str) -> Value {
    let mut obj = Map::new();
    obj.insert(
        "rationale".to_string(),
        Value::String(rationale.to_string()),
    );
    obj.insert("tuning".to_string(), Value::String(tuning.to_string()));
    match category {
        CategoryTemplateSpec::Retryable {
            wait_ms,
            reason,
            busy,
        } => {
            obj.insert("kind".to_string(), Value::String("retryable".to_string()));
            obj.insert("wait_ms".to_string(), Value::Number((*wait_ms).into()));
            obj.insert("reason".to_string(), Value::String(reason.clone()));
            if let Some(busy) = busy {
                obj.insert(
                    "busy".to_string(),
                    Value::String(
                        match busy {
                            BusyDispositionSpec::Upstream => "upstream",
                            BusyDispositionSpec::Downstream => "downstream",
                        }
                        .to_string(),
                    ),
                );
            }
        }
        CategoryTemplateSpec::Timeout => {
            obj.insert("kind".to_string(), Value::String("timeout".to_string()));
        }
        CategoryTemplateSpec::ProtocolViolation { close_message } => {
            obj.insert(
                "kind".to_string(),
                Value::String("protocol_violation".to_string()),
            );
            obj.insert(
                "close_message".to_string(),
                Value::String(close_message.clone()),
            );
        }
        CategoryTemplateSpec::ResourceExhausted { budget } => {
            obj.insert(
                "kind".to_string(),
                Value::String("resource_exhausted".to_string()),
            );
            obj.insert(
                "budget".to_string(),
                Value::String(
                    match budget {
                        BudgetDispositionSpec::Decode => "decode",
                        BudgetDispositionSpec::Flow => "flow",
                    }
                    .to_string(),
                ),
            );
        }
        CategoryTemplateSpec::Cancelled => {
            obj.insert("kind".to_string(), Value::String("cancelled".to_string()));
        }
        CategoryTemplateSpec::NonRetryable => {
            obj.insert(
                "kind".to_string(),
                Value::String("non_retryable".to_string()),
            );
        }
        CategoryTemplateSpec::Security { class } => {
            obj.insert("kind".to_string(), Value::String("security".to_string()));
            obj.insert(
                "class".to_string(),
                Value::String(
                    match class {
                        SecurityClassSpec::Authentication => "authentication",
                        SecurityClassSpec::Authorization => "authorization",
                        SecurityClassSpec::Confidentiality => "confidentiality",
                        SecurityClassSpec::Integrity => "integrity",
                        SecurityClassSpec::Audit => "audit",
                        SecurityClassSpec::Unknown => "unknown",
                    }
                    .to_string(),
                ),
            );
        }
    }
    Value::Object(obj)
}

/// 渲染 AsyncAPI 文档。
fn render_asyncapi(contract: &ConfigEventsContract, errors: &[ErrorMatrixEntry]) -> Value {
    let mut schemas = Map::new();
    let mut messages = Map::new();
    let mut channels = Map::new();

    // 复用 JSON Schema 逻辑构建结构体定义。
    for st in &contract.structs {
        schemas.insert(
            st.ident.clone(),
            schema_for_fields(
                &st.fields,
                Some(SchemaDocMeta {
                    title: &st.summary,
                    detail: Some(&st.description),
                    rationale: Some(&st.rationale),
                }),
            ),
        );
    }

    for event in &contract.events {
        schemas.insert(
            event.ident.clone(),
            schema_for_fields(
                &event.fields,
                Some(SchemaDocMeta {
                    title: &event.summary,
                    detail: Some(&event.description),
                    rationale: None,
                }),
            ),
        );

        let message = json!({
            "name": event.code,
            "title": event.name,
            "summary": event.summary,
            "description": event.description,
            "contentType": "application/json",
            "payload": {"$ref": format!("#/components/schemas/{}", event.ident)},
            "tags": [
                {"name": format!("family:{}", event.family)},
                {"name": format!("severity:{}", event.severity)},
            ],
            "x-audit": {
                "action": event.audit.action,
                "entity_kind": event.audit.entity_kind,
                "entity_id_field": event.audit.entity_id_field,
            },
            "x-drills": event.drills.iter().map(|drill| json!({
                "title": drill.title,
                "goal": drill.goal,
                "setup": drill.setup,
                "steps": drill.steps,
                "expectations": drill.expectations,
            })).collect::<Vec<_>>()
        });
        messages.insert(event.ident.clone(), message);

        channels.insert(
            event.code.clone(),
            json!({
                "publish": {
                    "summary": format!("发布 {} 事件", event.name),
                    "message": {"$ref": format!("#/components/messages/{}", event.ident)},
                }
            }),
        );
    }

    let mut error_map = Map::new();
    for entry in errors {
        error_map.insert(
            entry.code.clone(),
            category_to_json(&entry.category, &entry.rationale, &entry.tuning),
        );
    }

    json!({
        "asyncapi": "2.6.0",
        "info": {
            "title": "Spark Configuration Events",
            "version": contract.version,
            "description": contract.summary,
        },
        "defaultContentType": "application/json",
        "channels": channels,
        "components": {
            "schemas": schemas,
            "messages": messages,
        },
        "x-errorMatrix": error_map,
    })
}

/// 确保目录存在。
fn ensure_dir(path: &Path) {
    fs::create_dir_all(path).unwrap_or_else(|err| panic!("创建目录 {:?} 失败: {}", path, err));
}

/// 将 JSON 以缩进格式写入磁盘。
fn write_pretty_json(path: &Path, value: &Value) {
    let data = serde_json::to_string_pretty(value).expect("序列化 JSON 失败");
    fs::write(path, data).unwrap_or_else(|err| panic!("写入 {:?} 失败: {}", path, err));
}

/// 从落盘的 JSON Schema 与 AsyncAPI 文档中重建事件契约结构体。
///
/// # Why
/// - 将 Schema/AsyncAPI 视为跨语言 SOT，SDK 渲染必须完全依赖其内容，避免继续读取 TOML 合约造成事实来源混用；
/// - 生成器在同一进程内先写入 SOT，再立刻解析，可在 CI 中保证 JSON 文档自洽且包含所有元数据。
///
/// # What
/// - 输入：`schema_json` 为 JSON Schema、`asyncapi_json` 为 AsyncAPI；
/// - 输出：`ConfigEventsContract` 结构体，字段顺序依据 Schema 中的 `x-structOrder` 与 `x-eventOrder`，字段顺序依据各定义的
///   `x-fieldOrder`，确保与生成器预期一致。
/// - 前置条件：Schema 必须包含 `x-structOrder`、`x-eventOrder` 等扩展字段；AsyncAPI 需要提供消息摘要、标签、审计与演练扩展。
/// - 后置条件：返回的结构可直接传入后续 Python/Java 渲染函数，且其数据完全来源于 SOT。
///
/// # How
/// 1. 解析 AsyncAPI `info` 得到版本与总览描述；
/// 2. 读取 Schema `$defs` 并依据扩展顺序拆分结构体/事件定义；
/// 3. 对于每个事件，结合 AsyncAPI `components.messages` 拿到 code/family/severity/audit/drills；
/// 4. 字段定义通过 `parse_fields_from_schema` 解包 `x-fieldOrder` 与属性类型描述。
fn reconstruct_contract_from_sot(
    schema_json: &Value,
    asyncapi_json: &Value,
) -> ConfigEventsContract {
    let info = asyncapi_json
        .get("info")
        .and_then(|v| v.as_object())
        .expect("AsyncAPI 缺少 info 对象");
    let version = info
        .get("version")
        .and_then(|v| v.as_str())
        .expect("AsyncAPI info.version 缺失")
        .to_string();
    let summary = info
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    let defs = schema_json
        .get("$defs")
        .and_then(|v| v.as_object())
        .expect("JSON Schema 缺少 $defs 对象");

    let struct_order = schema_json
        .get("x-structOrder")
        .and_then(|v| v.as_array())
        .expect("JSON Schema 缺少 x-structOrder");

    let events_obj = schema_json
        .get("properties")
        .and_then(|v| v.get("events"))
        .and_then(|v| v.as_object())
        .expect("JSON Schema 缺少 events 属性");
    let event_order = events_obj
        .get("x-eventOrder")
        .and_then(|v| v.as_array())
        .expect("JSON Schema 缺少 x-eventOrder");
    let event_ident_set: BTreeSet<String> = event_order
        .iter()
        .map(|v| {
            v.as_str()
                .expect("x-eventOrder 元素必须为字符串")
                .to_string()
        })
        .collect();

    let messages = asyncapi_json
        .get("components")
        .and_then(|v| v.get("messages"))
        .and_then(|v| v.as_object())
        .expect("AsyncAPI 缺少 components.messages");

    let mut structs = Vec::new();
    for ident_value in struct_order {
        let ident = ident_value
            .as_str()
            .expect("x-structOrder 元素必须为字符串");
        if event_ident_set.contains(ident) {
            continue;
        }
        let def = defs
            .get(ident)
            .unwrap_or_else(|| panic!("$defs 缺少结构体定义 {}", ident));
        let fields = parse_fields_from_schema(def, ident);
        let summary = def
            .get("x-summary")
            .or_else(|| def.get("title"))
            .and_then(|v| v.as_str())
            .unwrap_or(ident)
            .to_string();
        let description = def
            .get("x-detail")
            .or_else(|| def.get("description"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let rationale = def
            .get("x-rationale")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        structs.push(EventStructSpec {
            ident: ident.to_string(),
            summary,
            rationale,
            description,
            fields,
        });
    }

    let mut events = Vec::new();
    for ident_value in event_order {
        let ident = ident_value.as_str().expect("x-eventOrder 元素必须为字符串");
        let def = defs
            .get(ident)
            .unwrap_or_else(|| panic!("$defs 缺少事件定义 {}", ident));
        let fields = parse_fields_from_schema(def, ident);

        let message = messages
            .get(ident)
            .and_then(|v| v.as_object())
            .unwrap_or_else(|| panic!("AsyncAPI 缺少事件 {} 的消息定义", ident));
        let code = message
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("事件 {} 缺少 name", ident))
            .to_string();
        let name = message
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("事件 {} 缺少 title", ident))
            .to_string();
        let summary_text = message
            .get("summary")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let description = message
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let mut family = None;
        let mut severity = None;
        if let Some(tags) = message.get("tags").and_then(|v| v.as_array()) {
            for tag in tags {
                if let Some(name) = tag.get("name").and_then(|v| v.as_str()) {
                    if let Some(rest) = name.strip_prefix("family:") {
                        family = Some(rest.to_string());
                    } else if let Some(rest) = name.strip_prefix("severity:") {
                        severity = Some(rest.to_string());
                    }
                }
            }
        }
        let family = family.unwrap_or_else(|| panic!("事件 {} 缺少 family 标签", ident));
        let severity = severity.unwrap_or_else(|| panic!("事件 {} 缺少 severity 标签", ident));

        let audit = message
            .get("x-audit")
            .and_then(|v| v.as_object())
            .unwrap_or_else(|| panic!("事件 {} 缺少 x-audit", ident));
        let audit_spec = EventAuditSpec {
            action: audit
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("事件 {} 的 x-audit 缺少 action", ident))
                .to_string(),
            entity_kind: audit
                .get("entity_kind")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("事件 {} 的 x-audit 缺少 entity_kind", ident))
                .to_string(),
            entity_id_field: audit
                .get("entity_id_field")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("事件 {} 的 x-audit 缺少 entity_id_field", ident))
                .to_string(),
        };

        let drills = message
            .get("x-drills")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|entry| {
                        let obj = entry.as_object().expect("x-drills 元素必须为对象");
                        EventDrillSpec {
                            title: obj
                                .get("title")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            goal: obj
                                .get("goal")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            setup: obj
                                .get("setup")
                                .and_then(|v| v.as_array())
                                .map(|items| {
                                    items
                                        .iter()
                                        .map(|v| {
                                            v.as_str()
                                                .expect("x-drills.setup 元素必须为字符串")
                                                .to_string()
                                        })
                                        .collect()
                                })
                                .unwrap_or_default(),
                            steps: obj
                                .get("steps")
                                .and_then(|v| v.as_array())
                                .map(|items| {
                                    items
                                        .iter()
                                        .map(|v| {
                                            v.as_str()
                                                .expect("x-drills.steps 元素必须为字符串")
                                                .to_string()
                                        })
                                        .collect()
                                })
                                .unwrap_or_default(),
                            expectations: obj
                                .get("expectations")
                                .and_then(|v| v.as_array())
                                .map(|items| {
                                    items
                                        .iter()
                                        .map(|v| {
                                            v.as_str()
                                                .expect("x-drills.expectations 元素必须为字符串")
                                                .to_string()
                                        })
                                        .collect()
                                })
                                .unwrap_or_default(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        events.push(ConfigEventSpec {
            ident: ident.to_string(),
            code,
            family,
            name,
            severity,
            summary: summary_text,
            description,
            audit: audit_spec,
            fields,
            drills,
        });
    }

    ConfigEventsContract {
        version,
        summary,
        structs,
        events,
    }
}

/// 从 JSON Schema 定义中解析字段集合。
///
/// # 契约解释（What）
/// - `definition`：Schema `$defs` 下的单个对象；`context`：调试输出使用的标识。
/// - 返回：保持 `x-fieldOrder` 中声明顺序的 `EventFieldSpec` 列表。
///
/// # 实现要点（How）
/// - 读取 `properties` 映射与 `required` 列表构建字段；
/// - 通过 `x-fieldOrder` 维持字段顺序；
/// - 调用 `parse_field_type_from_schema` 递归解析类型，包括数组与 `$ref` 嵌套。
fn parse_fields_from_schema(definition: &Value, context: &str) -> Vec<EventFieldSpec> {
    let properties = definition
        .get("properties")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("{} 缺少 properties", context));
    let required: BTreeSet<String> = definition
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|value| {
                    value
                        .as_str()
                        .expect("required 列表必须为字符串")
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default();
    let order = definition
        .get("x-fieldOrder")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("{} 缺少 x-fieldOrder", context));

    let mut fields = Vec::new();
    for name_value in order {
        let name = name_value.as_str().expect("x-fieldOrder 元素必须为字符串");
        let prop = properties
            .get(name)
            .unwrap_or_else(|| panic!("{} 缺少字段 {}", context, name));
        let doc = prop
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let field_type = parse_field_type_from_schema(prop, &format!("{}::{}", context, name));
        fields.push(EventFieldSpec {
            name: name.to_string(),
            required: required.contains(name),
            ty: field_type,
            doc,
        });
    }
    fields
}

/// 解析单个字段的 JSON Schema 类型定义。
///
/// # Why
/// - Python/Java SDK 类型信息完全来自 Schema，必须覆盖 `$ref`、标量与数组三类情况。
///
/// # How
/// - 优先识别 `$ref` 并提取引用标识；
/// - 解析 `type` + `format` 对于整数/布尔/字符串；
/// - 针对 `array` 递归解析 `items` 字段。
fn parse_field_type_from_schema(node: &Value, context: &str) -> EventFieldTypeSpec {
    if let Some(reference) = node.get("$ref").and_then(|v| v.as_str()) {
        let ident = reference
            .rsplit('/')
            .next()
            .expect("$ref 应包含标识")
            .to_string();
        return EventFieldTypeSpec::Struct { ident };
    }

    if let Some(kind) = node.get("type").and_then(|v| v.as_str()) {
        match kind {
            "string" => return EventFieldTypeSpec::String,
            "boolean" => return EventFieldTypeSpec::Bool,
            "integer" => {
                let format = node
                    .get("format")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("{} 缺少整数 format", context));
                if format == "uint64" {
                    return EventFieldTypeSpec::U64;
                }
                panic!("{} 暂不支持整数 format {}", context, format);
            }
            "array" => {
                let items = node
                    .get("items")
                    .unwrap_or_else(|| panic!("{} 缺少 array items", context));
                let item_type = parse_field_type_from_schema(items, &format!("{}[]", context));
                return EventFieldTypeSpec::List {
                    item: Box::new(item_type),
                };
            }
            other => panic!("{} 使用了未支持的类型 {}", context, other),
        }
    }

    panic!("{} 缺少可识别的类型声明", context);
}

/// 生成 Python 包入口。
fn render_python_init() -> String {
    concat!(
        "\"\"\"Spark 配置事件 SDK 顶层包。\n",
        "\n",
        "Why:\n",
        "    - 暴露跨语言共享的事件数据模型、Schema 与错误分类映射，供 TCK 与业务代码复用。\n",
        "    - 通过 `__all__` 限定公开接口，避免内部生成细节泄露至调用方。\n",
        "\n",
        "What:\n",
        "    - `events`: 自动生成的 dataclass 定义与事件元数据。\n",
        "    - `errors`: 错误码到分类模板的映射。\n",
        "    - `schemas`: JSON Schema 与 AsyncAPI 访问器。\n",
        "\n",
        "How:\n",
        "    - 由 `tools/gen_config_events_artifacts.rs` 根据 schemas/ 下的 JSON Schema 与 AsyncAPI SOT 生成，不应手工编辑。\n",
        "\"\"\"\n\n",
        "from .events import EVENT_PAYLOAD_TYPES, EVENT_DESCRIPTORS\n",
        "from .errors import ERROR_MATRIX\n",
        "from .schemas import load_json_schema, load_asyncapi\n\n",
        "__all__ = [\n",
        "    \"EVENT_PAYLOAD_TYPES\",\n",
        "    \"EVENT_DESCRIPTORS\",\n",
        "    \"ERROR_MATRIX\",\n",
        "    \"load_json_schema\",\n",
        "    \"load_asyncapi\",\n",
        "]\n"
    )
    .to_string()
}

/// 渲染 Python dataclass 定义与事件描述符。
fn render_python_events(contract: &ConfigEventsContract) -> String {
    let mut buf = String::new();
    buf.push_str("\"\"\"\n");
    buf.push_str("配置事件数据模型：自动生成的 dataclass 定义。\n\n");
    buf.push_str("Why:\n");
    buf.push_str("    - 为客户端与 TCK 提供强类型载体，确保字段与审计映射与 SOT 一致。\n");
    buf.push_str("How:\n");
    buf.push_str("    - 由生成器解析 schemas/ 下的 JSON Schema 与 AsyncAPI SOT 渲染；字段说明嵌入在 docstring 中，支持教学级自解释。\n");
    buf.push_str("What:\n");
    buf.push_str("    - `STRUCT_TYPES`: 复用结构体定义；\n");
    buf.push_str("    - `EVENT_PAYLOAD_TYPES`: 事件 payload dataclass；\n");
    buf.push_str("    - `EVENT_DESCRIPTORS`: 元数据字典，含审计映射与演练用例。\n\n");
    buf.push_str("注意：文件由生成器维护，请勿手工编辑。\n\"\"\"\n\n");
    buf.push_str("from __future__ import annotations\n\n");
    buf.push_str("from dataclasses import dataclass, field\n");
    buf.push_str("from typing import Dict, List, Optional, Type\n\n");

    // 先生成嵌套结构体。
    for st in &contract.structs {
        buf.push_str(&format!(
            "@dataclass\nclass {ident}:\n    \"\"\"\n    Why:\n        {summary}\n    What:\n        {description}\n    Rationale:\n        {rationale}\n    How:\n        - 由 SOT 自动生成；使用者仅需按字段填充数据。\n    Pre-conditions:\n        - 调用方需保证字段遵循契约规定的类型与业务约束。\n    Post-conditions:\n        - 数据可直接序列化为事件 payload，供审计/演练复现。\n    Trade-offs:\n        - 若需新增字段，请更新 schemas/ 下的 JSON Schema/AsyncAPI 并重新生成 SDK。\n    \"\"\"\n",
            ident = st.ident,
            summary = st.summary,
            description = st.description.replace('\n', "\n        "),
            rationale = st.rationale.replace('\n', "\n        "),
        ));

        if st.fields.is_empty() {
            buf.push_str("    pass\n\n");
        } else {
            for field in &st.fields {
                buf.push_str(&python_field_definition(field));
            }
            buf.push('\n');
        }
    }

    // 生成事件 payload。
    for event in &contract.events {
        buf.push_str(&format!(
            "@dataclass\nclass {ident}:\n    \"\"\"\n    Why:\n        {summary}\n    What:\n        {description}\n    How:\n        - 字段顺序源自 JSON Schema 的 x-fieldOrder，便于跨语言比对 diff。\n    Audit Mapping:\n        - action={action}\n        - entity_kind={entity_kind}\n        - entity_id_field={entity_id}\n    Trade-offs:\n        - 修改字段时需同时更新 SOT（Schema/AsyncAPI）、审计、演练及 SDK。\n    \"\"\"\n",
            ident = event.ident,
            summary = event.summary.replace('\n', "\n        "),
            description = event.description.replace('\n', "\n        "),
            action = event.audit.action,
            entity_kind = event.audit.entity_kind,
            entity_id = event.audit.entity_id_field,
        ));
        if event.fields.is_empty() {
            buf.push_str("    pass\n\n");
        } else {
            for field in &event.fields {
                buf.push_str(&python_field_definition(field));
            }
            buf.push('\n');
        }
    }

    // 构建结构体 / 事件类型映射。
    buf.push_str("STRUCT_TYPES: Dict[str, Type[object]] = {\n");
    for st in &contract.structs {
        buf.push_str(&format!("    \"{ident}\": {ident},\n", ident = st.ident));
    }
    buf.push_str("}\n\n");

    buf.push_str("EVENT_PAYLOAD_TYPES: Dict[str, Type[object]] = {\n");
    for event in &contract.events {
        buf.push_str(&format!("    \"{ident}\": {ident},\n", ident = event.ident));
    }
    buf.push_str("}\n\n");

    buf.push_str("EVENT_DESCRIPTORS: Dict[str, Dict[str, object]] = {\n");
    for event in &contract.events {
        let drills_value: Vec<Value> = event
            .drills
            .iter()
            .map(|d| {
                json!({
                    "title": d.title,
                    "goal": d.goal,
                    "setup": d.setup,
                    "steps": d.steps,
                    "expectations": d.expectations,
                })
            })
            .collect();
        let drills_json = serde_json::to_string(&drills_value).expect("序列化 drill 失败");
        buf.push_str(&format!(
            "    \"{code}\": {{\n        \"ident\": \"{ident}\",\n        \"family\": \"{family}\",\n        \"severity\": \"{severity}\",\n        \"name\": \"{name}\",\n        \"summary\": \"{summary}\",\n        \"description\": \"{description}\",\n        \"audit\": {{\n            \"action\": \"{action}\",\n            \"entity_kind\": \"{entity_kind}\",\n            \"entity_id_field\": \"{entity_id}\"\n        }},\n        \"drills\": {drills}\n    }},\n",
            code = event.code,
            ident = event.ident,
            family = event.family,
            severity = event.severity,
            name = event.name,
            summary = event.summary.replace('"', "\\\""),
            description = event.description.replace('"', "\\\""),
            action = event.audit.action,
            entity_kind = event.audit.entity_kind,
            entity_id = event.audit.entity_id_field,
            drills = drills_json,
        ));
    }
    buf.push_str("}\n");

    buf
}

/// 生成 Python dataclass 字段定义。
fn python_field_definition(field: &EventFieldSpec) -> String {
    let (type_hint, default) = python_type_hint(&field.ty, field.required);
    let mut line = String::new();
    line.push_str("    ");
    line.push_str(&field.name);
    line.push_str(": ");
    line.push_str(&type_hint);
    if let Some(default) = default {
        line.push_str(" = ");
        line.push_str(&default);
    }
    line.push_str(&format!("  # {doc}\n", doc = field.doc));
    line
}

/// 根据字段类型推导 Python 类型提示与默认值。
fn python_type_hint(ty: &EventFieldTypeSpec, required: bool) -> (String, Option<String>) {
    match ty {
        EventFieldTypeSpec::String => (
            if required {
                "str".to_string()
            } else {
                "Optional[str]".to_string()
            },
            if required {
                None
            } else {
                Some("None".to_string())
            },
        ),
        EventFieldTypeSpec::U64 => (
            if required {
                "int".to_string()
            } else {
                "Optional[int]".to_string()
            },
            if required {
                None
            } else {
                Some("None".to_string())
            },
        ),
        EventFieldTypeSpec::Bool => (
            if required {
                "bool".to_string()
            } else {
                "Optional[bool]".to_string()
            },
            if required {
                None
            } else {
                Some("None".to_string())
            },
        ),
        EventFieldTypeSpec::Struct { ident } => (
            if required {
                ident.clone()
            } else {
                format!("Optional[{ident}]", ident = ident)
            },
            if required {
                None
            } else {
                Some("None".to_string())
            },
        ),
        EventFieldTypeSpec::List { item } => {
            let (item_hint, _) = python_type_hint(item, true);
            let base = format!("List[{item}]", item = item_hint);
            if required {
                (base, Some("field(default_factory=list)".to_string()))
            } else {
                (format!("Optional[{base}]"), Some("None".to_string()))
            }
        }
    }
}

/// 渲染 Python 错误矩阵字典。
fn render_python_errors(errors: &[ErrorMatrixEntry]) -> String {
    let mut buf = String::new();
    buf.push_str("\"\"\"\n错误矩阵映射：从错误码到分类模板。\n\nWhy:\n    - 客户端可在 TCK 中验证错误处理逻辑是否与服务端一致。\nHow:\n    - 由生成器读取 contracts/error_matrix.toml 自动产出；包含 Why/How 文档以便教学。\nWhat:\n    - ERROR_MATRIX: Dict[str, dict]，键为错误码，值含 kind/wait_ms 等字段。\n\"\"\"\n\n");
    buf.push_str("from typing import Dict\n\n");
    buf.push_str("ERROR_MATRIX: Dict[str, dict] = {\n");
    for entry in errors {
        let json_value = category_to_json(&entry.category, &entry.rationale, &entry.tuning);
        let serialized = serde_json::to_string(&json_value).expect("序列化错误矩阵失败");
        buf.push_str(&format!("    \"{}\": {},\n", entry.code, serialized));
    }
    buf.push_str("}\n");
    buf
}

/// 渲染 Python schema 访问器。
fn render_python_schemas(schema: &Value, asyncapi: &Value) -> String {
    let schema_str = serde_json::to_string_pretty(schema).expect("序列化 schema 失败");
    let asyncapi_str = serde_json::to_string_pretty(asyncapi).expect("序列化 asyncapi 失败");
    let schema_literal = schema_str.replace("'''", "\\'\\'\\'");
    let asyncapi_literal = asyncapi_str.replace("'''", "\\'\\'\\'");

    format!(
        "\"\"\"\nSchema 访问器：提供 JSON Schema 与 AsyncAPI 的懒加载封装。\n\nWhy:\n    - 确保测试与 SDK 使用者无需重新读取磁盘即可访问 SOT。\nHow:\n    - 将 JSON 文本嵌入常量，再通过 json.loads 转换为字典。\nWhat:\n    - load_json_schema() / load_asyncapi()。\n\"\"\"\n\nimport json\nfrom functools import lru_cache\nfrom typing import Any, Dict\n\n_SCHEMA_TEXT = r'''{schema}'''\n_ASYNCAPI_TEXT = r'''{asyncapi}'''\n\n@lru_cache(maxsize=1)\ndef load_json_schema() -> Dict[str, Any]:\n    \"\"\"返回 JSON Schema 的深拷贝副本，保障调用方不会修改内部常量。\"\"\"\n    return json.loads(_SCHEMA_TEXT)\n\n@lru_cache(maxsize=1)\ndef load_asyncapi() -> Dict[str, Any]:\n    \"\"\"返回 AsyncAPI 文档的深拷贝副本。\"\"\"\n    return json.loads(_ASYNCAPI_TEXT)\n",
        schema = schema_literal,
        asyncapi = asyncapi_literal,
    )
}

/// 生成 Python 项目 pyproject。
fn render_python_pyproject(version: &str) -> String {
    format!(
        "[project]\nname = \"spark-config-sdk\"\nversion = \"{version}\"\ndescription = \"Spark configuration event SDK generated from SOT\"\nrequires-python = \">=3.9\"\ndependencies = []\n\n[build-system]\nrequires = [\"setuptools\"]\nbuild-backend = \"setuptools.build_meta\"\n",
        version = version
    )
}

/// 渲染 Java 主类。
fn render_java_configuration_events(
    contract: &ConfigEventsContract,
    errors: &[ErrorMatrixEntry],
    schema: &Value,
    asyncapi: &Value,
) -> String {
    let schema_str = serde_json::to_string_pretty(schema).expect("schema JSON");
    let asyncapi_str = serde_json::to_string_pretty(asyncapi).expect("asyncapi JSON");

    let mut buf = String::new();
    writeln!(buf, "package com.spark.config;\n").unwrap();
    writeln!(
        buf,
        "import java.util.Collections;\nimport java.util.HashMap;\nimport java.util.List;\nimport java.util.Map;\n"
    )
    .unwrap();
    writeln!(
        buf,
        "/**\n * Spark 配置事件 Java SDK：由 SOT 自动生成。\n *\n * <p>Why：\n *   <ul>\n *     <li>提供不可变的数据模型与元数据，供客户端 TCK 断言。</li>\n *     <li>确保 Java 调用方使用的 Schema 与服务端完全一致。</li>\n *   </ul>\n * How：\n *   <ul>\n *     <li>由 tools/gen_config_events_artifacts.rs 解析 schemas/ 下的 JSON Schema 与 AsyncAPI SOT 渲染。</li>\n *     <li>所有字段以 final 表示不可变，便于线程安全使用。</li>\n *   </ul>\n * What：\n *   <ul>\n *     <li>内部静态类表示事件载荷；</li>\n *     <li>EVENT_DESCRIPTORS / ERROR_MATRIX 提供元数据；</li>\n *     <li>jsonSchema() / asyncApi() 返回规范文本。</li>\n *   </ul>\n */"
    )
    .unwrap();
    writeln!(buf, "public final class ConfigurationEvents {{").unwrap();
    writeln!(buf, "").unwrap();
    writeln!(buf, "    private ConfigurationEvents() {{}}\n").unwrap();

    let schema_block = schema_str
        .lines()
        .map(|line| format!("        {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    writeln!(buf, "    /** JSON Schema 文本，保持不可变。 */").unwrap();
    writeln!(buf, "    private static final String JSON_SCHEMA = \"\"\"").unwrap();
    writeln!(buf, "{schema_block}").unwrap();
    writeln!(buf, "    \"\"\";\n").unwrap();

    let asyncapi_block = asyncapi_str
        .lines()
        .map(|line| format!("        {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    writeln!(buf, "    /** AsyncAPI 文本，保持不可变。 */").unwrap();
    writeln!(buf, "    private static final String ASYNCAPI = \"\"\"").unwrap();
    writeln!(buf, "{asyncapi_block}").unwrap();
    writeln!(buf, "    \"\"\";\n").unwrap();

    for event in &contract.events {
        writeln!(buf, "    /**").unwrap();
        writeln!(buf, "     * 事件 `{}` 的 payload 定义。", event.code).unwrap();
        writeln!(buf, "     *").unwrap();
        writeln!(buf, "     * <p>Why：{}", event.summary.replace('\n', " ")).unwrap();
        writeln!(buf, "     * <p>How：字段与 SOT 完全同步，不可手工修改。").unwrap();
        writeln!(buf, "     */").unwrap();
        writeln!(buf, "    public static final class {} {{", event.ident).unwrap();

        for field in &event.fields {
            writeln!(buf, "        /** {} */", field.doc.replace('\n', " ")).unwrap();
            writeln!(
                buf,
                "        private final {} {};",
                java_type_for_field(field),
                field.name
            )
            .unwrap();
        }

        let params = event
            .fields
            .iter()
            .map(|field| format!("{} {}", java_type_for_field(field), field.name))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(buf, "").unwrap();
        writeln!(buf, "        public {}({}) {{", event.ident, params).unwrap();
        for field in &event.fields {
            writeln!(buf, "            this.{0} = {0};", field.name).unwrap();
        }
        writeln!(buf, "        }}\n").unwrap();

        for field in &event.fields {
            writeln!(
                buf,
                "        public {} get{}() {{",
                java_type_for_field(field),
                to_camel_case(&field.name)
            )
            .unwrap();
            writeln!(buf, "            return this.{0};", field.name).unwrap();
            writeln!(buf, "        }}\n").unwrap();
        }

        writeln!(buf, "    }}\n").unwrap();
    }

    writeln!(
        buf,
        "    private static final Map<String, Map<String, Object>> EVENT_DESCRIPTORS;"
    )
    .unwrap();
    writeln!(
        buf,
        "    private static final Map<String, Map<String, Object>> ERROR_MATRIX;\n"
    )
    .unwrap();
    writeln!(buf, "    static {{").unwrap();
    writeln!(
        buf,
        "        Map<String, Map<String, Object>> descriptors = new HashMap<>();"
    )
    .unwrap();
    for event in &contract.events {
        let descriptor = json!({
            "ident": event.ident,
            "family": event.family,
            "severity": event.severity,
            "name": event.name,
            "summary": event.summary,
            "description": event.description,
            "audit": {
                "action": event.audit.action.clone(),
                "entity_kind": event.audit.entity_kind.clone(),
                "entity_id_field": event.audit.entity_id_field.clone(),
            },
            "drills": event
                .drills
                .iter()
                .map(|d| json!({
                    "title": d.title,
                    "goal": d.goal,
                    "setup": d.setup,
                    "steps": d.steps,
                    "expectations": d.expectations,
                }))
                .collect::<Vec<_>>(),
        });
        writeln!(
            buf,
            "        descriptors.put(\"{}\", Map.copyOf({}));",
            event.code,
            java_map_literal(&descriptor)
        )
        .unwrap();
    }
    writeln!(
        buf,
        "        EVENT_DESCRIPTORS = Collections.unmodifiableMap(descriptors);\n"
    )
    .unwrap();
    writeln!(
        buf,
        "        Map<String, Map<String, Object>> matrix = new HashMap<>();"
    )
    .unwrap();
    for entry in errors {
        let json_value = category_to_json(&entry.category, &entry.rationale, &entry.tuning);
        writeln!(
            buf,
            "        matrix.put(\"{}\", Map.copyOf({}));",
            entry.code,
            java_map_literal(&json_value)
        )
        .unwrap();
    }
    writeln!(
        buf,
        "        ERROR_MATRIX = Collections.unmodifiableMap(matrix);"
    )
    .unwrap();
    writeln!(buf, "    }}\n").unwrap();

    writeln!(
        buf,
        "    public static Map<String, Map<String, Object>> eventDescriptors() {{"
    )
    .unwrap();
    writeln!(buf, "        return EVENT_DESCRIPTORS;").unwrap();
    writeln!(buf, "    }}\n").unwrap();

    writeln!(
        buf,
        "    public static Map<String, Map<String, Object>> errorMatrix() {{"
    )
    .unwrap();
    writeln!(buf, "        return ERROR_MATRIX;").unwrap();
    writeln!(buf, "    }}\n").unwrap();

    writeln!(buf, "    public static String jsonSchema() {{").unwrap();
    writeln!(buf, "        return JSON_SCHEMA;").unwrap();
    writeln!(buf, "    }}\n").unwrap();

    writeln!(buf, "    public static String asyncApi() {{").unwrap();
    writeln!(buf, "        return ASYNCAPI;").unwrap();
    writeln!(buf, "    }}").unwrap();
    writeln!(buf, "}}").unwrap();

    buf
}

/// 将 JSON Value 转换为 Java Map 字面量（基于 Map.ofEntries）。
fn java_map_literal(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                return "Map.of()".to_string();
            }
            let mut entries = Vec::new();
            for (key, val) in map {
                entries.push(format!(
                    "Map.entry(\"{key}\", {value})",
                    key = key,
                    value = java_value_literal(val),
                ));
            }
            format!("Map.ofEntries({})", entries.join(", "))
        }
        _ => panic!("期望对象"),
    }
}

/// 将 JSON value 转换为 Java 常量表达式。
fn java_value_literal(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
        Value::Array(arr) => {
            if arr.is_empty() {
                "List.of()".to_string()
            } else {
                format!(
                    "List.of({})",
                    arr.iter()
                        .map(java_value_literal)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
        Value::Object(_) => java_map_literal(value),
    }
}

/// Java 字段类型推导。
fn java_type_for_field(field: &EventFieldSpec) -> String {
    java_type_with_required(&field.ty, field.required)
}

fn java_type_with_required(ty: &EventFieldTypeSpec, required: bool) -> String {
    match ty {
        EventFieldTypeSpec::String => "String".to_string(),
        EventFieldTypeSpec::U64 => {
            if required {
                "long".to_string()
            } else {
                "Long".to_string()
            }
        }
        EventFieldTypeSpec::Bool => {
            if required {
                "boolean".to_string()
            } else {
                "Boolean".to_string()
            }
        }
        EventFieldTypeSpec::Struct { ident } => ident.clone(),
        EventFieldTypeSpec::List { item } => format!("List<{}>", java_boxed_type(item)),
    }
}

fn java_boxed_type(ty: &EventFieldTypeSpec) -> String {
    match ty {
        EventFieldTypeSpec::String => "String".to_string(),
        EventFieldTypeSpec::U64 => "Long".to_string(),
        EventFieldTypeSpec::Bool => "Boolean".to_string(),
        EventFieldTypeSpec::Struct { ident } => ident.clone(),
        EventFieldTypeSpec::List { item } => format!("List<{}>", java_boxed_type(item)),
    }
}

fn to_camel_case(name: &str) -> String {
    let mut result = String::new();
    let mut upper = true;
    for ch in name.chars() {
        if ch == '_' {
            upper = true;
        } else if upper {
            result.extend(ch.to_uppercase());
            upper = false;
        } else {
            result.push(ch);
        }
    }
    result
}

fn render_java_build_gradle() -> String {
    "plugins {\n    id 'java'\n}\n\njava {\n    toolchain {\n        languageVersion = JavaLanguageVersion.of(17)\n    }\n}\n\nrepositories {\n    mavenCentral()\n}\n\n".to_string()
}

fn render_java_settings_gradle() -> String {
    "rootProject.name = 'spark-config-sdk'\n".to_string()
}
