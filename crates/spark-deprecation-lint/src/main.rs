//! `spark-deprecation-lint`：CI 阶段自动核验弃用标注的轻量工具。
//!
//! # 设计动机（Why）
//! - 遵循 T23 目标：任何 `#[deprecated]` 必须配齐版本窗口与迁移指引。
//! - 通过源码扫描避免漏写 `since`、`note` 字段，减少人工审查开销。
//! - 采用零第三方依赖，确保可在受限 CI 环境中运行。
//!
//! # 工作方式（How）
//! 1. 解析工作区根目录，递归遍历除 `target/` 与 `.git/` 之外的所有子目录。
//! 2. 对每个 `.rs` 文件执行基于字符串的轻量语法分析，捕获 `#[deprecated(..)]` 属性。
//! 3. 验证属性参数是否包含策略要求的关键片段；若缺失则记录违规详情。
//! 4. 汇总结果：若存在违规即以非零状态码退出，阻止 CI 继续。
//!
//! # 使用契约（What）
//! - **输入**：当前工作目录位于 Workspace 根目录（由 `Makefile` 保证）。
//! - **前置条件**：弃用注解遵循 `note = "removal: ...; migration: ...; tracking: ..."` 模板。
//! - **后置条件**：若所有检查通过，进程以零退出码结束，不产生任何输出。
//!
//! # 风险提示（Trade-offs & Gotchas）
//! - 该工具不解析 Rust AST，无法处理宏动态生成的弃用标注；如需更复杂的检测请改用 `syn`/`rust-analyzer` 能力。
//! - 字符串匹配策略要求注解格式相对规范；如需跨多行排版，请保持关键字完整无拆分。
//! - 由于递归遍历全部源码目录，极大仓库可能带来小幅开销；目前代码库规模可接受。

use std::{
    env,
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
};

fn main() {
    if let Err(error) = run() {
        match error {
            ToolError::Io(io_error) => {
                eprintln!(
                    "spark-deprecation-lint: 读取文件失败：{io_error}. 请检查文件权限或路径。"
                );
                std::process::exit(1);
            }
            ToolError::Policy(findings) => {
                eprintln!("spark-deprecation-lint: 检测到弃用标注违规:");
                for finding in findings {
                    eprintln!("  - {}", finding.format());
                }
                eprintln!("请参考 docs/governance/deprecation.md 修复上述问题。");
                std::process::exit(1);
            }
        }
    }
}

/// 工具运行的主逻辑，负责遍历工作区并聚合全部违规项。
///
/// # 设计说明（Why）
/// - 独立函数便于单元测试与未来扩展（如增加忽略目录白名单）。
///
/// # 执行流程（How）
/// 1. 获取工作区根目录，确保所有路径输出均为相对路径，便于 CI 日志阅读。
/// 2. 调用 [`collect_rust_files`] 汇总所有需要扫描的 `.rs` 文件列表。
/// 3. 对每个文件执行 [`inspect_file`]，累计违规条目。
/// 4. 根据违规结果返回 `Ok(())` 或 `ToolError::Policy`。
///
/// # 契约约束（What）
/// - **前置条件**：当前进程具备读取源码文件的权限。
/// - **后置条件**：若返回 `Ok`，表示未检测到任何策略违规；若返回 `Err`，保证其中的 `Finding` 至少包含一条信息。
fn run() -> Result<(), ToolError> {
    let workspace_root = workspace_root();
    let mut files = Vec::new();
    collect_rust_files(&workspace_root, &mut files)?;

    let mut findings = Vec::new();
    for path in files {
        findings.extend(inspect_file(&workspace_root, &path)?);
    }

    if findings.is_empty() {
        Ok(())
    } else {
        Err(ToolError::Policy(findings))
    }
}

/// 计算工作区根目录。
///
/// # 设计考量（Why）
/// - 依赖编译期常量 `CARGO_MANIFEST_DIR`，可避免运行时查找配置文件。
/// - 相对路径输出有助于阅读 CI 日志并定位文件。
///
/// # 逻辑解析（How）
/// - `CARGO_MANIFEST_DIR` 指向 `crates/spark-deprecation-lint`，向上两级即可抵达仓库根。
/// - 使用 `PathBuf` 确保跨平台兼容性（Windows/Unix）。
///
/// # 契约说明（What）
/// - **前置条件**：目录深度固定为 `workspace/crates/spark-deprecation-lint`，若后续调整需同步更新此处逻辑。
/// - **后置条件**：返回的路径总是以绝对形式呈现。
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("工具目录结构已被破坏：无法推导仓库根")
        .to_path_buf()
}

/// 遍历目录树，收集所有 Rust 源文件路径。
///
/// # 设计缘由（Why）
/// - 统一在此处屏蔽无需扫描的目录（如 `target/`、`.git/`），避免重复代码。
///
/// # 算法步骤（How）
/// - 深度优先遍历：对文件夹递归调用自身，对文件则根据扩展名判定是否纳入。
/// - 跳过隐藏或特定目录以减少噪音。
///
/// # 契约约束（What）
/// - **输入参数**：
///   - `root`：递归起点目录。
///   - `files`：输出容器，函数将追加路径而非覆盖。
/// - **前置条件**：`files` 在调用前已初始化，可为空。
/// - **后置条件**：`files` 包含所有符合条件的 `.rs` 文件绝对路径。
///
/// # 风险提示（Trade-offs）
/// - 当前实现未并发化；若未来仓库体量显著扩大，可考虑 rayon 并行遍历。
fn collect_rust_files(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), ToolError> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let entries = fs::read_dir(&path).map_err(ToolError::Io)?;
        for entry in entries {
            let entry = entry.map_err(ToolError::Io)?;
            let entry_path = entry.path();
            if entry.file_type().map_err(ToolError::Io)?.is_dir() {
                if should_skip_dir(&entry_path) {
                    continue;
                }
                stack.push(entry_path);
            } else if entry_path
                .extension()
                .and_then(OsStr::to_str)
                .map(|ext| ext.eq_ignore_ascii_case("rs"))
                .unwrap_or(false)
            {
                files.push(entry_path);
            }
        }
    }
    Ok(())
}

/// 判断是否需要跳过指定目录。
///
/// # 设计考量（Why）
/// - 排除构建产物与版本控制目录，避免重复扫描和噪音。
/// - 允许未来在此扩展黑名单（例如第三方子模块）。
fn should_skip_dir(path: &Path) -> bool {
    path.components().any(|component| {
        matches!(component.as_os_str().to_str(), Some("target" | ".git"))
            || path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "snapshots")
    })
}

/// 检查单个源文件的弃用标注是否符合策略。
///
/// # 设计背景（Why）
/// - 将文件解析逻辑集中在此，便于未来替换为 AST 方案。
///
/// # 工作流程（How）
/// - 顺序遍历文件行，捕获以 `#[deprecated` 开头的属性。
/// - 对多行属性进行拼接，直至遇到 `]` 为止。
/// - 校验必要字段与关键字，构造对应的 [`Finding`]。
///
/// # 契约说明（What）
/// - **输入参数**：
///   - `workspace_root`：用于生成相对路径的基线。
///   - `path`：当前检查的源文件。
/// - **前置条件**：文件可被完整读取为 UTF-8 文本。
/// - **后置条件**：返回的 `Vec<Finding>` 只在存在违规时包含元素；若无违规则返回空向量。
fn inspect_file(workspace_root: &Path, path: &Path) -> Result<Vec<Finding>, ToolError> {
    let content = fs::read_to_string(path).map_err(ToolError::Io)?;
    let mut findings = Vec::new();

    let mut lines = content.lines().enumerate();
    while let Some((line_index, line)) = lines.next() {
        if let Some(start) = line.find("#[deprecated") {
            if !line.trim_start().starts_with("#[deprecated") {
                continue;
            }
            let mut attribute = String::from(&line[start..]);
            let mut end_line = line_index;
            while !attribute.contains(']') {
                if let Some((next_index, next_line)) = lines.next() {
                    attribute.push('\n');
                    attribute.push_str(next_line);
                    end_line = next_index;
                } else {
                    break;
                }
            }

            if let Some(messages) = validate_attribute(&attribute) {
                let relative = path
                    .strip_prefix(workspace_root)
                    .unwrap_or(path)
                    .to_path_buf();
                for message in messages {
                    findings.push(Finding {
                        path: relative.clone(),
                        line: line_index + 1,
                        message,
                        span_end: end_line + 1,
                    });
                }
            }
        }
    }

    Ok(findings)
}

/// 针对单个弃用属性执行字段校验。
///
/// # 逻辑说明（How）
/// - 若检测到任一字段缺失，则返回需要提示的消息列表；否则返回 `None` 表示通过。
fn validate_attribute(attribute: &str) -> Option<Vec<String>> {
    let mut messages = Vec::new();
    if !attribute.contains("since") {
        messages.push("缺少 since 字段".to_string());
    }
    if !attribute.contains("note") {
        messages.push("缺少 note 字段".to_string());
    }
    if attribute.contains("\"\"") {
        messages.push("note 字段内容为空".to_string());
    }
    if attribute.contains("TBD") {
        messages.push("请勿在弃用注解中使用 TBD 占位符".to_string());
    }
    if !attribute.contains("removal:") {
        messages.push("note 字段必须包含 removal: 关键字".to_string());
    }
    if !attribute.contains("migration:") {
        messages.push("note 字段必须包含 migration: 关键字".to_string());
    }

    if messages.is_empty() {
        None
    } else {
        Some(messages)
    }
}

/// 描述单条策略违规信息。
///
/// # 结构说明（What）
/// - `path`：相对于工作区根目录的文件路径，方便在 CI 日志中直接定位。
/// - `line` 与 `span_end`：标记违规范围的起止行号（包含两端），便于快速跳转。
/// - `message`：面向开发者的修复建议。
#[derive(Debug)]
struct Finding {
    path: PathBuf,
    line: usize,
    span_end: usize,
    message: String,
}

impl Finding {
    /// 将违规信息格式化为可读文本。
    fn format(&self) -> String {
        format!(
            "{}:{}-{} {}",
            self.path.display(),
            self.line,
            self.span_end,
            self.message
        )
    }
}

/// 工具错误类型，统一封装 IO 与策略违规两类场景。
#[derive(Debug)]
enum ToolError {
    Io(io::Error),
    Policy(Vec<Finding>),
}

impl From<io::Error> for ToolError {
    fn from(error: io::Error) -> Self {
        ToolError::Io(error)
    }
}
