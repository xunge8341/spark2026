//! # spark-contract-tests-macros
//!
//! 该 crate 提供 `spark_tck` 属性宏，用于为实现契约一致性测试的模块自动注入
//! 标准化的测试入口。通过宏生成的测试会在 CI 中充当“最低限度门禁”，确保所有
//! 契约套件始终被执行，避免由于手写样板代码而产生遗漏。宏的实现分为三个阶段：
//! 解析调用参数、确定目标套件列表以及将测试桩植入目标模块。

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Ident, ItemMod, Meta, Result as SynResult, Token, parse_macro_input};

#[proc_macro_attribute]
/// 教案级说明：
/// - **意图（Why）**：`spark_tck` 属性宏负责将契约测试套件的运行入口注入到目标
///   模块，确保任一实现者只需声明目标套件即可获得完整的测试覆盖。
/// - **逻辑（How）**：宏会先解析属性参数（见 `parse_suites`），再根据解析结果调
///   用 `inject_tests` 将 `#[test]` 函数追加到模块。若解析或注入过程中出现语法错误，
///   将生成编译期诊断。
/// - **契约（What）**：输入参数为属性 `TokenStream` 与模块 `TokenStream`；调用者需
///   保证模块语法正确且属性符合规范；成功返回的 `TokenStream` 包含原始模块与新增测
///   试函数。
/// - **权衡（Trade-offs）**：宏在编译期展开，避免运行时代码生成，但也要求提供友好
///   的诊断信息以降低调试成本。
pub fn spark_tck(attr: TokenStream, item: TokenStream) -> TokenStream {
    let module = parse_macro_input!(item as ItemMod);

    match parse_suites(attr).and_then(|suites| inject_tests(suites, module)) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// 教案级说明：
/// - **意图**：解析属性参数，确定待生成的契约套件列表，允许调用方按需选择测试范畴。
/// - **逻辑**：当属性为空时返回默认套件；否则解析为 `Meta` 并要求 `suites(...)` 格式。
/// - **契约**：输入为属性 `TokenStream`，前置条件是其可解析为 `Meta`；成功输出
///   `Vec<Ident>`；如遇语法错误返回 `syn::Error`，供上层转化为诊断。
/// - **权衡**：保持调用形式精简，不支持嵌套配置，以便未来在此函数集中处理向后兼容。
fn parse_suites(attr: TokenStream) -> SynResult<Vec<Ident>> {
    if attr.is_empty() {
        return Ok(default_suite_idents());
    }

    let meta = syn::parse::<Meta>(attr)?;
    match meta {
        Meta::List(list) if list.path.is_ident("suites") => {
            let nested: Punctuated<Meta, Token![,]> =
                list.parse_args_with(Punctuated::parse_terminated)?;
            let mut suites = Vec::new();
            for meta in nested {
                match meta {
                    Meta::Path(path) => {
                        if let Some(ident) = path.get_ident() {
                            suites.push(ident.clone());
                        } else {
                            return Err(syn::Error::new(path.span(), "suite 需为标识符"));
                        }
                    }
                    other => {
                        return Err(syn::Error::new(other.span(), "suites(...) 仅接受标识符"));
                    }
                }
            }
            if suites.is_empty() {
                Ok(default_suite_idents())
            } else {
                Ok(suites)
            }
        }
        Meta::Path(path) if path.is_ident("suites") => Ok(default_suite_idents()),
        other => Err(syn::Error::new(
            other.span(),
            "spark_tck 属性仅支持 suites(...)",
        )),
    }
}

/// 教案级说明：
/// - **意图**：在调用方未指定套件时提供默认清单，避免遗漏核心契约测试。
/// - **逻辑**：通过静态字符串数组列举套件名称，并统一转换为 `Ident`。
/// - **契约**：无输入参数；输出保证至少包含一个元素。
/// - **权衡**：采用固定数组保持顺序稳定，而非哈希集合。
fn default_suite_idents() -> Vec<Ident> {
    [
        "backpressure",
        "cancellation",
        "errors",
        "state_machine",
        "hot_swap",
        "hot_reload",
        "observability",
    ]
    .iter()
    .map(|name| Ident::new(name, Span::call_site()))
    .collect()
}

/// 教案级说明：
/// - **意图**：根据套件列表为目标模块生成对应的 `#[test]` 函数，形成统一的测试入口。
/// - **逻辑**：
///   1. 为每个套件构造测试函数名与运行函数路径；
///   2. 生成 `syn::Item` 表示的测试函数并加入列表；
///   3. 根据模块是否已有内容选择扩展或重新拼装；
///   4. 返回最终的 `TokenStream`。
/// - **契约**：输入为已验证的 `suites` 列表与目标模块；前置条件是模块可被 `quote!`
///   正确展开；输出的 `TokenStream` 保留原有项并追加测试函数。
/// - **权衡**：语法树级别注入便于调试，但需处理内联与文件模块两种情况，为此在
///   `module.content` 为空时重新拼装模块以保留可见性与属性。
fn inject_tests(suites: Vec<Ident>, mut module: ItemMod) -> SynResult<proc_macro2::TokenStream> {
    let mut generated = Vec::new();
    for suite in suites {
        let test_ident = format_ident!("{}_suite", suite);
        let run_fn: syn::Path =
            syn::parse_str(&format!("spark_contract_tests::run_{}_suite", suite))?;
        let item: syn::Item = syn::parse_quote! {
            #[test]
            fn #test_ident() {
                #run_fn();
            }
        };
        generated.push(item);
    }

    if let Some((_, ref mut items)) = module.content {
        items.extend(generated);
        Ok(quote! { #module })
    } else {
        let ident = &module.ident;
        let vis = &module.vis;
        let attrs = &module.attrs;
        Ok(quote! {
            #(#attrs)*
            #vis mod #ident {
                #(#generated)*
            }
        })
    }
}
