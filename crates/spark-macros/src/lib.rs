//! Spark 框架过程宏入口。
//!
//! # 设计意图（Why）
//! - 将繁琐的 Service 模板代码下沉到编译期展开，降低业务开发者的重复劳动；
//! - 保证生成实现遵循 `spark-core::service::Service` 契约，避免手写 `poll_ready` 导致的唤醒遗漏；
//! - 通过过程宏维持统一风格，便于在文档与样例中直接引用同一套 API。
//!
//! # 集成方式（How）
//! - 在业务 crate 中将 `spark_core` 引用重命名为 `spark`，即可使用 `#[spark::service]`；
//! - 宏会将原始 `async fn` 重命名为内部逻辑函数，并生成同名构造函数返回顺序执行的 `Service` 实例；
//! - 生成的 Service 默认采用顺序执行模型：并发请求会在 `poll_ready` 中排队等待上一个调用完成。

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Error, FnArg, GenericArgument, ItemFn, PatType, PathArguments, ReturnType, Type,
    parse_macro_input, parse_quote, spanned::Spanned,
};

/// 宏实现内部的统一返回类型，避免循环依赖 `spark-core` 同时保持与 `syn::Error` 的互操作。
type MacroResult<T> = core::result::Result<T, Error>;

/// 将开发者编写的 `async fn(ctx, req) -> spark_core::Result<_, _>` 转换为完整的 `Service` 实现。
///
/// # 语义说明（What）
/// - **输入**：仅接受无泛型的 `async fn`，必须有两个参数（执行上下文与请求）。
/// - **输出**：保留原函数逻辑，并生成同名构造函数返回顺序执行的 `Service` 实例。
/// - **前置条件**：返回类型需要是 `spark_core::Result<Response, Error>`，其中 `Error: spark::Error`。
/// - **后置条件**：生成的 Service 在每次调用后都会唤醒等待的 waker，确保零悬挂。
///
/// # 风险提示（Trade-offs）
/// - 当前实现不支持泛型函数或可变参数，如需扩展需同步评估生成代码的复杂度；
/// - 宏假设调用方在编译单元中将 `spark_core` 重命名为 `spark`，否则路径解析会失败。
#[proc_macro_attribute]
pub fn service(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return Error::new(
            proc_macro2::Span::call_site(),
            "#[spark::service] 不接受参数",
        )
        .to_compile_error()
        .into();
    }

    let func = parse_macro_input!(item as ItemFn);
    expand_service(func)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

fn expand_service(func: ItemFn) -> MacroResult<proc_macro2::TokenStream> {
    if func.sig.asyncness.is_none() {
        return Err(Error::new(
            func.sig.span(),
            "#[spark::service] 仅支持 async fn",
        ));
    }

    if !func.sig.generics.params.is_empty() {
        return Err(Error::new(
            func.sig.generics.span(),
            "#[spark::service] 暂不支持带泛型参数的函数",
        ));
    }

    let mut inputs = func.sig.inputs.iter();
    match inputs.next() {
        Some(FnArg::Typed(_)) => {}
        _ => {
            return Err(Error::new(
                func.sig.inputs.span(),
                "#[spark::service] 期望第一个参数为执行上下文",
            ));
        }
    };
    let req_arg = match inputs.next() {
        Some(FnArg::Typed(pat)) => pat,
        _ => {
            return Err(Error::new(
                func.sig.inputs.span(),
                "#[spark::service] 期望第二个参数为请求体",
            ));
        }
    };

    if inputs.next().is_some() {
        return Err(Error::new(
            func.sig.inputs.span(),
            "#[spark::service] 仅支持两个参数：执行上下文与请求体",
        ));
    }

    let request_ty = extract_type(req_arg)?;
    let (response_ty, error_ty) = extract_result_types(&func.sig)?;

    let fn_ident = func.sig.ident.clone();
    let logic_ident = format_ident!("__spark_service_logic_{}", fn_ident);
    let audit_ident = format_ident!("__spark_service_audit_{}", fn_ident);
    let attrs = func.attrs.clone();
    let vis = func.vis.clone();

    let mut logic_fn = func.clone();
    logic_fn.attrs.clear();
    logic_fn.attrs.push(parse_quote!(#[doc(hidden)]));
    logic_fn.sig.ident = logic_ident.clone();

    let expanded = quote! {
        #logic_fn

        #(#attrs)*
        #vis fn #fn_ident() -> impl spark::service::Service<#request_ty, Response = #response_ty, Error = #error_ty>
            + spark::service::AutoDynBridge<DynOut = spark::service::BoxService>
        {
            spark::service::DynBridge::<_, #request_ty>::new(
                spark::service::simple::SimpleServiceFn::new(#logic_ident),
            )
        }

        #[cfg(test)]
        #[allow(dead_code)]
        fn #audit_ident() {
            /// 检查宏展开生成的逻辑函数是否满足 `Send + Sync + 'static` 约束，并返回符合契约的 Future。
            fn assert_logic_contract<F, Fut>(_: &F)
            where
                F: FnMut(spark::CallContext, #request_ty) -> Fut + Send + Sync + 'static,
                Fut: core::future::Future<Output = core::result::Result<#response_ty, #error_ty>> + Send + 'static,
            {
            }

            /// 检查最终 Service 是否实现合约指定的所有 Trait 约束，同时验证 `call` 返回的 Future 类型。
            fn assert_service_contract<S>(service: &S)
            where
                S: spark::service::Service<#request_ty, Response = #response_ty, Error = #error_ty>
                    + spark::service::AutoDynBridge<DynOut = spark::service::BoxService>
                    + Send
                    + Sync
                    + 'static,
            {
                fn assert_future<Fut>()
                where
                    Fut: core::future::Future<Output = core::result::Result<#response_ty, #error_ty>>
                        + Send
                        + 'static,
                {
                }

                assert_future::<
                    <S as spark::service::Service<#request_ty>>::Future,
                >();
                let _ = service;
            }

            let logic = #logic_ident;
            assert_logic_contract(&logic);

            let service = #fn_ident();
            assert_service_contract(&service);
        }
    };

    Ok(expanded)
}

fn extract_type(arg: &PatType) -> MacroResult<&Type> {
    Ok(&arg.ty)
}

fn extract_result_types(sig: &syn::Signature) -> MacroResult<(&Type, &Type)> {
    match &sig.output {
        ReturnType::Type(_, ty) => match ty.as_ref() {
            Type::Path(type_path) => {
                let segment = type_path
                    .path
                    .segments
                    .last()
                    .ok_or_else(|| Error::new(type_path.span(), "返回类型缺失"))?;
                if segment.ident != "Result" {
                    return Err(Error::new(
                        segment.ident.span(),
                        "#[spark::service] 要求返回 spark_core::Result<_, _>",
                    ));
                }
                match &segment.arguments {
                    PathArguments::AngleBracketed(args) => {
                        let mut generics = args.args.iter();
                        let response_ty = match generics.next() {
                            Some(GenericArgument::Type(ty)) => ty,
                            _ => return Err(Error::new(args.span(), "Result 必须提供响应类型")),
                        };
                        let error_ty = match generics.next() {
                            Some(GenericArgument::Type(ty)) => ty,
                            _ => return Err(Error::new(args.span(), "Result 必须提供错误类型")),
                        };
                        Ok((response_ty, error_ty))
                    }
                    _ => Err(Error::new(
                        segment.arguments.span(),
                        "Result 泛型参数解析失败",
                    )),
                }
            }
            _ => Err(Error::new(ty.span(), "返回类型需为 Result")),
        },
        ReturnType::Default => Err(Error::new(
            sig.span(),
            "#[spark::service] 需要返回 spark_core::Result<_, _>",
        )),
    }
}
