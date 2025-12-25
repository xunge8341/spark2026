use alloc::string::String;

use super::TransportParams;

/// 端点分类。
///
/// # 设计动机（Why）
/// - 与 Envoy、Istio、Linkerd 等控制面的实践一致，区分逻辑发现地址与物理监听地址。
/// - 便于连接意图根据 `EndpointKind` 选择不同的解析流程（服务发现或直连）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EndpointKind {
    /// 逻辑地址（如 `srv://orders`）。
    Logical,
    /// 物理地址（如 `tcp://10.0.0.8:8080`）。
    Physical,
}

/// `Endpoint` 将协议方案、访问主体与可选路径统一表达。
///
/// # 设计背景（Why）
/// - **行业案例吸收**：参考 gRPC URI、Pulsar Topic、NATS Subject 的抽象，将 scheme/authority/path 作为核心三元组，保证跨协议一致性。
/// - **科研需求支持**：引入 `kind` 与参数表，方便在 Intent-Based Networking、拓扑感知路由等研究中注入额外语义。
///
/// # 契约说明（What）
/// - `scheme`：协议或访问方式（如 `tcp`、`quic`、`mem`）。
/// - `authority`：主机、服务名或控制面注册名。
/// - `port`：可选端口；逻辑地址允许缺省，由工厂或服务发现补全。
/// - `resource`：可选路径/资源名，兼容 HTTP/WS/RPC 多种模式。
/// - `params`：额外参数，遵循 [`TransportParams`] 约定。
/// - `kind`：逻辑或物理标记，驱动上游策略。
/// - **前置条件**：`scheme` 与 `authority` 必须为非空字符串。
/// - **后置条件**：调用者可通过访问器读取所有字段；`params` 默认为空表。
///
/// # 风险提示（Trade-offs）
/// - 未内建 URI 语法校验，保持 `no_std` 轻量；若需严格校验，请在构造前完成。
/// - `resource` 不强制格式，需双方约定含义。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    scheme: String,
    authority: String,
    port: Option<u16>,
    resource: Option<String>,
    params: TransportParams,
    kind: EndpointKind,
}

impl Endpoint {
    /// 构建物理端点，通常用于监听或直连。
    pub fn physical(scheme: String, authority: String, port: u16) -> Self {
        Self {
            scheme,
            authority,
            port: Some(port),
            resource: None,
            params: TransportParams::new(),
            kind: EndpointKind::Physical,
        }
    }

    /// 构建逻辑端点，常见于服务发现。
    pub fn logical(scheme: String, authority: String) -> Self {
        Self {
            scheme,
            authority,
            port: None,
            resource: None,
            params: TransportParams::new(),
            kind: EndpointKind::Logical,
        }
    }

    /// 自定义构建流程，供高级场景使用。
    pub fn builder(scheme: String, authority: String) -> EndpointBuilder {
        EndpointBuilder::new(scheme, authority)
    }

    /// 返回协议方案。
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// 返回主体（主机名/服务名）。
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// 返回端口（若存在）。
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// 返回资源路径或 Topic。
    pub fn resource(&self) -> Option<&str> {
        self.resource.as_deref()
    }

    /// 返回参数表。
    pub fn params(&self) -> &TransportParams {
        &self.params
    }

    /// 返回端点类型。
    pub fn kind(&self) -> EndpointKind {
        self.kind
    }
}

/// `EndpointBuilder` 以链式 API 构建复杂端点。
///
/// # 设计理由（Why）
/// - 与 AWS Smithy、gRPC Channel Builder 等实践一致，使用 Builder 提供易扩展的配置入口。
/// - 在实验场景中便于扩展更多字段而不破坏现有调用。
#[derive(Clone, Debug)]
pub struct EndpointBuilder {
    inner: Endpoint,
}

impl EndpointBuilder {
    fn new(scheme: String, authority: String) -> Self {
        Self {
            inner: Endpoint {
                scheme,
                authority,
                port: None,
                resource: None,
                params: TransportParams::new(),
                kind: EndpointKind::Logical,
            },
        }
    }

    /// 指定端口并切换为物理端点。
    pub fn with_port(mut self, port: u16) -> Self {
        self.inner.port = Some(port);
        self.inner.kind = EndpointKind::Physical;
        self
    }

    /// 指定资源路径。
    pub fn with_resource(mut self, resource: String) -> Self {
        self.inner.resource = Some(resource);
        self
    }

    /// 覆盖端点类型。
    pub fn with_kind(mut self, kind: EndpointKind) -> Self {
        self.inner.kind = kind;
        self
    }

    /// 设置参数表（覆盖）。
    pub fn with_params(mut self, params: TransportParams) -> Self {
        self.inner.params = params;
        self
    }

    /// 完成构造，返回端点。
    pub fn finish(self) -> Endpoint {
        self.inner
    }
}
