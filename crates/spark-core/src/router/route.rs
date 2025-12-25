use alloc::borrow::Cow;
use alloc::vec::Vec;

/// 路由类型枚举，区分不同的语义域。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum RouteKind {
    /// 远程过程调用或面向请求-响应的业务。
    Rpc,
    /// 事件或消息通知流。
    Event,
    /// 控制面或运维接口。
    Management,
}

/// 路由段的组成单元。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum RouteSegment {
    /// 字面量段，要求目标完全一致。
    Literal(Cow<'static, str>),
    /// 参数占位符，匹配任意单段。
    Parameter(Cow<'static, str>),
    /// 通配符，匹配剩余所有段。
    Wildcard,
}

/// 路由模式，由路由类型与段序列组成。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutePattern {
    kind: RouteKind,
    segments: Vec<RouteSegment>,
}

impl RoutePattern {
    /// 构造新的路由模式。
    pub fn new(kind: RouteKind, segments: Vec<RouteSegment>) -> Self {
        Self { kind, segments }
    }

    /// 访问路由类型。
    pub fn kind(&self) -> &RouteKind {
        &self.kind
    }

    /// 访问段迭代器。
    pub fn segments(&self) -> core::slice::Iter<'_, RouteSegment> {
        self.segments.iter()
    }
}

/// 稳定的路由标识符，由路由类型与解析后的字面量段组成。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RouteId {
    kind: RouteKind,
    segments: Vec<Cow<'static, str>>,
}

impl RouteId {
    /// 构造新的路由 ID。
    pub fn new(kind: RouteKind, segments: Vec<Cow<'static, str>>) -> Self {
        Self { kind, segments }
    }

    /// 访问路由类型。
    pub fn kind(&self) -> &RouteKind {
        &self.kind
    }

    /// 访问字面量段集合。
    pub fn segments(&self) -> core::slice::Iter<'_, Cow<'static, str>> {
        self.segments.iter()
    }
}
