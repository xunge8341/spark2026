//! 对象层桥接测试：验证实现 `Decode`/`Encode` 契约的 Service 可以通过 `AutoDynBridge`
//! 自动转化为 [`BoxService`](spark_core::service::BoxService) 并在对象层正常工作。

use core::future::Future;
use core::pin::pin;
use core::task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker};

use spark_core::SparkError;
use spark_core::buffer::PipelineMessage;
use spark_core::context::Context as ExecutionView;
use spark_core::contract::CallContext;
use spark_core::service::{
    AutoDynBridge, Decode, Encode, Service, bridge_to_box_service, type_mismatch_error,
};
use spark_core::status::{PollReady, ReadyCheck, ReadyState};

use std::sync::Arc;

/// 示范业务请求体，承载字符串负载。
#[derive(Debug, PartialEq, Eq)]
struct MyRequest {
    payload: String,
}

/// 示范业务响应体，回显处理结果。
#[derive(Debug, PartialEq, Eq)]
struct MyResponse {
    payload: String,
}

/// 业务 Service：收到请求后追加 `"-ack"` 返回。
struct MyService;

impl AutoDynBridge for MyService {
    type DynOut = spark_core::service::BoxService;

    fn into_dyn<Request>(self) -> Self::DynOut
    where
        Self: Service<Request, Error = SparkError>,
        Request: Decode + Send + Sync + 'static,
        <Self as Service<Request>>::Response: Encode + Send + Sync + 'static,
    {
        bridge_to_box_service::<Self, Request>(self)
    }
}

impl Decode for MyRequest {
    fn decode(message: PipelineMessage) -> spark_core::Result<Self, SparkError> {
        match message.try_into_user::<MyRequest>() {
            Ok(request) => Ok(request),
            Err(original) => {
                let actual = original.user_kind().unwrap_or("pipeline.unknown");
                Err(type_mismatch_error(
                    core::any::type_name::<MyRequest>(),
                    actual,
                ))
            }
        }
    }
}

impl Encode for MyResponse {
    fn encode(self) -> PipelineMessage {
        PipelineMessage::from_user(self)
    }
}

impl Service<MyRequest> for MyService {
    type Response = MyResponse;
    type Error = SparkError;
    type Future = core::future::Ready<spark_core::Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &ExecutionView<'_>,
        _: &mut TaskContext<'_>,
    ) -> PollReady<Self::Error> {
        Poll::Ready(ReadyCheck::Ready(ReadyState::Ready))
    }

    fn call(&mut self, _: CallContext, req: MyRequest) -> Self::Future {
        let response = MyResponse {
            payload: format!("{}-ack", req.payload),
        };
        core::future::ready(Ok(response))
    }
}

fn noop_waker() -> Waker {
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

fn noop_raw_waker() -> RawWaker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}

    RawWaker::new(
        core::ptr::null(),
        &RawWakerVTable::new(clone, wake, wake_by_ref, drop),
    )
}

fn block_on_ready<F>(future: F) -> F::Output
where
    F: Future,
{
    let waker = noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    let mut future = pin!(future);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("ready future unexpectedly pending"),
    }
}

#[test]
fn into_dyn_bridge_roundtrip() {
    // 教案式说明：构建泛型 Service 与上下文，验证自动桥接后可透过 DynService 正确处理消息。
    let service = MyService;
    let call_ctx = CallContext::builder().build();
    let request_payload = PipelineMessage::from_user(MyRequest {
        payload: "ping".to_string(),
    });

    let boxed = service.into_dyn::<MyRequest>();
    let mut arc = boxed.into_arc();
    let dyn_service = Arc::get_mut(&mut arc).expect("bridge should hand out unique Arc");

    let waker = noop_waker();
    let mut task_cx = TaskContext::from_waker(&waker);
    let readiness = dyn_service.poll_ready_dyn(&call_ctx.execution(), &mut task_cx);
    match readiness {
        Poll::Ready(ReadyCheck::Ready(ReadyState::Ready)) => {}
        other => panic!("unexpected readiness: {other:?}"),
    }

    let response_future = dyn_service.call_dyn(call_ctx.clone(), request_payload);
    let response_message = block_on_ready(response_future).expect("service should succeed");
    let response = response_message
        .try_into_user::<MyResponse>()
        .expect("response type should downcast");
    assert_eq!(response.payload, "ping-ack");
}
