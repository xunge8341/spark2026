use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use spark::service::Service;
use spark_core as spark;

#[spark::service]
async fn greet(
    _ctx: spark::CallContext,
    name: String,
) -> spark_core::Result<String, spark::CoreError> {
    Ok(format!("Hello, {name}!"))
}

fn main() {
    let mut service = greet();
    let ctx = spark::CallContext::builder().build();
    let waker = noop_waker();
    let mut ready_cx = Context::from_waker(&waker);

    match service.poll_ready(&ctx.execution(), &mut ready_cx) {
        Poll::Ready(spark::ReadyCheck::Ready(spark::ReadyState::Ready)) => {}
        other => panic!("service not ready: {other:?}"),
    }

    let mut future = Box::pin(service.call(ctx, "Spark".to_string()));
    let mut call_cx = Context::from_waker(&waker);
    match future.as_mut().poll(&mut call_cx) {
        Poll::Ready(Ok(message)) => println!("{message}"),
        Poll::Ready(Err(error)) => panic!("service returned error: {error}"),
        Poll::Pending => panic!("example future should complete immediately"),
    }
}

fn noop_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}
