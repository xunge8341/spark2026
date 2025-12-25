use core::num::NonZeroU16;

use core::mem::MaybeUninit;

use spark_core::buffer::{PoolStats, ReadableBuffer, WritableBuffer};
use spark_core::codec::{DecodeContext, EncodeContext};
use spark_core::error::codes;
use spark_core::types::{Budget, BudgetKind};
use spark_core::{BufferPool, CoreError};
use std::sync::Arc;

/// `NoopBuffer` 是测试使用的最小可写缓冲实现，仅满足接口契约。
struct NoopBuffer;

impl WritableBuffer for NoopBuffer {
    fn capacity(&self) -> usize {
        0
    }

    fn remaining_mut(&self) -> usize {
        0
    }

    fn written(&self) -> usize {
        0
    }

    fn reserve(&mut self, _additional: usize) -> spark_core::Result<(), CoreError> {
        Ok(())
    }

    fn put_slice(&mut self, _src: &[u8]) -> spark_core::Result<(), CoreError> {
        Ok(())
    }

    fn write_from(
        &mut self,
        _src: &mut dyn ReadableBuffer,
        _len: usize,
    ) -> spark_core::Result<(), CoreError> {
        Ok(())
    }

    fn chunk_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        empty_mut_slice()
    }

    fn advance_mut(&mut self, len: usize) -> spark_core::Result<(), CoreError> {
        if len == 0 {
            Ok(())
        } else {
            Err(CoreError::new(
                codes::APP_INVALID_ARGUMENT,
                "noop buffer has no capacity for advance_mut",
            ))
        }
    }

    fn clear(&mut self) {}

    fn freeze(self: Box<Self>) -> spark_core::Result<Box<dyn ReadableBuffer>, CoreError> {
        Err(CoreError::new(
            codes::PROTOCOL_DECODE,
            "noop buffer does not support freeze",
        ))
    }
}

fn empty_mut_slice() -> &'static mut [MaybeUninit<u8>] {
    static mut EMPTY: [MaybeUninit<u8>; 0] = [];
    unsafe { &mut EMPTY[..] }
}

/// `TestAllocator` 始终返回零实现缓冲，用于构造上下文。
struct TestAllocator;

impl BufferPool for TestAllocator {
    fn acquire(
        &self,
        _min_capacity: usize,
    ) -> spark_core::Result<Box<dyn WritableBuffer>, CoreError> {
        Ok(Box::new(NoopBuffer))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
        Ok(0)
    }

    fn statistics(&self) -> spark_core::Result<PoolStats, CoreError> {
        Ok(PoolStats::default())
    }
}

#[test]
fn decode_context_rejects_oversized_frame() {
    let allocator = TestAllocator;
    let ctx = DecodeContext::with_limits(&allocator, None, Some(8), None);
    let err = ctx.check_frame_constraints(16).expect_err("帧超限应被拒绝");
    assert_eq!(err.code(), codes::PROTOCOL_BUDGET_EXCEEDED);
}

#[test]
fn decode_context_budget_consumption_and_refund() {
    let allocator = TestAllocator;
    let budget = Budget::new(BudgetKind::Decode, 5);
    let ctx = DecodeContext::with_limits(&allocator, Some(&budget), None, None);

    ctx.check_frame_constraints(3).expect("首次消费不应失败");
    assert_eq!(budget.remaining(), 2);

    ctx.refund_budget(1);
    assert_eq!(budget.remaining(), 3);

    let err = ctx.check_frame_constraints(4).expect_err("预算耗尽应拒绝");
    assert_eq!(err.code(), codes::PROTOCOL_BUDGET_EXCEEDED);
    assert_eq!(budget.remaining(), 3, "失败路径不应扣减预算");
}

#[test]
fn encode_context_budget_and_depth_behavior_matches_decoder() {
    let allocator = TestAllocator;
    let budget = Budget::new(BudgetKind::Flow, 6);
    let mut ctx =
        EncodeContext::with_limits(&allocator, Some(&budget), Some(10), NonZeroU16::new(1));

    ctx.check_frame_constraints(4).expect("应扣减预算");
    assert_eq!(budget.remaining(), 2);

    ctx.refund_budget(1);
    assert_eq!(budget.remaining(), 3);

    let err = ctx.check_frame_constraints(8).expect_err("帧长超限应失败");
    assert_eq!(err.code(), codes::PROTOCOL_BUDGET_EXCEEDED);

    std::mem::drop(ctx.enter_frame().expect("第一次进入深度应成功"));
    assert_eq!(ctx.current_depth(), 0, "守卫释放后深度归零");
}

#[test]
fn encode_context_accepts_trait_object_allocator() {
    let pool = TestAllocator;
    let dyn_view: &dyn BufferPool = &pool;
    let adapter = dyn_view.as_allocator();
    let ctx = EncodeContext::new(&adapter);

    ctx.acquire_buffer(0)
        .expect("trait object应当满足 BufferAllocator blanket 实现");
}

#[test]
fn encode_context_accepts_arc_trait_object_allocator() {
    let pool: Arc<dyn BufferPool> = Arc::new(TestAllocator);
    let adapter = pool.as_allocator();
    let ctx = EncodeContext::new(&adapter);

    ctx.acquire_buffer(0)
        .expect("Arc<dyn BufferPool> 也应实现 BufferAllocator");
}
