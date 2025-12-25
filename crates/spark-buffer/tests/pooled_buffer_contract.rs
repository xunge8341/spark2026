//! `pooled_buffer_contract` 集成测试：聚焦 `PooledBuffer` 生命周期与接口契约。
//!
//! # 测试总览（Why）
//! - 校验写入、冻结、拆分、扩容等状态转换是否正确通知回收器；
//! - 覆盖越界访问、错误路径，确保返回的 `CoreError` 与约束一致；
//! - 以 `RecordingRecycler` 观察回收事件，验证池与缓冲之间的协作协议。

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use spark_buffer::{BufferRecycler, PooledBuffer, ReclaimedBuffer, SlabBufferPool};
use spark_core::buffer::{ReadableBuffer, WritableBuffer};

/// `RecordingRecycler`：测试场景下用于捕获回收事件的探针实现。
///
/// # 设计动机（Why）
/// - 回收器是 `PooledBuffer` 合约的关键观察点；若遗漏或顺序错误，将导致内存泄漏或统计失真。
///
/// # 行为描述（How）
/// - 利用 `Mutex<Vec<(usize, bool)>>` 保存每一次 `reclaim` 的容量与是否成功夺回底层 `BytesMut`；
/// - `take_events` 在断言前清空事件队列，确保各个测试相互独立。
#[derive(Default)]
struct RecordingRecycler {
    events: Mutex<Vec<(usize, bool)>>,
}

impl RecordingRecycler {
    fn take_events(&self) -> Vec<(usize, bool)> {
        self.events
            .lock()
            .expect("mutex poisoned")
            .drain(..)
            .collect()
    }
}

impl BufferRecycler for RecordingRecycler {
    fn reclaim(&self, reclaimed: ReclaimedBuffer) {
        let capacity = reclaimed.capacity();
        let had_buffer = reclaimed.into_buffer().is_some();
        self.events
            .lock()
            .expect("mutex poisoned")
            .push((capacity, had_buffer));
    }
}

/// 冻结操作应转换为只读视图并触发一次回收记录。
#[test]
fn freeze_transitions_to_read_only_and_recycles_buffer() {
    let recycler = Arc::new(RecordingRecycler::default());
    let mut buffer = PooledBuffer::new(BytesMut::with_capacity(8), recycler.clone());
    buffer.put_slice(b"abc").expect("写入示例数据");
    let expected_capacity = buffer.written();
    let mut readable = Box::new(buffer).freeze().expect("冻结应成功");
    let mut out = [0u8; 3];
    readable
        .copy_into_slice(&mut out)
        .expect("应能读取冻结后的数据");
    assert_eq!(&out, b"abc");
    drop(readable);
    let events = recycler.take_events();
    assert_eq!(events, vec![(expected_capacity, true)]);
}

/// `chunk_mut` + `advance_mut` 组合实现零拷贝写入后仍可安全冻结。
#[test]
fn chunk_mut_allows_zero_copy_write_followed_by_freeze() {
    let recycler = Arc::new(RecordingRecycler::default());
    let mut buffer = PooledBuffer::new(BytesMut::with_capacity(4), recycler.clone());
    let chunk = buffer.chunk_mut();
    assert!(chunk.len() >= 2, "分配的可写空间应不小于示例写入");
    chunk[0].write(b'X');
    chunk[1].write(b'Y');
    buffer
        .advance_mut(2)
        .expect("advance_mut 应根据写入字节推进指针");
    assert_eq!(buffer.written(), 2);
    let mut readable = Box::new(buffer).freeze().expect("freeze 之后应转换为只读");
    let mut out = [0u8; 2];
    readable
        .copy_into_slice(&mut out)
        .expect("冻结后的缓冲应可读取");
    assert_eq!(&out, b"XY");
    drop(readable);
    let events = recycler.take_events();
    assert_eq!(events.len(), 1, "生命周期结束时应触发一次回收");
    assert!(events[0].1, "预期成功夺回底层 BytesMut");
}

/// 拆分出的多个视图应共享同一租约，并在全部释放后才触发回收。
#[test]
fn split_to_preserves_lease_and_defer_recycle_until_all_views_drop() {
    let recycler = Arc::new(RecordingRecycler::default());
    let mut buffer = PooledBuffer::new(BytesMut::with_capacity(16), recycler.clone());
    buffer.put_slice(b"abcdef").expect("初始写入不应失败");
    let mut head = buffer
        .split_to(2)
        .expect("拆分前缀应获得新的 ReadableBuffer");
    assert_eq!(head.remaining(), 2);
    head.advance(2).expect("拆分片段应能推进至末尾");
    drop(head);
    assert!(
        recycler.take_events().is_empty(),
        "仍有剩余视图持有租约时不应回收"
    );
    drop(buffer);
    let events = recycler.take_events();
    assert_eq!(events.len(), 1, "所有视图释放后应触发一次回收");
}

/// 拆分与读取接口在越界情况下应返回错误，防止未定义行为。
#[test]
fn split_to_and_read_operations_validate_bounds() {
    let recycler = Arc::new(RecordingRecycler::default());
    let mut buffer = PooledBuffer::new(BytesMut::with_capacity(8), recycler.clone());
    buffer.put_slice(b"rust").expect("写入字符串字节不应失败");
    assert!(buffer.split_to(10).is_err(), "拆分超出剩余长度应报错");
    buffer.advance(2).expect("前进读指针应成功");
    let mut dst = [0u8; 2];
    buffer
        .copy_into_slice(&mut dst)
        .expect("读取剩余字节应成功");
    assert_eq!(&dst, b"st");
    assert!(
        buffer.copy_into_slice(&mut [0u8; 1]).is_err(),
        "剩余字节不足时应返回错误"
    );
    drop(buffer);
    assert_eq!(recycler.take_events().len(), 1);
}

/// `advance_mut` 在推进超过剩余可写空间时应返回错误。
#[test]
fn advance_mut_rejects_out_of_bounds_progress() {
    let recycler = Arc::new(RecordingRecycler::default());
    let mut buffer = PooledBuffer::new(BytesMut::with_capacity(2), recycler.clone());
    let span = buffer.chunk_mut().len();
    assert!(
        buffer.advance_mut(span + 1).is_err(),
        "推进超过剩余容量应失败"
    );
    drop(buffer);
    assert_eq!(recycler.take_events().len(), 1);
}

/// `write_from` 应同步推进源缓冲指针，且在读取超界时返回错误。
#[test]
fn write_from_moves_requested_bytes_and_updates_source() {
    let recycler = Arc::new(RecordingRecycler::default());
    let mut buffer = PooledBuffer::new(BytesMut::with_capacity(16), recycler.clone());
    let pool = SlabBufferPool::new();
    let mut src = pool.alloc_readable(b"12345").expect("应能生成只读源缓冲");
    buffer
        .write_from(src.as_mut(), 3)
        .expect("从只读缓冲写入应成功");
    assert_eq!(buffer.written(), 3);
    assert_eq!(src.remaining(), 2, "源缓冲应同步推进读指针");
    assert!(
        buffer.write_from(src.as_mut(), 10).is_err(),
        "读取超出剩余字节数应失败"
    );
    drop(buffer);
    assert_eq!(recycler.take_events().len(), 1);
}

/// 扩容后应刷新租约记录的容量，回收事件应看到最新值。
#[test]
fn reserve_refreshes_capacity_recorded_by_recycler() {
    let recycler = Arc::new(RecordingRecycler::default());
    let mut buffer = PooledBuffer::new(BytesMut::with_capacity(4), recycler.clone());
    let before = buffer.capacity();
    buffer.reserve(64).expect("扩容请求不应失败");
    let after = buffer.capacity();
    assert!(after >= before);
    drop(buffer);
    let events = recycler.take_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, after, "租约记录的容量应刷新为扩容后的值");
    assert!(events[0].1, "扩容后的缓冲仍应成功回收");
}

/// `ReclaimedBuffer` 的构造与字段访问应保持元数据一致。
#[test]
fn reclaimed_buffer_retains_metadata() {
    let capacity = 32;
    let buf = BytesMut::with_capacity(capacity);
    let reclaimed = ReclaimedBuffer::new(capacity, Some(buf));
    assert_eq!(reclaimed.capacity(), capacity);
    assert!(reclaimed.into_buffer().is_some());
}
