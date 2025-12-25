//! `pool_contract` 集成测试：验证 `SlabBufferPool` 在真实调用路径下的契约执行情况。
//!
//! # 测试目标（Why）
//! - 保障缓冲租借、回收、统计等核心流程能在 crate 公开 API 下正确协作；
//! - 通过外部 crate 视角（integration test）模拟用户调用，避免依赖内部实现细节；
//! - 及时捕获统计字段、容量回收等回归，确保后续重构仍维持行为兼容。
//!
//! # 结构安排（How）
//! - `reusable_capacity_returns_to_pool`：验证写入并释放后可复用同一块内存；
//! - `stats_track_allocation_lifecycle`：检查 `PoolStats` 中自定义维度的一致性；
//! - 其它测试覆盖空负载、自由链表收缩等边界场景，形成全链路覆盖。

use spark_buffer::SlabBufferPool;
use spark_core::{BufferPool, buffer::PoolStats};

/// 帮助函数：在 `PoolStats::custom_dimensions` 中查找命名指标。
///
/// # 设计动机（Why）
/// - 集中处理 `Vec<PoolStatDimension>` 的遍历逻辑，避免在多处测试中重复样板代码。
///
/// # 契约说明（What）
/// - `stats`：待查询的统计快照；
/// - `key`：指标名称；
/// - 返回值：若存在该指标，则返回其 `usize` 数值，否则为 0（按业务默认值对齐）。
fn dimension(stats: &PoolStats, key: &str) -> usize {
    stats
        .custom_dimensions
        .iter()
        .find(|dim| dim.key == key)
        .map(|dim| dim.value)
        .unwrap_or_default()
}

/// 验证写入后释放租约能够让容量重新进入自由链表。
///
/// # 测试意图（Why）
/// - 若回收路径失效，`available_bytes` 将持续为 0，后续租借只能重新分配，导致性能回退。
///
/// # 步骤说明（How）
/// 1. 租借一个至少 64 字节的缓冲并写入示例数据；
/// 2. 在缓冲 `Drop` 后读取统计快照，确认 `available_bytes` 增长；
/// 3. 再次租借较小容量，确保可复用同一内存块并保持分配统计单调。
///
/// # 契约校验（What）
/// - 前置条件：缓冲池为默认实例，无任何租借；
/// - 后置条件：第二次租借后 `allocated_bytes` 不小于之前快照，说明统计路径稳定。
#[test]
fn reusable_capacity_returns_to_pool() {
    let pool = SlabBufferPool::new();
    {
        let mut writable = pool.alloc_writable(64).expect("租借缓冲失败");
        assert!(writable.remaining_mut() >= 64);
        writable.put_slice(&[1, 2, 3, 4]).expect("写入测试数据");
    }
    let snapshot = pool.statistics().expect("读取统计失败");
    assert!(snapshot.available_bytes >= 64);
    {
        let _second = pool.alloc_writable(16).expect("复用缓冲失败");
    }
    let after = pool.statistics().expect("读取统计失败");
    assert!(after.allocated_bytes >= snapshot.allocated_bytes);
}

/// 验证 `alloc_readable` 能够完整保留输入负载并冻结为只读视图。
#[test]
fn alloc_readable_preserves_payload() {
    let pool = SlabBufferPool::new();
    let payload = [9u8, 8, 7, 6];
    let mut readable = pool.alloc_readable(&payload).expect("分配只读缓冲失败");
    let mut out = [0u8; 4];
    readable.copy_into_slice(&mut out).expect("读取数据失败");
    assert_eq!(out, payload);
}

/// 通过多次租借 / 回收验证统计维度的生命周期演进。
///
/// # 核心关注点
/// - `active_buffers`：租借过程中的实时活跃数量；
/// - `total_allocated` / `total_recycled`：累计分配与回收次数；
/// - `pool_misses`：自由链表未命中次数，用于确认首次分配必然触发一次 miss。
#[test]
fn stats_track_allocation_lifecycle() {
    let pool = SlabBufferPool::new();
    let initial = pool.stats();
    assert_eq!(dimension(&initial, "total_allocated"), 0);
    assert_eq!(dimension(&initial, "total_recycled"), 0);
    assert_eq!(dimension(&initial, "pool_misses"), 0);

    {
        let _first = pool.alloc_writable(32).expect("首次租借失败");
        let during_first = pool.stats();
        assert_eq!(dimension(&during_first, "active_buffers"), 1);
        assert_eq!(dimension(&during_first, "total_allocated"), 1);
        assert_eq!(dimension(&during_first, "pool_misses"), 1);
        assert!(dimension(&during_first, "total_bytes") >= 32);
    }

    let after_first = pool.stats();
    assert_eq!(dimension(&after_first, "active_buffers"), 0);
    assert_eq!(dimension(&after_first, "total_allocated"), 1);
    assert_eq!(dimension(&after_first, "total_recycled"), 1);

    {
        let _second = pool.alloc_writable(8).expect("第二次租借失败");
        let during_second = pool.stats();
        assert_eq!(dimension(&during_second, "active_buffers"), 1);
        assert_eq!(dimension(&during_second, "total_allocated"), 2);
        assert_eq!(dimension(&during_second, "pool_misses"), 1);
    }

    let after_second = pool.stats();
    assert_eq!(dimension(&after_second, "active_buffers"), 0);
    assert_eq!(dimension(&after_second, "total_allocated"), 2);
    assert_eq!(dimension(&after_second, "total_recycled"), 2);
}

/// 验证 `shrink_to_fit` 能够释放自由链表缓存并刷新统计。
#[test]
fn shrink_to_fit_releases_cached_buffers() {
    let pool = SlabBufferPool::new();
    let cached_capacity = {
        let writable = pool.alloc_writable(48).expect("初次租借失败");
        writable.capacity()
    };
    let reclaimed = pool.shrink_to_fit().expect("释放自由链表容量不应失败");
    assert!(
        reclaimed >= cached_capacity,
        "回收字节数至少应覆盖已缓存容量"
    );
    let stats = pool.stats();
    assert_eq!(stats.available_bytes, 0, "收缩后不应保留闲置容量");
}

/// 验证空负载的只读分配不会触发异常。
#[test]
fn alloc_readable_accepts_empty_payload() {
    let pool = SlabBufferPool::new();
    let mut readable = pool.alloc_readable(&[]).expect("空负载的只读分配应成功");
    assert_eq!(readable.remaining(), 0);
    assert!(
        readable.copy_into_slice(&mut []).is_ok(),
        "读取零字节应平稳返回"
    );
}
