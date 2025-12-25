use alloc::vec::Vec;
use core::{fmt, iter::FusedIterator};

/// `BufView` 为上层组件提供零拷贝的只读缓冲观察接口。
///
/// # 设计初衷（Why）
/// - **跨层共享语义**：在 `ReadableBuffer` 强调所有权迁移的同时，业务往往只需临时借用；`BufView` 用于暴露只读、零拷贝的轻量视图。
/// - **生命周期对齐**：通过借用 `&self` 获取视图，保证底层缓冲只要未被可变访问就能稳定暴露切片，避免不必要的 `clone`/`Vec` 分配。
/// - **可测试性**：统一的视图接口便于在契约测试 (TCK) 中断言零拷贝约束，例如“切片指针相同”“多分片长度一致”。
///
/// # 实现逻辑（How）
/// - `as_chunks` 返回 [`Chunks`] 迭代器，内部仅存储对原始分片的借用，不会复制字节数据。
/// - `len`/`is_empty` 负责汇总总字节数，默认语义要求与分片长度总和一致；`chunk_count` 基于迭代器提供分片数量观测。
/// - 典型实现可将内部 `VecDeque`、`Bytes`、`Box<[u8]>` 等结构映射为若干 `&[u8]` 切片，再由 [`Chunks::from_vec`] 包装成迭代器。
///
/// # 契约条款（What）
/// - **输入/前置条件**：
///   1. `self` 必须代表一段只读数据，在视图生命周期内不得被写入或释放。
///   2. 若缓冲存在分片，调用 `as_chunks` 时需保证分片切片的生命周期至少与返回的迭代器同长。
/// - **返回值/后置条件**：
///   1. `as_chunks` 返回的迭代器应稳定产出零拷贝切片，且切片按逻辑顺序排列。
///   2. `len` 必须等于所有分片 `len()` 之和，`is_empty` 等价于 `len() == 0`。
///   3. `chunk_count` 返回值代表逻辑分片数，供测试判定是否触发了分片折叠或额外拷贝。
///
/// # 设计考量与注意事项（Trade-offs & Gotchas）
/// - **对象安全**：接口仅包含 `&self` 方法与返回具体结构的默认实现，保持对象安全，便于通过 trait 对象在运行时传递视图。
/// - **生命周期约束**：分片迭代器借用了 `self`，调用方不得持有超出视图生命周期的引用；如需延长生命周期，应显式复制。
/// - **性能权衡**：`Chunks::from_vec` 需要分配 `Vec<&[u8]>`，但只存指针；如实现者希望零分配，可使用 [`Chunks::from_single`] 或实现自定义压缩结构后再封装。
/// - **边界情况**：若底层缓冲中途缩容或释放，违反“视图期间不可变”将导致未定义行为；因此 `BufView` 明确要求实现者在暴露视图时冻结内部状态。
pub trait BufView: Send + Sync {
    /// 以零拷贝方式返回当前缓冲的分片迭代器。
    ///
    /// - **Why**：统一的“借用分片”接口便于跨组件快速完成 `IOVec`/`scatter-gather` 发送。
    /// - **How**：实现者需将自身状态拆分为 `&[u8]` 切片，再使用 [`Chunks`] 封装；若底层仅有单片，可直接调用 [`Chunks::from_single`].
    /// - **Contract**：迭代器生命周期不得超过 `&self`，且不得返回悬垂引用或重复拷贝。
    fn as_chunks(&self) -> Chunks<'_>;

    /// 返回视图内的总字节数。
    ///
    /// - **Why**：调用方需要快速获取逻辑长度以决定发送窗口或流控策略。
    /// - **How**：默认实现建议直接复用底层缓存的 `len`/`remaining` 指标。
    /// - **Contract**：必须满足 `self.len() == self.as_chunks().total_len()`, 若无法恒成立应视为违反零拷贝契约。
    fn len(&self) -> usize;

    /// 判断视图是否为空，默认实现基于 `len`。
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 返回分片数量，主要用于测试或调试观察。
    ///
    /// - **Why**：许多契约测试需要断言实现是否保持原始分片数量，以监测是否发生了隐式复制或折叠。
    /// - **How**：通过完整迭代 [`Chunks`] 统计实际产生的分片数量，同时比对 `size_hint` 作为防御式检查。
    /// - **Contract**：返回值等于真实的分片个数；若实现的 `size_hint` 与实际不符，会在调试构建中触发断言提醒开发者修复。
    fn chunk_count(&self) -> usize {
        let mut chunks = self.as_chunks();
        let (count, _) = chunks.size_hint();
        // 迭代一次确保 size_hint 与实际一致。
        let mut consumed = 0usize;
        while chunks.next().is_some() {
            consumed += 1;
        }
        debug_assert_eq!(count, consumed, "Chunks::size_hint 不应与实际分片数不一致");
        consumed
    }
}

/// `Chunks` 封装零拷贝分片迭代逻辑。
///
/// # 设计初衷（Why）
/// - 为 `BufView` 提供统一的惰性迭代器接口，允许调用方使用 `for` 循环或 `Iterator` 组合操作处理分片。
/// - 通过区分 `Single` 与 `Many` 两种模式，避免对单片数据额外分配容器。
///
/// # 实现逻辑（How）
/// - 内部采用枚举保存状态：
///   - `Empty`：无任何字节；
///   - `Single`：仅包含一段切片，用布尔标记消费状态；
///   - `Many`：存储 `Vec<&[u8]>` 与游标索引，按顺序返回。
/// - `Iterator::next` 基于状态机推进；`ExactSizeIterator` 的 `len` 与 `size_hint` 保持同步，保证测试中 `len()` 可直接返回剩余分片数。
///
/// # 契约条款（What）
/// - **输入**：
///   - `from_single` 需要一段生命周期 `'a` 的切片；
///   - `from_vec` 接受已准备好的切片向量，不会复制底层字节。
/// - **后置条件**：
///   - `total_len` 返回剩余分片的字节数之和；
///   - 迭代结束后返回 `None`，并保持“耗尽即空”的幂等性。
///
/// # 注意事项（Trade-offs & Gotchas）
/// - 迭代器本身不保证分片之间的字节连续性，仅保证逻辑顺序；需要连续视图的调用方可自行检测是否只有单片。
/// - `from_vec` 会复制切片引用，若底层动态变化需确保引用仍然有效；可在暴露视图前进行 `freeze` 或引用计数。
/// - 若未来引入自定义压缩结构，可通过新增 `from_boxed_slice` 等辅助构造函数以减少临时分配。
#[derive(Clone)]
pub struct Chunks<'a> {
    repr: ChunksRepr<'a>,
}

impl<'a> Chunks<'a> {
    /// 构建空迭代器。
    pub fn empty() -> Self {
        Self {
            repr: ChunksRepr::Empty,
        }
    }

    /// 以单片切片构建迭代器。
    pub fn from_single(chunk: &'a [u8]) -> Self {
        if chunk.is_empty() {
            Self::empty()
        } else {
            Self {
                repr: ChunksRepr::Single {
                    chunk,
                    consumed: false,
                },
            }
        }
    }

    /// 以分片向量构建迭代器，向量元素会按原有顺序返回。
    pub fn from_vec(chunks: Vec<&'a [u8]>) -> Self {
        let filtered: Vec<&'a [u8]> = chunks
            .into_iter()
            .filter(|chunk| !chunk.is_empty())
            .collect();
        if filtered.is_empty() {
            Self::empty()
        } else if filtered.len() == 1 {
            Self::from_single(filtered[0])
        } else {
            Self {
                repr: ChunksRepr::Many {
                    slices: filtered,
                    index: 0,
                },
            }
        }
    }

    /// 返回剩余分片总字节数，主要用于验证零拷贝契约。
    pub fn total_len(&self) -> usize {
        match &self.repr {
            ChunksRepr::Empty => 0,
            ChunksRepr::Single { chunk, consumed } => {
                if *consumed {
                    0
                } else {
                    chunk.len()
                }
            }
            ChunksRepr::Many { slices, index } => {
                slices[*index..].iter().map(|chunk| chunk.len()).sum()
            }
        }
    }
}

impl<'a> Iterator for Chunks<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.repr {
            ChunksRepr::Empty => None,
            ChunksRepr::Single { chunk, consumed } => {
                if *consumed {
                    None
                } else {
                    *consumed = true;
                    Some(*chunk)
                }
            }
            ChunksRepr::Many { slices, index } => {
                if *index >= slices.len() {
                    None
                } else {
                    let chunk = slices[*index];
                    *index += 1;
                    Some(chunk)
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match &self.repr {
            ChunksRepr::Empty => 0,
            ChunksRepr::Single { consumed, .. } => {
                if *consumed {
                    0
                } else {
                    1
                }
            }
            ChunksRepr::Many { slices, index } => slices.len().saturating_sub(*index),
        };
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for Chunks<'a> {
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

impl<'a> FusedIterator for Chunks<'a> {}

impl<'a> fmt::Debug for Chunks<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Chunks")
            .field("remaining_chunks", &self.len())
            .field("remaining_bytes", &self.total_len())
            .finish()
    }
}

#[derive(Clone)]
enum ChunksRepr<'a> {
    Empty,
    Single { chunk: &'a [u8], consumed: bool },
    Many { slices: Vec<&'a [u8]>, index: usize },
}

impl BufView for [u8] {
    fn as_chunks(&self) -> Chunks<'_> {
        Chunks::from_single(self)
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}

impl BufView for alloc::boxed::Box<[u8]> {
    fn as_chunks(&self) -> Chunks<'_> {
        Chunks::from_single(self.as_ref())
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

impl BufView for alloc::vec::Vec<u8> {
    fn as_chunks(&self) -> Chunks<'_> {
        Chunks::from_single(self.as_slice())
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<T> BufView for &T
where
    T: BufView + ?Sized,
{
    fn as_chunks(&self) -> Chunks<'_> {
        (**self).as_chunks()
    }

    fn len(&self) -> usize {
        (**self).len()
    }
}

#[cfg(test)]
mod tests {
    extern crate alloc;

    use super::*;
    use alloc::vec;

    struct MultiSliceView<'a> {
        segments: &'a [&'a [u8]],
    }

    impl<'a> BufView for MultiSliceView<'a> {
        fn as_chunks(&self) -> Chunks<'_> {
            Chunks::from_vec(self.segments.to_vec())
        }

        fn len(&self) -> usize {
            self.segments.iter().map(|chunk| chunk.len()).sum()
        }
    }

    /// - **意图 (Why)**：验证 `Chunks::from_vec` 会丢弃空分片，并且在消费过程中保持长度与剩余字节数的正确性。
    /// - **实现说明 (How)**：构造包含空切片的输入向量，逐步调用 `next` 并对 `len`/`total_len` 断言，确保边界条件处理得当。
    /// - **契约 (What)**：测试的成功意味着 `Chunks` 在面对空切片时返回的迭代器满足“零拷贝 + 正确统计”的基础契约。
    #[test]
    fn chunks_from_vec_filters_empty_segments() {
        let mut iter = Chunks::from_vec(vec![b"ab".as_slice(), &[], b"cdef".as_slice()]);

        assert_eq!(iter.len(), 2);
        assert_eq!(iter.total_len(), 6);
        assert_eq!(iter.next(), Some(b"ab".as_slice()));

        assert_eq!(iter.len(), 1);
        assert_eq!(iter.total_len(), 4);
        assert_eq!(iter.next(), Some(b"cdef".as_slice()));

        assert_eq!(iter.len(), 0);
        assert_eq!(iter.total_len(), 0);
        assert_eq!(iter.next(), None);
    }

    /// - **意图 (Why)**：确保默认实现的 `chunk_count` 会返回真实分片数量，从而帮助上层测试捕获潜在的分片折叠或复制。
    /// - **实现说明 (How)**：使用测试视图 `MultiSliceView` 暴露两段静态切片，分别检查 `len`、`chunk_count` 与 `as_chunks` 的产出顺序。
    /// - **契约 (What)**：一旦测试通过，意味着 `BufView::chunk_count` 能在对象安全上下文中正确统计多分片视图的数量。
    #[test]
    fn buf_view_chunk_count_matches_actual_segments() {
        static SEGMENTS: [&[u8]; 2] = [b"hello", b"world"];
        let view = MultiSliceView {
            segments: &SEGMENTS,
        };

        assert_eq!(view.len(), 10);
        assert_eq!(view.chunk_count(), 2);

        let mut iter = view.as_chunks();
        assert_eq!(iter.next(), Some(SEGMENTS[0]));
        assert_eq!(iter.next(), Some(SEGMENTS[1]));
        assert_eq!(iter.next(), None);
    }
}
