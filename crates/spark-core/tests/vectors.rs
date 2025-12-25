//! 向量化用例测试入口，聚合黄金输出验证。
//!
//! 暴露 `tests::vectors::golden` 命名空间，确保可以使用
//! `cargo test -- tests::vectors::golden::*` 精准筛选。

#[cfg(feature = "std_json")]
#[path = "vectors/golden.rs"]
pub mod golden_impl;

pub mod tests {
    pub mod vectors {
        pub mod golden {
            #[cfg(feature = "std_json")]
            /// 桥接到实际实现，保证测试路径与文档一致。
            #[test]
            fn golden_vectors_match() {
                crate::golden_impl::golden_vectors_match();
            }
        }
    }
}
