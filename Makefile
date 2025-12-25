.PHONY: ci-lints ci-zc-asm ci-no-std-alloc ci-doc-warning ci-bench-smoke ci-fuzz-regression

ci-lints:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo run --quiet --package spark-deprecation-lint
	./tools/ci/public_api_diff_budget.sh
	./tools/ci/check_public_trait_budget.sh
	./tools/ci/check_observability_keys.sh
	./tools/ci/check_contracts_index.sh

ci-zc-asm:
	cargo build --workspace

ci-no-std-alloc:
	cargo build --workspace --no-default-features --features alloc

ci-doc-warning:
	cargo doc --workspace

ci-bench-smoke:
	# `spark-macros` 为 proc-macro crate，未引入 Criterion，因此需排除以避免 `--quick` 参数触发 libtest 错误。
	@rm -f bench.out
	/bin/bash -o pipefail -c 'cargo bench --workspace --exclude spark-macros -- --quick | tee bench.out'
	python3 tools/ci/check_zerocopy_bench.py bench.out

ci-fuzz-regression:
	./tools/ci/fuzz_regression.sh
