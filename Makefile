.PHONY: coverage unit-tests specific-unit-test benchmarks clippy pre-commit

coverage:
	cargo +nightly tarpaulin --engine llvm --out html

unit-tests:
	cargo nextest run --no-capture --no-fail-fast

e2e-tests:
	cargo nextest run --release --features=e2e-tests --no-fail-fast --no-capture --test=end-to-end

specific-unit-test:
	@read -p "Enter the unit test name: " test_name; \
	cargo nextest run --features=e2e-tests --no-fail-fast --no-capture -- $$test_name

benchmarks:
	cargo bench

clippy:
	cargo clippy

pre-commit: clippy unit-tests e2e-tests coverage benchmarks
	@echo "All pre-commit checks completed."

fuzz-node:
	cargo fuzz run --release node -- -rss_limit_mb=8192 -max_len=450000 -len_control=0

fuzz-protocol:
	cargo fuzz run --release protocol -- -rss_limit_mb=8192 -max_len=450000 -len_control=0

help:
	@echo "Available targets:"
	@echo "  help                - Display this help message"
	@echo "  coverage            - Run code coverage analysis"
	@echo "  unit-tests          - Run all unit tests"
	@echo "  e2e-tests    - Run all end-to-end tests"
	@echo "  benchmarks          - Run benchmarks"
	@echo "  clippy              - Run Clippy linter"
	@echo "  pre-commit          - Run all pre-commit checks"
	@echo "  specific-unit-test  - Run a specific unit test (prompts for test name). For programmatic usage, you can use \n\t\t\t'echo \"testname\" | make specific-unit-test'"
