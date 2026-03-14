.PHONY: up down status smoke-eager-count benchmark-compare-temporal benchmark-compare-temporal-target benchmark-streaming-release-gate

up:
	./scripts/dev-stack.sh up

down:
	./scripts/dev-stack.sh down

status:
	./scripts/dev-stack.sh status

smoke-eager-count:
	./scripts/eager-count-smoke.sh

benchmark-compare-temporal:
	./scripts/run-temporal-comparison-benchmark.sh --profile gate

benchmark-compare-temporal-target:
	./scripts/run-temporal-comparison-benchmark.sh --profile target

benchmark-streaming-release-gate:
	./scripts/run-streaming-release-gate.sh
