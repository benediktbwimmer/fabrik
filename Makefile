.PHONY: up down status smoke-eager-count

up:
	./scripts/dev-stack.sh up

down:
	./scripts/dev-stack.sh down

status:
	./scripts/dev-stack.sh status

smoke-eager-count:
	./scripts/eager-count-smoke.sh
