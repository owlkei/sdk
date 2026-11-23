.DEFAULT_GOAL := help

.PHONY: help
help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Install the JavaScript dependencies and the git hooks
	yarn install --frozen-lockfile
	cd js && yarn install --frozen-lockfile

.PHONY: check
check: check-spec check-js check-go ## Everything CI runs, for every language

.PHONY: check-spec
check-spec: ## Validate the protocol specs and their test vectors
	node scripts/check-spec.mjs

.PHONY: check-js
check-js: ## TypeScript: format, lint, types, tests, build, package and size checks
	cd js && yarn run check

.PHONY: check-go
check-go: ## Go: gofmt, vet, golangci-lint, tests with the race detector
	@cd go && test -z "$$(gofmt -l .)" || (gofmt -l .; echo "run gofmt -w"; exit 1)
	cd go && go vet ./...
	cd go && golangci-lint run ./...
	cd go && go test -race ./...
