.PHONY: test test-unit test-integration test-bench lint lint-fix security coverage clean help check ci install-tools install-hooks

.DEFAULT_GOAL := help

help: ## Display available commands
	@echo "grub Development Commands"
	@echo "=============================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

test: ## Run all tests with race detector
	@go test -v -race -tags testing ./...

test-unit: ## Run unit tests only (short mode)
	@go test -v -race -tags testing -short $(shell go list ./... | grep -v '/testing/')

test-integration: ## Run integration tests
	@go test -v -tags testing -timeout=10m ./testing/integration/...

test-bench: ## Run benchmark tests
	@go test -v -race -tags testing -timeout=10m ./testing/benchmarks/...

lint: ## Run linters
	@golangci-lint run --config=.golangci.yml --timeout=5m

lint-fix: ## Run linters with auto-fix
	@golangci-lint run --config=.golangci.yml --fix

security: ## Run security scanner
	@gosec -quiet ./...

coverage: ## Generate coverage report (HTML)
	@go test -tags testing -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
	@echo "Coverage report: coverage.html"

clean: ## Remove generated files
	@rm -f coverage.out coverage.html coverage.txt
	@find . -name "*.test" -delete
	@find . -name "*.prof" -delete
	@find . -name "*.out" -delete

install-tools: ## Install development tools
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2
	@go install github.com/securego/gosec/v2/cmd/gosec@latest

install-hooks: ## Install git pre-commit hook
	@mkdir -p .git/hooks
	@echo '#!/bin/sh' > .git/hooks/pre-commit
	@echo 'make check' >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed"

check: lint test ## Run lint and tests
	@echo "All checks passed!"

ci: clean check coverage ## Full CI simulation
	@echo "CI simulation complete!"
