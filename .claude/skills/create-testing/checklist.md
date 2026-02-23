# Testing Create Checklist

## Phase 1: Discovery

### Package Understanding
- [ ] List all `.go` files in the package root
- [ ] Identify which files need corresponding `_test.go` files
- [ ] Review existing tests if any
- [ ] Identify domain-specific testing needs

### Scope Determination
- [ ] Ask: "Is this a new test setup or adding to existing?"
- [ ] Ask: "What external dependencies need integration tests?" (databases, APIs, etc.)
- [ ] Ask: "What operations need benchmarking?"

## Phase 2: Unit Tests

### 1:1 Mapping
For each source file, ensure a corresponding test file exists:

- [ ] `api.go` → `api_test.go`
- [ ] `[feature].go` → `[feature]_test.go`
- [ ] No orphan test files (test without source)

**Exception:** Files containing only delegation, re-exports, or trivial wiring may omit tests if there is no testable logic.

### Test Conventions
- [ ] Tests use `testing` package
- [ ] Table-driven tests for multiple cases
- [ ] Test both success and failure paths
- [ ] Use `t.Run()` for subtests
- [ ] Use `t.Parallel()` where safe

### Test Commands
Tests should run with:
```bash
go test -v -race -tags testing ./...
```

## Phase 3: testing/ Directory

### Structure
- [ ] Create `testing/` directory
- [ ] Create `testing/README.md`
- [ ] Create `testing/helpers.go`
- [ ] Create `testing/helpers_test.go`
- [ ] Create `testing/benchmarks/` directory
- [ ] Create `testing/benchmarks/README.md`
- [ ] Create `testing/integration/` directory
- [ ] Create `testing/integration/README.md`

### testing/README.md

```markdown
# Testing

Overview of testing strategy for [package].

## Running Tests

```bash
make test              # All tests with race detector
make test-unit         # Unit tests only (short mode)
make test-integration  # Integration tests
make test-bench        # Benchmarks
```

## Structure

- `helpers.go` — Domain-specific test utilities
- `benchmarks/` — Performance tests
- `integration/` — End-to-end tests

## Coverage

Target: 70% project, 80% new code
```

## Phase 4: Test Helpers

### helpers.go

```go
//go:build testing

package testing

import "testing"

// Helper template - customize for package domain
func AssertValid(t *testing.T, thing Thing) {
    t.Helper()
    if !thing.IsValid() {
        t.Errorf("expected valid thing, got: %+v", thing)
    }
}
```

### Helper Conventions
- [ ] Build tag: `//go:build testing`
- [ ] All helpers call `t.Helper()` first
- [ ] All helpers accept `*testing.T` as first parameter
- [ ] Helpers are domain-specific, not generic
- [ ] Assertions follow `Assert[Condition]` naming
- [ ] Setup functions follow `New[Thing]` or `With[Thing]` naming

### helpers_test.go
- [ ] Every helper function has tests
- [ ] Test both passing and failing cases
- [ ] Verify error messages are helpful

## Phase 5: Benchmarks

### testing/benchmarks/README.md

```markdown
# Benchmarks

Performance tests for [package].

## Running

```bash
make test-bench
# or
go test -tags testing -bench=. -benchmem ./testing/benchmarks/...
```

## Results

Document baseline results here for comparison.
```

### Benchmark Conventions
- [ ] File naming: `[feature]_test.go`
- [ ] Function naming: `Benchmark[Operation]`
- [ ] Include memory benchmarks (`-benchmem`)
- [ ] Document baseline results in README

## Phase 6: Integration Tests

### testing/integration/README.md

```markdown
# Integration Tests

End-to-end tests for [package].

## Prerequisites

[List any external dependencies: databases, services, etc.]

## Running

```bash
make test-integration
# or
go test -v -race -tags testing ./testing/integration/...
```

## Test Isolation

Each test should set up and tear down its own state.
```

### Integration Test Conventions
- [ ] File naming: `[feature]_test.go`
- [ ] Tests are self-contained (setup/teardown)
- [ ] Skip if dependencies unavailable: `t.Skip("requires [dependency]")`
- [ ] Use build tags if needed: `//go:build integration`

## Phase 7: CI Coverage Configuration

Integration tests must contribute to coverage reports.

### Coverage Workflow Updates

Add to `.github/workflows/coverage.yml`:

```yaml
- name: Run tests with coverage
  run: |
    # Unit test coverage
    go test -v -race -tags testing -coverprofile=coverage-unit.out -covermode=atomic ./...

    # Integration test coverage (separate profile)
    go test -v -race -tags testing -coverprofile=coverage-integration.out -covermode=atomic ./testing/integration/...

    # Merge coverage profiles
    echo "mode: atomic" > coverage.out
    tail -n +2 coverage-unit.out >> coverage.out
    tail -n +2 coverage-integration.out >> coverage.out 2>/dev/null || true

- name: Upload coverage
  uses: codecov/codecov-action@v4
  with:
    files: ./coverage.out
    flags: unit,integration
```

### Makefile Updates

Ensure Makefile supports combined coverage:

```makefile
coverage: ## Generate coverage report (unit + integration)
	@go test -tags testing -coverprofile=coverage-unit.out -covermode=atomic ./...
	@go test -tags testing -coverprofile=coverage-integration.out -covermode=atomic ./testing/integration/... 2>/dev/null || true
	@echo "mode: atomic" > coverage.out
	@tail -n +2 coverage-unit.out >> coverage.out
	@tail -n +2 coverage-integration.out >> coverage.out 2>/dev/null || true
	@go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
	@echo "Coverage report: coverage.html"
```

### Codecov Flags

Update `.codecov.yml` to track coverage sources:

```yaml
flags:
  unit:
    paths:
      - "*.go"
      - "!testing/**"
    carryforward: true
  integration:
    paths:
      - "testing/integration/**"
    carryforward: true
```

## Phase 8: Verification

- [ ] `make test` passes
- [ ] `make test-integration` passes (or skips gracefully)
- [ ] `make test-bench` runs benchmarks
- [ ] `make coverage` generates combined report
- [ ] All source files have corresponding test files
- [ ] Helpers are tested
- [ ] CI workflow captures integration coverage
