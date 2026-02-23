# Test Review Checklist

## Phase 1: Inventory — What Exists?

### Source Files
- [ ] List all `.go` files in package root (excluding `_test.go`)
- [ ] List all `_test.go` files in package root
- [ ] Identify source files without test files — each is a finding
- [ ] Identify orphan test files without corresponding source — suspect

### testing/ Directory
- [ ] Does `testing/` directory exist?
- [ ] Does `testing/README.md` exist?
- [ ] Does `testing/helpers.go` exist?
- [ ] Does `testing/helpers_test.go` exist?
- [ ] Does `testing/benchmarks/` exist?
- [ ] Does `testing/benchmarks/README.md` exist?
- [ ] Does `testing/integration/` exist?
- [ ] Does `testing/integration/README.md` exist?
- [ ] Every missing component is a finding

## Phase 2: 1:1 Mapping — Where Are the Gaps?

### Completeness
For each source file without a test file:
- [ ] Is it a pure delegation file? (rare valid exception)
- [ ] Is it a re-export file? (rare valid exception)
- [ ] Is it trivial wiring with zero logic? (challenge this claim)
- [ ] If none of the above, it's untested code — finding

### Orphan Tests
- [ ] Any test files without corresponding source files?
- [ ] Are these testing deleted/moved code? (stale tests)

## Phase 3: Test Quality — Which Tests Lie?

### Assertion Quality
For each test file:
- [ ] Do tests have meaningful assertions — or just call functions?
- [ ] Do assertions check values — or just check `err == nil`?
- [ ] Are assertions specific — or so loose anything passes?
- [ ] Do tests check the actual behavior — or just confirm the mock?

### Path Coverage
- [ ] Are error paths tested — or only the happy path?
- [ ] Are boundary conditions tested (zero, one, many, max)?
- [ ] Are nil/empty inputs tested?
- [ ] Is concurrent behavior tested (race conditions)?
- [ ] Are state transitions tested?

### Test Patterns
- [ ] Tests use `testing` package — not third-party frameworks?
- [ ] Table-driven tests for multiple cases?
- [ ] `t.Run()` for subtests?
- [ ] `t.Parallel()` where safe?
- [ ] Race detector enabled (`-race` flag)?

### Test Isolation
- [ ] Tests don't depend on execution order — or do they?
- [ ] Tests clean up after themselves — or leave state?
- [ ] No shared mutable state between tests — or is there leakage?

## Phase 4: Helper Assessment — Is the Infrastructure Sound?

### helpers.go
- [ ] Build tag present: `//go:build testing` — or missing?
- [ ] All helpers call `t.Helper()` as first statement — or will failures mislocate?
- [ ] All helpers accept `*testing.T` as first parameter?
- [ ] Helpers are domain-specific — or generic utilities that belong elsewhere?

### helpers_test.go
- [ ] Every exported helper has tests — or are helpers untested?
- [ ] Both passing and failing cases covered?
- [ ] Error messages verified to be helpful — or just checked for non-nil?

## Phase 5: Benchmark Assessment — Do Benchmarks Tell the Truth?

### Structure
- [ ] Benchmarks exist in `testing/benchmarks/` — or missing entirely?
- [ ] README documents how to run and interpret?
- [ ] Baseline results documented?

### Honesty
- [ ] Input allocated inside the benchmark loop — or outside to hide cost?
- [ ] Measured operation can't be optimized away by the compiler?
- [ ] `b.ReportAllocs()` present — or allocation cost hidden?
- [ ] Parallel variants included — or only serial?
- [ ] Function naming: `Benchmark[Operation]`?

## Phase 6: Integration Test Assessment — Are System Tests Real?

### Structure
- [ ] Integration tests exist in `testing/integration/` — or missing?
- [ ] README documents prerequisites and setup?
- [ ] Tests are self-contained (setup/teardown)?

### Conventions
- [ ] Tests skip gracefully if dependencies unavailable?
- [ ] Build tags used if needed: `//go:build integration`?
- [ ] External dependencies documented?

## Phase 7: CI Coverage Configuration — Does CI Catch What It Claims?

### Coverage Workflow
- [ ] CI runs unit tests with coverage — or without?
- [ ] CI runs integration tests with coverage — or ignores them?
- [ ] Coverage profiles are merged before upload — or only partial?
- [ ] Combined coverage uploaded to Codecov?

### Thresholds
- [ ] Project target: 70% — is it met?
- [ ] Patch target: 80% — is it enforced?

### Makefile
- [ ] `make test` passes?
- [ ] `make coverage` generates combined report?
- [ ] Coverage includes integration tests?
