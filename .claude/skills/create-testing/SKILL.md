# Testing Create

Set up test infrastructure for a Go package: unit tests, helpers, benchmarks, integration tests, and CI coverage configuration.

## Principles

1. **1:1 mapping** — Every source file MUST have a corresponding test file
2. **Colocated tests** — Unit tests live next to source, infrastructure lives in `testing/`
3. **Helpers are tested** — Test utilities MUST themselves be tested
4. **Coverage includes integration** — CI MUST capture coverage from integration tests

## Execution

1. Read `checklist.md` in this skill directory
2. Determine scope: full setup or adding to existing?
3. Ask user about package-specific testing needs
4. Create files per specifications
5. Verify with `make test`

## Specifications

### Directory Structure

```
/
├── api.go
├── api_test.go           # Unit tests for api.go
├── cache.go
├── cache_test.go         # Unit tests for cache.go
└── testing/
    ├── README.md         # Testing strategy overview
    ├── helpers.go        # Domain-specific test utilities
    ├── helpers_test.go   # Tests for helpers themselves
    ├── benchmarks/
    │   ├── README.md
    │   └── core_test.go
    └── integration/
        ├── README.md
        └── [feature]_test.go
```

### 1:1 Mapping Rule

Every `.go` file MUST have a corresponding `_test.go` file.

Exception: Files containing only delegation, re-exports, or trivial wiring with no testable logic.

### Helper Requirements

`testing/helpers.go` MUST:
- Have build tag: `//go:build testing`
- Have all helpers call `t.Helper()` as first statement
- Have all helpers accept `*testing.T` as first parameter
- Be domain-specific, not generic utilities

Naming conventions:
- Assertions: `Assert[Condition]`
- Setup: `New[Thing]` or `With[Thing]`

### Coverage Targets

| Metric | Target | Threshold |
|--------|--------|-----------|
| Project | 70% | 1% drop allowed |
| Patch | 80% | 0% drop allowed |

### CI Integration Test Coverage

CI MUST capture integration test coverage. See checklist for exact workflow configuration.

## Prohibitions

DO NOT:
- Create test files without corresponding source files
- Create helpers that don't call `t.Helper()`
- Use third-party test frameworks (use standard `testing` package)
- Skip the `testing/helpers_test.go` file

## Output

A complete test infrastructure that:
- Enforces 1:1 source-to-test mapping
- Provides domain-specific test helpers
- Includes benchmark and integration test scaffolding
- Captures all test coverage in CI reports
