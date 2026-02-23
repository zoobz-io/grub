# Test Review

Find untested code, broken test infrastructure, and tests that lie about what they verify.

## Principles

1. **Untested code is undefended code** — No test means no safety net. Any change can break it silently.
2. **Test infrastructure must work or nothing works** — Broken helpers mislocate failures. Missing conventions create confusion.
3. **Coverage without quality is a lie** — A test that hits a line without checking the result is decoration, not verification.
4. **Missing tests are findings** — If code exists, tests should exist. Absence is not neutral.

## Execution

1. Read `checklist.md` in this skill directory
2. Run `go build ./...` — if it doesn't build, stop
3. Run `go test -race ./...` — observe what passes, what fails, what's missing
4. Work through each phase — hunt for gaps, weak patterns, and false confidence
5. For each finding, ask: "If I introduced a bug here, would any test catch it?"
6. Compile findings into structured report

## Specifications

### 1:1 Test Mapping

Every `.go` file MUST have a corresponding `_test.go` file. Exceptions only for:
- Pure delegation files
- Re-export files
- Trivial wiring with no logic

Every exception is suspect. Challenge it.

### testing/ Directory

MUST contain:
- `testing/README.md` — Testing strategy overview
- `testing/helpers.go` — Domain-specific test helpers
- `testing/helpers_test.go` — Tests for helpers
- `testing/benchmarks/` — Performance tests
- `testing/integration/` — End-to-end tests

Missing components are findings.

### Helper Conventions

Helpers MUST:
- Have build tag: `//go:build testing`
- Call `t.Helper()` as first statement (mislocated failures without this)
- Accept `*testing.T` as first parameter
- Be domain-specific, not generic utilities

### Test Quality

Tests that lie about coverage:
- **No assertions** — Calls code but doesn't verify outcome
- **Only happy path** — Skips error handling, edge cases
- **Weak assertions** — Checks existence, not correctness
- **Tautological** — Asserts what was just set
- **Mock everything** — Tests wiring, not behavior

### Benchmark Conventions

Benchmarks must be honest:
- Input allocated outside the loop where appropriate (allocation inside skews results)
- `b.ResetTimer()` called after setup, before the measured loop
- Compiler can't optimize away the measured operation
- `b.ReportAllocs()` present
- Parallel variants included where applicable

### CI Coverage

CI MUST capture coverage from both unit and integration tests, merge profiles, and upload combined reports.

## Output

### Finding Format

```markdown
## Findings

| ID | Category | Severity | Location | Description |
|----|----------|----------|----------|-------------|
| TST-001 | [mapping/infrastructure/quality/benchmark/ci] | [critical/high/medium/low] | [file] | [what's wrong] |

### TST-001: [Title]

**Category:** [Test Mapping | Infrastructure | Quality | Benchmark | CI Coverage]
**Severity:** [Critical | High | Medium | Low]
**Location:** [file or directory]
**Description:** [What's missing or broken]
**Impact:** [What defects would survive because of this gap]
**Evidence:** [Missing file, weak assertion, broken helper]
**Recommendation:** [How to fix it]
```
