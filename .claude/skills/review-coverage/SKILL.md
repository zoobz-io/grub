# Coverage Review

Find tests that inflate coverage without catching defects, and code paths that survive without any safety net.

## Principles

1. **Coverage percentage is a vanity metric** — 90% coverage with weak assertions means 90% of the code is touched but not verified.
2. **A test that passes but doesn't check anything is worse than no test** — It says "this works!" when nobody actually verified it.
3. **Uncovered critical paths are undefended** — Public API, error handling, security-sensitive code without tests are open vulnerabilities.
4. **If I introduced a bug here, would any test catch it?** — The only question that matters.

## Execution

1. Read `checklist.md` in this skill directory
2. Run coverage analysis: `go test -coverprofile=coverage.out ./...`
3. Examine coverage report for gaps and false confidence
4. Audit test quality in covered code — are the tests real?
5. Compile findings into structured report

## Specifications

### Flaccid Test Patterns

Tests that inflate coverage without providing value:

| Pattern | Problem | How to Detect |
|---------|---------|---------------|
| No assertions | Calls code but doesn't verify | Function called, result thrown away or only `err == nil` |
| Only happy path | Error handling untested | No test triggers error returns |
| Weak assertions | Checks existence, not correctness | `assert.NotNil` without checking actual value |
| Mock everything | Tests wiring, not behavior | Every dependency mocked, only mock interactions verified |
| Tautological | Asserts what was just set | `got := "foo"; assert(got == "foo")` |

Each flaccid test is a finding — it lies about what's verified.

### Quality Indicators

Good coverage includes:

| Indicator | What to Check |
|-----------|---------------|
| Error paths tested | Every error return has a test that triggers it |
| Boundary conditions | Zero, one, many, max, overflow |
| Nil/empty handling | Nil inputs, empty slices, zero values |
| Concurrent safety | Race conditions under `-race` flag |
| State transitions | All valid state changes exercised |

### Coverage Priority

Focus efforts on what matters most:

1. Public API surface — consumers depend on this
2. Error handling paths — failures must be handled correctly
3. Security-sensitive code — authentication, authorization, input validation
4. Complex business logic — branches, conditions, edge cases
5. Recently changed code — new code needs new tests

Lower priority (findings still valid but lower severity):
- Simple getters/setters
- Delegation-only code
- Generated code

## Output

### Finding Format

```markdown
## Findings

| ID | Category | Severity | Location | Description |
|----|----------|----------|----------|-------------|
| COV-001 | [flaccid/uncovered/false-confidence] | [critical/high/medium/low] | [file:line] | [what's wrong] |

### COV-001: [Title]

**Category:** [Flaccid Test | Uncovered Path | False Confidence]
**Severity:** [Critical | High | Medium | Low]
**Location:** [file:line range]
**Description:** [What's undefended or falsely defended]
**Impact:** [What defect would survive because of this]
**Evidence:** [Coverage report line, weak assertion, missing test]
**Recommendation:** [What test should exist, what assertion should be stronger]
```
