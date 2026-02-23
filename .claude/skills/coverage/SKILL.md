# Coverage

Analyze test coverage for quality, not just metrics. Identify flaccid tests and coverage that lies.

## Philosophy

Coverage percentage is a vanity metric. A codebase can have 90% coverage and still be undertested if:
- Tests hit lines without verifying behavior
- Assertions are weak or missing
- Error paths are "covered" but not validated
- Edge cases are skipped

This skill focuses on meaningful coverage.

## Execution

1. Read checklist.md in this skill directory
2. Run coverage and identify gaps
3. Audit test quality in covered code
4. Report findings with actionable recommendations

## Specifications

### Flaccid Test Patterns

Tests that inflate coverage without providing value:

| Pattern | Problem |
|---------|---------|
| No assertions | Calls code but doesn't verify outcome |
| Only happy path | Skips error handling, edge cases |
| Weak assertions | Checks existence, not correctness |
| Mock everything | Tests wiring, not behavior |
| Tautological | Asserts what was just set |

### Quality Indicators

Good coverage includes:

| Indicator | What to check |
|-----------|---------------|
| Error paths tested | Every error return has a test that triggers it |
| Boundary conditions | Zero, one, many, max, overflow |
| Nil/empty handling | Nil inputs, empty slices, zero values |
| Concurrent safety | Race conditions under test |
| State transitions | All valid state changes exercised |

### Coverage Analysis

When analyzing coverage reports:

1. **Uncovered lines**: Identify what's missing
2. **Partially covered**: Branches not fully exercised
3. **Covered but untested**: Lines hit without meaningful assertions

### Prioritization

Focus coverage efforts on:

1. Public API surface
2. Error handling paths
3. Security-sensitive code
4. Complex business logic
5. Recently changed code

Lower priority:
- Simple getters/setters
- Delegation-only code
- Generated code

## Output

Report with:
- Current coverage metrics
- Flaccid test identification
- Uncovered critical paths
- Prioritized recommendations
