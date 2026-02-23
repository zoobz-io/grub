# Coverage Checklist

## Phase 1: Generate Report

- [ ] Run coverage: go test -coverprofile=coverage.out -covermode=atomic ./...
- [ ] Generate HTML report: go tool cover -html=coverage.out -o coverage.html
- [ ] Get summary: go tool cover -func=coverage.out

## Phase 2: Identify Gaps

### Uncovered Code
- [ ] List functions with 0% coverage
- [ ] List files with less than 50% coverage
- [ ] Identify public API methods without tests

### Partially Covered
- [ ] Find functions with coverage between 1-70%
- [ ] Identify which branches are missed
- [ ] Check if error returns are covered

## Phase 3: Audit Test Quality

### Flaccid Test Detection

For each test file, check for:

#### No Assertions
- [ ] Tests that call functions but never assert
- [ ] Tests that only check err == nil
- [ ] Tests with no failure conditions

Example of flaccid test:
```go
func TestProcess(t *testing.T) {
    Process(input) // Called but result ignored
}
```

#### Weak Assertions
- [ ] Assertions that only check length, not content
- [ ] Assertions that check type, not value
- [ ] Assertions using reflect.DeepEqual on large structures without isolation

Example of weak assertion:
```go
if len(result) > 0 { // Weak: doesn't verify content
    t.Log("got results")
}
```

#### Happy Path Only
- [ ] No tests for nil input
- [ ] No tests for empty input
- [ ] No tests for invalid input
- [ ] No tests that expect errors

#### Tautological Tests
- [ ] Tests that assert what was just assigned
- [ ] Tests that mock return values then assert those values

Example of tautology:
```go
mock.Return(expected)
result := sut.Call()
assert.Equal(expected, result) // Just testing the mock
```

### Error Path Coverage
- [ ] Every function that returns error has test triggering each error
- [ ] Error wrapping is tested (correct context added)
- [ ] Error types are verified, not just err != nil

### Boundary Testing
- [ ] Zero values tested
- [ ] Empty collections tested
- [ ] Single element tested
- [ ] Maximum/overflow tested
- [ ] Nil pointers tested

### Concurrency Testing
- [ ] Race detector enabled: go test -race
- [ ] Concurrent access patterns tested
- [ ] Goroutine leaks checked

## Phase 4: Critical Path Analysis

### Public API
- [ ] All exported functions have tests
- [ ] All exported methods have tests
- [ ] All exported types have construction tests

### Error Handling
- [ ] All error returns are reachable via tests
- [ ] Error messages are meaningful
- [ ] Errors are wrapped with context

### Security-Sensitive
- [ ] Input validation tested
- [ ] Authentication paths tested
- [ ] Authorization checks tested
- [ ] Sanitization tested

## Phase 5: Report

### Metrics Summary
- [ ] Overall coverage percentage
- [ ] Per-package coverage
- [ ] Coverage delta from last run (if available)

### Flaccid Tests Found
- [ ] List tests with no assertions
- [ ] List tests with weak assertions
- [ ] List tests covering only happy path

### Critical Gaps
- [ ] Untested public API
- [ ] Untested error paths
- [ ] Untested security code

### Recommendations
Prioritize by:
1. Security-sensitive uncovered code
2. Public API gaps
3. Error handling gaps
4. Flaccid test fixes
5. Edge case additions

## Phase 6: Quick Wins

Identify low-effort improvements:
- [ ] Add error case to existing test
- [ ] Add nil check to existing test
- [ ] Add assertion to assertion-free test
- [ ] Add boundary value to table test
