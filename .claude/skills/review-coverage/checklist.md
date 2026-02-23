# Coverage Review Checklist

## Phase 1: Coverage Baseline — What Does the Number Say?

### Generate Coverage
- [ ] Run `go test -coverprofile=coverage.out ./...`
- [ ] Record total coverage percentage
- [ ] Does it meet the 70% project floor?
- [ ] Generate HTML report: `go tool cover -html=coverage.out`

### Per-File Coverage
- [ ] List coverage per source file
- [ ] Identify files below 50% — these are the weakest points
- [ ] Identify files at 0% — completely undefended

## Phase 2: Uncovered Lines — What's Undefended?

### Critical Uncovered Paths
- [ ] Public API functions without coverage — consumers can't trust these
- [ ] Error handling code without coverage — failures are untested
- [ ] Security-sensitive code without coverage — vulnerabilities are invisible
- [ ] Complex conditionals without full branch coverage — some paths never execute

### Partially Covered Functions
- [ ] Functions where only the happy path is covered
- [ ] Functions where error returns are never triggered
- [ ] Functions where some branches are hit but others are not

## Phase 3: Flaccid Test Detection — Which Tests Lie?

### No-Assertion Tests
For each test function:
- [ ] Does the test have assertions — or just call the function?
- [ ] Does the test check return values — or ignore them?
- [ ] Does the test verify side effects — or assume they happened?

### Happy-Path-Only Tests
For each tested function with error returns:
- [ ] Is there a test that triggers each error path?
- [ ] Is there a test with invalid input?
- [ ] Is there a test with nil/empty/zero input?

### Weak Assertion Tests
- [ ] Tests that only check `err == nil` without checking the returned value
- [ ] Tests that check `!= nil` without checking what the value actually is
- [ ] Tests that check string contains without checking the full behavior

### Tautological Tests
- [ ] Tests that set a value then immediately assert it
- [ ] Tests that assert against the same mock they configured
- [ ] Tests that test the test framework more than the code

### Mock-Heavy Tests
- [ ] Tests where every dependency is mocked
- [ ] Tests that only verify mock interactions (method called with args)
- [ ] Tests that would pass even if the implementation were empty

## Phase 4: Quality Assessment — Is Coverage Real?

### Public API Coverage Quality
For each public function/method:
- [ ] Is it covered? (if no, critical finding)
- [ ] Is the coverage meaningful? (assertions check behavior, not just existence)
- [ ] Are edge cases covered? (boundaries, nil, error)

### Error Path Coverage Quality
For each error-returning function:
- [ ] Is each error return triggered by at least one test?
- [ ] Does the test verify the correct error is returned?
- [ ] Does the test verify error wrapping/context?

### Concurrent Coverage
- [ ] Tests run with `-race` flag?
- [ ] Concurrent code paths exercised under test?
- [ ] Any race conditions detected?

## Phase 5: CI Configuration — Does the Pipeline Catch This?

### Coverage in CI
- [ ] CI runs tests with coverage flag — or without?
- [ ] CI uploads coverage reports — or discards them?
- [ ] Coverage thresholds enforced — or advisory only?

### Threshold Enforcement
- [ ] Project floor (70%) would fail the build if violated?
- [ ] Patch floor (80%) would block the PR if violated?
- [ ] Are thresholds actually enforced — or just reported?
