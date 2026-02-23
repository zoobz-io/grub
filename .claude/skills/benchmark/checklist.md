# Benchmark Checklist

## Phase 1: Run Benchmarks

- [ ] Run all benchmarks: go test -bench=. -benchmem -count=10 ./testing/benchmarks/...
- [ ] Capture output to file for analysis
- [ ] Note any benchmarks that skip or fail

## Phase 2: Audit Benchmark Design

### For Each Benchmark

#### Setup Issues
- [ ] Is input allocated inside or outside the loop?
- [ ] Is b.ResetTimer() called after expensive setup?
- [ ] Is b.StopTimer()/b.StartTimer() used for per-iteration setup?

Example of hidden allocation cost:
```go
func BenchmarkProcess(b *testing.B) {
    input := makeHugeInput() // Allocated once, reused
    for i := 0; i < b.N; i++ {
        Process(input)
    }
}
```

Better:
```go
func BenchmarkProcess(b *testing.B) {
    for i := 0; i < b.N; i++ {
        input := makeHugeInput() // Shows true cost
        Process(input)
    }
}
```

Or if setup is legitimately separate:
```go
func BenchmarkProcess(b *testing.B) {
    input := makeHugeInput()
    b.ResetTimer() // Exclude setup
    for i := 0; i < b.N; i++ {
        Process(input)
    }
}
```

#### Compiler Elimination
- [ ] Is the result used?
- [ ] Could the work be optimized away?

Example of dead code:
```go
func BenchmarkCompute(b *testing.B) {
    for i := 0; i < b.N; i++ {
        Compute(42) // Result discarded, may be eliminated
    }
}
```

Fix with result sink:
```go
var result int // Package-level sink

func BenchmarkCompute(b *testing.B) {
    var r int
    for i := 0; i < b.N; i++ {
        r = Compute(42)
    }
    result = r // Prevent elimination
}
```

#### Allocation Reporting
- [ ] Does benchmark call b.ReportAllocs()?
- [ ] Are allocation numbers realistic for the operation?
- [ ] Is 0 allocs suspicious for this operation?

#### Input Realism
- [ ] Is input size representative of real usage?
- [ ] Are there size variants (small, medium, large)?
- [ ] Is input variety sufficient (not same data repeated)?

#### Concurrency
- [ ] Is there a parallel variant using b.RunParallel()?
- [ ] Does parallel variant show realistic contention?
- [ ] Are locks/channels exercised under load?

Example parallel benchmark:
```go
func BenchmarkProcess_Parallel(b *testing.B) {
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            Process(input)
        }
    })
}
```

### Benchmark Coverage

- [ ] Core operations benchmarked
- [ ] Hot paths benchmarked
- [ ] Allocation-heavy operations benchmarked
- [ ] Concurrent operations benchmarked

## Phase 3: Analyze Results

### Performance Metrics

For each benchmark, record:
- [ ] ns/op (time)
- [ ] B/op (bytes allocated)
- [ ] allocs/op (allocation count)

### Red Flags

- [ ] 0 B/op when operation clearly allocates
- [ ] Suspiciously fast (compiler eliminated work?)
- [ ] High variance between runs (noisy measurement)
- [ ] Parallel much slower than sequential (contention issue)
- [ ] Linear scaling when algorithm should be sub-linear

### Comparison (if baseline exists)

- [ ] Run benchstat for statistical comparison
- [ ] Identify significant regressions
- [ ] Identify significant improvements
- [ ] Note any that need investigation

## Phase 4: Memory Analysis

### Allocation Patterns

- [ ] Which operations allocate most?
- [ ] Are allocations per-call or amortized?
- [ ] Could allocations be pooled?
- [ ] Are there unexpected allocations?

### Profiling (if needed)

- [ ] Run with -memprofile: go test -bench=X -memprofile=mem.out
- [ ] Analyze: go tool pprof mem.out
- [ ] Identify allocation hotspots

## Phase 5: Report

### Results Summary

Table format:
| Benchmark | ns/op | B/op | allocs/op | Assessment |
|-----------|-------|------|-----------|------------|

### Naive Patterns Found

List benchmarks with issues:
- [ ] Pre-allocated input hiding costs
- [ ] Missing b.ReportAllocs()
- [ ] Dead code / compiler elimination risk
- [ ] Missing parallel variants
- [ ] Unrealistic input sizes

### Missing Benchmarks

Operations that should be benchmarked:
- [ ] Core public API
- [ ] Known hot paths
- [ ] Memory-intensive operations

### Recommendations

Prioritize by:
1. Benchmarks hiding allocation costs
2. Benchmarks at risk of compiler elimination
3. Missing parallel variants for concurrent code
4. Missing size variants for scaling analysis
5. Missing benchmarks for hot paths

## Phase 6: Quick Wins

- [ ] Add b.ReportAllocs() to all benchmarks
- [ ] Add result sink to prevent elimination
- [ ] Add parallel variant to sequential benchmark
- [ ] Add size variant to single-size benchmark
