# Benchmark

Analyze benchmarks for realism. Identify naive patterns that paint an optimistic picture.

## Philosophy

Benchmarks lie by default. A benchmark showing 1M ops/sec means nothing if:
- Input is pre-allocated outside the loop
- Data is cache-hot from previous iteration
- Work is optimized away by the compiler
- Real-world allocation patterns differ
- Contention isn't simulated

This skill focuses on honest benchmarks.

## Execution

1. Read checklist.md in this skill directory
2. Run existing benchmarks and capture results
3. Audit benchmark design for realism
4. Report findings with recommendations

## Specifications

### Naive Benchmark Patterns

Patterns that produce misleadingly good numbers:

| Pattern | Problem | Fix |
|---------|---------|-----|
| Pre-allocated input | Hides allocation cost | Allocate inside loop or use b.ResetTimer() |
| Cache-hot data | Unrealistic memory access | Use varying input sizes, cold starts |
| Compiler elimination | Dead code removed | Use result (assign to package-level var) |
| Single goroutine | Hides contention | Add b.RunParallel() variant |
| Tiny input | Hides scaling issues | Test across input sizes |
| No allocations check | Hides memory pressure | Always use b.ReportAllocs() |

### Realistic Benchmark Design

Good benchmarks include:

| Element | Purpose |
|---------|---------|
| b.ReportAllocs() | Show allocation impact |
| b.ResetTimer() | Exclude setup from measurement |
| b.StopTimer()/StartTimer() | Exclude per-iteration setup |
| Result sink | Prevent compiler optimization |
| Size variants | Show scaling behavior |
| Parallel variant | Show contention behavior |

### Benchmark Naming

Convention: Benchmark[Function]_[Variant]

Examples:
- BenchmarkParse
- BenchmarkParse_LargeInput
- BenchmarkParse_Parallel
- BenchmarkParse_ColdCache

### Result Interpretation

When reading benchmark output:

| Metric | What it means |
|--------|---------------|
| ns/op | Time per operation |
| B/op | Bytes allocated per operation |
| allocs/op | Number of allocations per operation |

Red flags:
- 0 B/op when allocations expected
- Wildly different parallel vs sequential
- Linear scaling when sub-linear expected

### Comparison Methodology

When comparing benchmarks:
- Use benchstat for statistical comparison
- Minimum 10 runs for significance
- Same hardware, same conditions
- Watch for variance (noisy results)

## Output

Report with:
- Benchmark results with allocations
- Naive pattern identification
- Realism assessment
- Recommended improvements
