# Benchmarks

Performance benchmarks for grub core operations.

## Running Benchmarks

```bash
# Run all benchmarks
make test-bench

# Run with memory allocation stats
go test -bench=. -benchmem ./testing/benchmarks/...

# Run specific benchmark
go test -bench=BenchmarkStore_Set -benchmem ./testing/benchmarks/...

# Run with CPU profiling
go test -bench=. -cpuprofile=cpu.prof ./testing/benchmarks/...

# Run with memory profiling
go test -bench=. -memprofile=mem.prof ./testing/benchmarks/...
```

## Available Benchmarks

### Store Operations

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkStore_Set` | Single Set operation |
| `BenchmarkStore_Get` | Single Get operation |
| `BenchmarkStore_SetGet` | Set followed by Get |
| `BenchmarkStore_Exists` | Exists check |

## Understanding Results

```
BenchmarkStore_Set-8     1000000     1050 ns/op     256 B/op     4 allocs/op
```

- **-8**: GOMAXPROCS (number of CPUs)
- **1000000**: iterations run
- **1050 ns/op**: nanoseconds per operation
- **256 B/op**: bytes allocated per operation
- **4 allocs/op**: allocations per operation

## Mock Provider

Benchmarks use an in-memory mock provider to isolate grub overhead from provider I/O. This measures:

- Codec serialization/deserialization
- Type-safe wrapper overhead

To benchmark actual provider performance, run integration tests with timing or create provider-specific benchmarks.

## Comparing Changes

```bash
# Install benchstat
go install golang.org/x/perf/cmd/benchstat@latest

# Run baseline
go test -bench=. -count=10 ./testing/benchmarks/... > old.txt

# Make changes, then run again
go test -bench=. -count=10 ./testing/benchmarks/... > new.txt

# Compare
benchstat old.txt new.txt
```

## CI Integration

Benchmarks run as part of CI to detect performance regressions. Results are not published but serve as a smoke test for significant degradation.
