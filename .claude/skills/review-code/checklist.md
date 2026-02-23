# Code Review Checklist

## Phase 1: Linter Compliance — Does the Baseline Pass?

### Run Linter
- [ ] Run `golangci-lint run ./...`
- [ ] Record total findings by linter
- [ ] Note any disabled linters or nolint directives — each is suspect

### Security Linters
- [ ] gosec: security findings?
- [ ] errorlint: error comparisons bypassing `errors.Is`/`errors.As`?
- [ ] noctx: HTTP requests without context?
- [ ] bodyclose: response bodies left open?
- [ ] sqlclosecheck: SQL rows/statements left open?

### Quality Linters
- [ ] govet: vet findings?
- [ ] ineffassign: ineffectual assignments?
- [ ] staticcheck: static analysis findings?
- [ ] unused: dead code?
- [ ] errcheck: unchecked errors?
- [ ] errchkjson: unchecked JSON marshal/unmarshal?
- [ ] wastedassign: wasted assignments?

### Best Practice Linters
- [ ] gocritic: diagnostic findings?
- [ ] revive: style violations?
- [ ] unconvert: unnecessary conversions?
- [ ] dupl: duplicated code blocks above 150 tokens?
- [ ] goconst: repeated strings that should be constants?
- [ ] godot: comments missing terminal periods?
- [ ] misspell: misspellings?
- [ ] prealloc: slices not preallocated where size is known?
- [ ] copyloopvar: loop variable capture issues?

### Nolint Directives — Each Is Suspect
- [ ] List all `//nolint` directives
- [ ] Does each specify which linter(s) it suppresses — or is it a blanket escape?
- [ ] Does each have a justification comment — or is it silent?
- [ ] Is the justification valid — or is it suppressing a real finding?

## Phase 2: Naming Conventions — Where Do Names Lie?

### Package Names
- [ ] Lowercase, single word where possible — or violating?
- [ ] No underscores or mixed caps
- [ ] Name reflects purpose — not util, common, misc

### Type Names
- [ ] MixedCaps convention followed
- [ ] Interface names describe behavior — not `I[Name]` prefix
- [ ] Struct names describe what they are

### Function/Method Names
- [ ] Constructors follow `New[Type]` — or inventing patterns?
- [ ] Functional options follow `With[Option]` — or inconsistent?
- [ ] Getters avoid `Get` prefix — or ignoring Go convention?

### Receiver Names
- [ ] Short (1-2 chars), consistent per type — or `this`/`self`?
- [ ] Same receiver name across all methods of a type — or drifting?

### Variable Names
- [ ] Error variables follow `Err[Condition]`
- [ ] Short names for short scopes, descriptive for wide — or inverted?

## Phase 3: Godoc Assessment — Where Is the API Undocumented?

### Package Documentation
- [ ] Does each package have a package comment — or is it silent?
- [ ] Is the comment on a single file?
- [ ] Does it describe what the package does?

### Exported Symbols
For each exported type, function, method, constant, variable:
- [ ] Has a godoc comment — or is it exposed without explanation?
- [ ] Comment starts with the symbol name
- [ ] Comment is a complete sentence ending with period
- [ ] Comment describes what it does, not how

## Phase 4: Error Handling — Where Do Failures Hide?

### Error Checking
- [ ] All error returns checked — any bare `_` for errors?
- [ ] Type assertions use two-value form — or will panic?
- [ ] Channel operations handle closed channels?

### Error Wrapping
- [ ] Errors wrapped with `fmt.Errorf("context: %w", err)` — or lost?
- [ ] Error messages describe the operation that failed — or generic?
- [ ] Error chain preserved — or broken by `errors.New`?

### Sentinel Errors
- [ ] Expected failures have sentinel errors — or ad hoc strings?
- [ ] Sentinel errors are package-level vars
- [ ] Implementations return consistent sentinels

## Phase 5: Context Usage — Where Is Control Abandoned?

### I/O Functions
- [ ] All I/O functions accept `context.Context` as first parameter?
- [ ] Context threaded through call chains — or created mid-chain?
- [ ] Any `context.TODO()` in production code? (unfinished work)

### Context Storage
- [ ] Context stored in struct fields? (violation)
- [ ] Context passed as parameter? (correct)

### Cancellation
- [ ] Long-running operations check `ctx.Done()`?
- [ ] Cancellation propagates to child operations?

## Phase 6: Pattern Consistency — Where Does the Code Contradict Itself?

### Constructor Patterns
- [ ] All constructors follow the same pattern — or mixed?
- [ ] Functional options vs config struct vs direct params — pick one

### Error Patterns
- [ ] Same error handling approach throughout — or ad hoc?
- [ ] Error wrapping format consistent — or varying?

### Interface Patterns
- [ ] Interface satisfaction consistent across types?
- [ ] Mock patterns consistent?

### Code Organization
- [ ] Similar types organized similarly across files?
- [ ] Related functionality colocated — or scattered?

## Phase 7: Dependency Usage — Where Is Trust Misplaced?

### Standard Library
- [ ] stdlib used where sufficient — or external for trivial ops?

### Reflect Usage
- [ ] `reflect` used only where necessary — or casual?
- [ ] Each usage justified?

### Unsafe Usage
- [ ] `unsafe` absent — or present without heavy justification?

### Init Functions
- [ ] No `init()` functions — or present without clear necessity?

## Phase 8: Workspace Structure — Is the Foundation Sound?

### Compilation
- [ ] `go build ./...` succeeds
- [ ] `go vet ./...` clean
- [ ] No build warnings

### Module Tidiness
- [ ] `go mod tidy` produces no changes — or is the module untidy?
- [ ] No unused dependencies in go.mod
- [ ] No missing dependencies

### Module Organization
- [ ] Provider-specific code isolated in submodules — or leaking?
- [ ] Build tags correct and consistent
- [ ] CI/release configuration references correct module paths
