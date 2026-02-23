# Workspace Create

Set up Go workspace structure for packages with providers or other multi-module needs, ensuring proper dependency isolation.

## When to Use

Use workspace structure when a package:
- Supports multiple backends/providers (databases, message queues, cloud services)
- Has heavy provider-specific dependencies that shouldn't pollute the core
- Wants consumers to import only what they need

## Principles

1. **Core stays lightweight** — Root module MUST have minimal dependencies
2. **Providers are independent** — Each provider MUST be its own module
3. **Consumers pay for what they use** — Importing one provider MUST NOT bring others' deps
4. **Local development is seamless** — `go.work` MUST unify development experience

## Execution

1. Read `checklist.md` in this skill directory
2. Ask user about providers needed and their dependencies
3. Create files per specifications
4. Verify with `go work sync` and `go build ./...`

## Specifications

### Directory Structure

```
/
├── go.work                    # Workspace definition
├── go.mod                     # Root module (lightweight)
├── go.sum
├── api.go                     # Core interfaces
├── [core].go                  # Core implementation
├── testing/                   # Test utilities (NOT a module)
│   ├── helpers.go
│   ├── helpers_test.go
│   ├── integration/
│   └── benchmarks/
├── [provider-a]/              # Provider module
│   ├── go.mod                 # Own dependencies + replace directive
│   ├── go.sum
│   ├── [provider].go
│   └── [provider]_test.go
└── [provider-b]/
    ├── go.mod
    ├── go.sum
    ├── [provider].go
    └── [provider]_test.go
```

### go.work Format

```go
go 1.24

toolchain go1.25.x

use (
    .
    ./[provider-a]
    ./[provider-b]
)
```

### Provider go.mod Format

```go
module github.com/zoobzio/[package]/[provider]

go 1.24

toolchain go1.25.x

require (
    // Provider-specific SDK
)

replace github.com/zoobzio/[package] => ../
```

The replace directive is REQUIRED for local development.

### Testing Package

`testing/` MUST:
- NOT be a separate module (no `testing/go.mod`)
- NOT import provider-specific dependencies
- Provide mocks/utilities for core interfaces

### Release Workflow

See checklist for exact release.yml content with submodule tagging.

**Critical:** The release workflow MUST update each submodule's go.mod to reference the released root version before tagging. Replace directives are ignored when modules are imported as dependencies.

## Prohibitions

DO NOT:
- Create `testing/go.mod` (testing is part of root module)
- Put provider-specific SDKs in root go.mod
- Skip replace directives in provider modules
- Create providers without corresponding test files

## Output

A workspace structure that:
- Unifies local development via `go.work`
- Isolates provider dependencies to their modules
- Keeps root package lightweight
- Provides clean consumer import paths
