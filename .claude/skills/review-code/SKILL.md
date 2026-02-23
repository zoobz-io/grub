# Code Review

Find where the code lies — inconsistencies, violations, patterns that will break under maintenance, and structural weakness in the workspace.

## Principles

1. **Code follows the spec — or it doesn't** — Deviations from architectural intent are defects, not style choices.
2. **Inconsistency is a defect factory** — Same problem solved differently in different places means one of them is wrong.
3. **Undocumented APIs are unusable APIs** — If consumers can't understand it from the signature and godoc, it's broken.
4. **The workspace is the foundation** — Module structure, dependency isolation, and build configuration underpin everything.

## Execution

1. Read `checklist.md` in this skill directory
2. Run `golangci-lint run ./...` and `go vet ./...`
3. Work through each phase — hunt for violations, inconsistencies, and maintenance traps
4. For each finding, ask: "What would a maintainer get wrong six months from now?"
5. Compile findings into structured report

## Specifications

### Linter Compliance

The linter is the baseline. If it fails, there's no need to look deeper — the basics aren't handled.

Security linters: gosec, errorlint, noctx, bodyclose, sqlclosecheck
Quality linters: govet, ineffassign, staticcheck, unused, errcheck, errchkjson, wastedassign
Best practices: gocritic, revive, unconvert, dupl (150), goconst (min-len 3, min-occurrences 3), godot, misspell, prealloc, copyloopvar

Every `//nolint` directive is a finding unless it specifies linter names AND has a justification comment.

### Naming Conventions

Names that don't follow Go conventions are defects:
- Package names: lowercase, single word, no underscores
- Types: MixedCaps, interfaces describe behavior
- Functions: `New[Type]`, `With[Option]`, no `Get` prefix on getters
- Receivers: short, consistent per type, not `this`/`self`
- Errors: `Err[Condition]`

### Godoc Coverage

Undocumented exported symbols are unusable:
- Package comment on one file per package
- Every exported type, function, method, constant, variable has a godoc
- Comments start with the symbol name, are complete sentences

### Error Handling

Error handling that hides failures:
- Unchecked error returns (bare `_`)
- Type assertions without two-value form
- Missing wrapping context (`fmt.Errorf("context: %w", err)`)
- Error chain breakage (`errors.New` that discards original)

### Context Usage

Context violations:
- I/O functions missing `context.Context` as first parameter
- Context stored in structs
- `context.TODO()` in production code

### Pattern Consistency

Inconsistencies that will confuse maintainers:
- Mixed constructor styles within the package
- Inconsistent error handling approaches
- Different interface satisfaction patterns
- Arbitrary file boundaries

### Workspace Structure

Foundation problems in module organization:
- `go.mod` with unnecessary dependencies
- Provider-specific code not isolated in submodules
- Missing or incorrect build tags
- `go mod tidy` produces changes (untidy module)

## Output

### Finding Format

```markdown
## Findings

| ID | Category | Severity | Location | Description |
|----|----------|----------|----------|-------------|
| COD-001 | [linter/naming/godoc/error/context/pattern/workspace] | [critical/high/medium/low] | [file:line] | [what's wrong] |

### COD-001: [Title]

**Category:** [Linter | Naming | Godoc | Error Handling | Context | Pattern | Workspace]
**Severity:** [Critical | High | Medium | Low]
**Location:** [file:line range]
**Description:** [What the defect is]
**Impact:** [What goes wrong if this isn't fixed]
**Evidence:** [Code snippet or linter output]
**Recommendation:** [How to fix it]
```
