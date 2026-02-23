# Architecture Review

Find where the architecture exposes unnecessary surface, violates its own principles, or creates structural weakness.

## Principles

1. **Interfaces are attack surface** — Every method on a public interface is a commitment. Wide interfaces are liabilities.
2. **Missing boundaries are open doors** — Data that crosses a boundary without transformation is data that crosses unchecked.
3. **Dependencies are trust decisions** — Every dependency is code you didn't write running in your process.
4. **Assumptions break** — Every architectural assumption is a candidate for failure. Find the ones that will hurt.

## Execution

1. Read `checklist.md` in this skill directory
2. Read `MISSION.md` — understand the package's stated purpose and goals
3. Read `PHILOSOPHY.md` — understand the composition model and architectural principles to evaluate against
4. Work through each phase — hunt for structural weakness
5. For each finding, ask: "What would break if this assumption were wrong?"
6. Compile findings into structured report

## Specifications

### Interface Design

Interfaces that are too wide, too abstract, or too coupled are findings:
- More methods than consumers need → unnecessary commitment surface
- Methods from multiple abstraction levels → confused responsibilities
- Requiring concrete type knowledge → leaky abstraction
- Missing `context.Context` on I/O → uncontrollable operations

### Composition Model

Violations of processor/connector/value separation:
- Stateful processors → hidden side effects
- Stateless connectors → lifecycle confusion
- Hybrid types → untestable, unpredictable
- Outward dependency flow (processors depending on connectors) → architectural inversion

### Boundary Design

Missing or implicit boundaries:
- No identifiable transformation at ingress → unchecked input
- No identifiable transformation at egress → leaked internals
- Business logic performing serialization → boundary confusion
- Implicit conversions outside boundaries → hidden data transformation

### Dependency Policy

Dependencies that violate the minimal-by-default principle:
- External packages for stdlib-available functionality → unnecessary trust
- Provider SDKs in root module → forced transitive dependencies
- Circular dependencies → structural fragility
- Missing zoobzio ecosystem packages where applicable

### Error Design

Error patterns that hide failures or lose context:
- Missing error wrapping → lost failure chain
- Inconsistent sentinel errors → unpredictable consumer behavior
- Error messages without actionable context → debugging in the dark

### Type Safety

Type system violations:
- `interface{}` or `any` in public API where generics work → compile-time safety abandoned
- Type assertions without checks → runtime panic candidates
- Missing compile-time interface satisfaction → silent contract violations

## Output

### Finding Format

```markdown
## Findings

| ID | Category | Severity | Location | Description |
|----|----------|----------|----------|-------------|
| ARC-001 | [interface/composition/boundary/dependency/error/type] | [critical/high/medium/low] | [file:line] | [what's wrong] |

### ARC-001: [Title]

**Category:** [Interface Design | Composition | Boundary | Dependency | Error Design | Type Safety]
**Severity:** [Critical | High | Medium | Low]
**Location:** [file:line range]
**Description:** [What the structural weakness is]
**Impact:** [What breaks when this assumption fails]
**Evidence:** [Code snippet or structural observation]
**Recommendation:** [How to fix it]
```
