# Mission Review

Find where implementation contradicts, drifts from, or fails to deliver on its stated mission.

## Principles

1. **Mission defines the contract** — Everything in the package MUST serve the mission. Anything else is drift.
2. **Scope is a boundary** — Non-goals are fences. If the fence is broken, something got through.
3. **Success criteria are promises** — If a promise can't be kept, it's a defect.
4. **Drift compounds** — Small deviations become large problems. Find them early.

## Execution

1. Read `checklist.md` in this skill directory
2. Read MISSION.md for the package's stated purpose
3. Read PHILOSOPHY.md for zoobzio-wide principles
4. Work through each phase — hunt for contradictions, gaps, and lies
5. Compile findings into structured report

## Specifications

### Drift Categories

**Mission-aligned** — Directly serves the stated purpose. No finding.

**Mission-adjacent** — Related but not explicitly covered. The mission is incomplete or the feature is unauthorized. Either way, it's a finding.

**Mission-contradictory** — Directly violates a non-goal or exclusion. This is a defect.

**Mission-orphaned** — Serves no apparent purpose. Dead weight.

### Success Criteria Verification

Each criterion is a promise. For each:
- Can a user actually do this with the current code?
- Where does the walk-through fail or get stuck?
- What's missing that blocks the promise?

### Philosophy Alignment

The package MUST align with PHILOSOPHY.md. Deviations are findings:
- Dependency policy violations (unnecessary production deps, non-isolated providers)
- Type safety violations (interface{} where generics work)
- Missing boundaries (implicit data transformation)
- Composition violations (wide interfaces, mixed stateful/stateless)
- Error design violations (missing context, inconsistent semantics)
- Context violations (missing context.Context on I/O)

## Output

### Finding Format

```markdown
## Findings

| ID | Category | Severity | Description |
|----|----------|----------|-------------|
| MSN-001 | [drift/gap/violation/orphan] | [critical/high/medium/low] | [what's wrong] |

### MSN-001: [Title]

**Category:** [Mission drift | Mission gap | Mission violation | Mission orphan]
**Severity:** [Critical | High | Medium | Low]
**Evidence:** [What the code does vs what the mission says]
**Recommendation:** [Fix the code, update the mission, or remove the feature]
```
