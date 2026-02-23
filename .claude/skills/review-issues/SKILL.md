# Issue Review

Find issues that will waste cycles, cause confusion, or block progress.

## Principles

1. **Issues are commitments** — A bad issue wastes everyone's time. Find the bad ones.
2. **Ambiguity kills velocity** — Vague scope causes build churn. Missing criteria make review impossible.
3. **Duplicates waste cycles** — Overlapping work is wasted work.
4. **Labels are signals** — Missing or wrong labels hide the state of work.

## Execution

1. Read `checklist.md` in this skill directory
2. Fetch open issues via `gh issue list`
3. Work through each phase — hunt for issues that will cause problems
4. Compile findings into structured report

## Specifications

### What Makes an Issue Dangerous

**Ambiguous scope** — The issue doesn't say what's in and what's out. Builders will guess, and they'll guess wrong.

**Missing acceptance criteria** — Nobody knows what "done" means. Review becomes an argument.

**Vague objectives** — "Make it better" is not actionable. "Improve performance" without targets is not actionable.

**Duplicate work** — Two issues describing the same problem. Two builders solving the same thing.

**Missing labels** — The workflow can't track what it can't see.

### Required Issue Sections

Every issue MUST have:

| Section | Purpose |
|---------|---------|
| Objective | One clear statement of what needs to be accomplished |
| Context | Why this matters, background for architecture |
| Acceptance Criteria | Specific, verifiable checklist items |
| Scope | What's in scope and what's explicitly out |

### Recommended Actions

For each problematic issue:
- **Rewrite** — Issue has value but structure is broken. Needs revision.
- **Merge** — Issue overlaps with another. Consolidate.
- **Close** — Issue is duplicate, obsolete, or not actionable.
- **Clarify** — Issue needs specific missing information before work can begin.

## Output

### Finding Format

```markdown
## Findings

| ID | Issue | Problem | Severity | Action |
|----|-------|---------|----------|--------|
| ISS-001 | #[number] | [what's wrong] | [critical/high/medium/low] | [rewrite/merge/close/clarify] |

### ISS-001: #[number] — [title]

**Problem:** [What's wrong with this issue]
**Impact:** [What will go wrong if this issue enters the workflow as-is]
**Action:** [Rewrite | Merge with #X | Close | Clarify: specific question]
```
