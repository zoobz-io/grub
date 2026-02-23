# Documentation Review

Find where documentation misleads, lies, or fails the user.

## Principles

1. **Documentation that doesn't match reality is a defect** — Stale docs are worse than no docs. They actively mislead.
2. **Missing docs are invisible features** — If it's not documented, users don't know it exists.
3. **Structure serves navigation** — Bad structure means users can't find what they need.
4. **Examples must work** — A broken example teaches the wrong thing.

## Execution

1. Read `checklist.md` in this skill directory
2. Review README.md and docs/ directory
3. Work through each phase — hunt for lies, gaps, and misleading content
4. For each finding, ask: "What would mislead a new user?"
5. Compile findings into structured report

## Specifications

### README Assessment

The README is the first thing users see. It must not:
- Describe features that don't exist
- Omit features that do exist
- Show examples that don't compile
- Have broken links or badge URLs
- Reference internal structure that users don't need

### Documentation Directory

If `docs/` exists, it must:
- Follow the learn/guides/reference structure
- Have correct frontmatter on all pages
- Have working cross-references
- Not duplicate README content unnecessarily
- Not describe aspirational features as current

### Content Accuracy

For every claim in documentation:
- Does the code actually do this?
- Does the API actually look like this?
- Does the example actually work?
- Is the version/compatibility info current?

### Content Completeness

For every public API surface:
- Is it documented somewhere accessible?
- Can a user find it without reading source code?
- Is the "happy path" documented?
- Are common pitfalls documented?

## Output

### Finding Format

```markdown
## Findings

| ID | Category | Severity | Location | Description |
|----|----------|----------|----------|-------------|
| DOC-001 | [accuracy/completeness/structure/example] | [critical/high/medium/low] | [file:section] | [what's wrong] |

### DOC-001: [Title]

**Category:** [Accuracy | Completeness | Structure | Example]
**Severity:** [Critical | High | Medium | Low]
**Location:** [file and section]
**Description:** [What the documentation gets wrong]
**Impact:** [What a user would misunderstand or fail to do]
**Evidence:** [The claim vs the reality]
**Recommendation:** [How to fix it]
```
