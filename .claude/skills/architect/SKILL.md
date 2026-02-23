# Architect

Design a solution within existing architecture and post the plan to the GitHub issue.

## Purpose

Transform an issue into an architectural plan that:
- Fits within existing patterns
- Identifies affected areas
- Provides clear implementation guidance
- Posts to the issue as a comment for visibility

## Execution

1. Read the issue thoroughly
2. Run ecosystem skill to understand downstream impact
3. Examine existing architecture and patterns
4. Design solution within established conventions
5. Post architecture plan as issue comment
6. OR flag concerns if design isn't feasible

## Architecture Plan Structure

```markdown
## Architecture Plan

### Summary
[One paragraph describing the approach]

### Affected Areas
- [file/area]: [what changes]
- [file/area]: [what changes]

### Approach

#### [Component 1]
[How this will be implemented]

#### [Component 2]
[How this will be implemented]

### Patterns to Follow
- [Existing pattern to match]
- [Convention to follow]

### Dependencies
- [Required dependency if any]

### Test Considerations
[Guidance for testing]

### Risks
- [Potential issue to watch]

---
Ready for implementation.
```

## Posting to Issue

```bash
gh issue comment [number] --body "[architecture plan]"
```

## If Concerns Exist

```markdown
## Architecture Concerns

### Issue
[What prevents straightforward implementation]

### Options
1. [Option A]: [tradeoffs]
2. [Option B]: [tradeoffs]

### Recommendation
[Preferred path forward]

### Questions
- [What needs clarification]

---
Requires clarification before proceeding.
```

## Language Rules

The architecture plan is posted as a public issue comment. It MUST NOT reference agent names, crew roles, or internal workflow structure. Review the comment-issue skill for the full language standard.

## What This Skill Does NOT Do

- Validate the plan against product requirements (separate concern)
- Implement the solution (separate concern)
- Test the implementation (separate concern)

This produces the technical design. Implementation follows.
