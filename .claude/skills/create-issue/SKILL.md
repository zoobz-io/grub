# Create Issue

Create a well-formed GitHub issue that triggers the development workflow.

## Purpose

Transform a validated plan into a GitHub issue that:
- Clearly states the objective
- Provides acceptance criteria
- Gives enough context for architecture
- Can be tracked through completion

## Execution

1. Ensure plan has been validated (validate-plan skill)
2. Structure the issue with required sections
3. Create issue via gh CLI
4. Apply appropriate labels
5. Confirm issue created

## Issue Structure

```markdown
## Objective

[What needs to be accomplished. One clear statement.]

## Context

[Why this matters. Background information needed for architecture.]

## Acceptance Criteria

- [ ] [Specific, verifiable criterion]
- [ ] [Specific, verifiable criterion]
- [ ] [Specific, verifiable criterion]

## Scope

### In Scope
- [What this issue covers]

### Out of Scope
- [What this issue explicitly does NOT cover]

## Notes

[Any additional context, constraints, or considerations]
```

## Labels

Apply appropriate labels:
- `feature` / `bug` / `docs` / `infra` — type
- `phase:plan` — enters the planning workflow

## Command

```bash
gh issue create \
  --title "[type]: [brief description]" \
  --body "[structured body]" \
  --label "phase:plan"
```

## Output

```
## Issue Created

**Issue:** #[number]
**Title:** [title]
**URL:** [url]

Assigned label: phase:plan
Ready for architecture.
```

## Language Rules

The issue body is public documentation posted under the ROCKHOPPER identity. It MUST NOT reference agent names, crew roles, protocol names, or internal workflow structure. Review the comment-issue skill for the full prohibited terms list.

## What This Skill Does NOT Do

- Validate the plan (use validate-plan first)
- Architect the solution (separate concern)
- Assign to implementers (workflow handles this)

This creates the issue. The workflow takes over from here.
