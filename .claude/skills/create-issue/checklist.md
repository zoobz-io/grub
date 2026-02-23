# Create Issue Checklist

## Phase 1: Pre-flight

- [ ] Plan has been validated (validate-plan skill run)
- [ ] No blocking gaps identified
- [ ] Scope is clear and appropriate

## Phase 2: Structure the Issue

### Title
- [ ] Format: [type]: [brief description]
- [ ] Type is one of: feature, fix, docs, infra, refactor
- [ ] Description is concise (<60 chars)

### Objective
- [ ] Single clear statement
- [ ] States what, not how
- [ ] Unambiguous

### Context
- [ ] Why this matters
- [ ] Background Fidgel needs
- [ ] Related issues or prior art if relevant

### Acceptance Criteria
- [ ] Each criterion is specific
- [ ] Each criterion is verifiable
- [ ] Criteria are checkboxes
- [ ] 3-7 criteria typical

### Scope
- [ ] In Scope clearly listed
- [ ] Out of Scope explicitly stated
- [ ] Boundaries are clear

### Notes (optional)
- [ ] Constraints mentioned
- [ ] Dependencies noted
- [ ] Open questions flagged

## Phase 3: Create Issue

- [ ] Compose issue body using template
- [ ] Run: gh issue create --title "[title]" --body "[body]"
- [ ] Apply label: needs-architecture
- [ ] Capture issue number and URL

## Phase 4: Confirm

- [ ] Issue created successfully
- [ ] Labels applied
- [ ] URL accessible
- [ ] Report issue details

## Issue Template

```markdown
## Objective

[One clear statement of what needs to be accomplished]

## Context

[Background and rationale for this work]

## Acceptance Criteria

- [ ] [Criterion 1]
- [ ] [Criterion 2]
- [ ] [Criterion 3]

## Scope

### In Scope
- [Included item]

### Out of Scope
- [Excluded item]

## Notes

[Additional context if needed]
```
