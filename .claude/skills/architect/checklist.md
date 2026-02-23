# Architect Checklist

## Phase 1: Understand the Issue

- [ ] Read issue objective
- [ ] Read acceptance criteria
- [ ] Understand scope boundaries
- [ ] Note any constraints in Notes section

## Phase 2: Ecosystem Context

- [ ] Run ecosystem skill
- [ ] Understand how this package is consumed
- [ ] Identify downstream impact of changes
- [ ] Note any ecosystem constraints

## Phase 3: Examine Existing Architecture

### Current Structure
- [ ] Review directory structure
- [ ] Identify relevant existing files
- [ ] Understand current patterns

### Patterns in Use
- [ ] How are similar features implemented?
- [ ] What conventions exist?
- [ ] What would consistency require?

### Dependencies
- [ ] What does this package depend on?
- [ ] Are new dependencies needed?
- [ ] Are there version constraints?

## Phase 4: Design Solution

### Approach
- [ ] Define overall approach
- [ ] Break into components/steps
- [ ] Identify affected files

### Fit Check
- [ ] Does this fit existing patterns?
- [ ] Does this require new patterns?
- [ ] Is this the simplest approach?

### Risk Assessment
- [ ] What could go wrong?
- [ ] Are there edge cases?
- [ ] What's the rollback story?

## Phase 5: Consider Testing

- [ ] What tests will Kevin need to write?
- [ ] Are there existing test patterns to follow?
- [ ] What coverage is expected?

## Phase 6: Produce Output

### If Feasible
- [ ] Write architecture plan
- [ ] Include all required sections
- [ ] Post as comment: gh issue comment [number] --body "[plan]"
- [ ] Indicate ready for implementation

### If Concerns
- [ ] Document specific concerns
- [ ] List options with tradeoffs
- [ ] Provide recommendation
- [ ] Post as comment
- [ ] Indicate clarification needed

## Architecture Plan Template

```markdown
## Architecture Plan

### Summary
[One paragraph approach description]

### Affected Areas
- [file]: [change description]

### Approach

#### [Component]
[Implementation details]

### Patterns to Follow
- [Pattern to match]

### Dependencies
- [Dependency if any]

### Test Considerations
[Testing guidance]

### Risks
- [Risk to watch]

---
Ready for implementation. @midgel
```
