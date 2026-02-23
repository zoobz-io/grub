# Validate Plan

Validate a proposed plan against the existing product before issue creation.

## Purpose

Before creating an issue, ensure the plan:
- Aligns with existing product direction
- Doesn't duplicate existing functionality
- Fits within established patterns
- Has clear, achievable scope

## Execution

1. Read the proposed plan
2. Examine MISSION.md for product purpose
3. Check existing codebase for overlap or conflicts
4. Review open issues for duplicates or related work
5. Assess feasibility within current architecture
6. Approve plan OR flag gaps requiring discussion

## Validation Checklist

### Alignment
- Does this serve the mission stated in MISSION.md?
- Does it fit the product's purpose?
- Is this the right package for this feature?

### Overlap
- Does similar functionality already exist?
- Are there open issues addressing this?
- Would this duplicate or conflict with existing code?

### Scope
- Is the scope clearly defined?
- Are acceptance criteria measurable?
- Can this be implemented incrementally?

### Feasibility
- Does current architecture support this?
- Are dependencies available?
- Are there known blockers?

## Output

### If Valid
```
## Plan Validation: Approved

### Alignment
[How this serves the mission]

### Scope Assessment
[Scope is appropriate / needs refinement]

### Recommendation
Proceed to issue creation.
```

### If Gaps Found
```
## Plan Validation: Gaps Identified

### Concerns
- [Specific concern 1]
- [Specific concern 2]

### Questions
- [Question requiring clarification]

### Recommendation
Address gaps before proceeding.
```

## What This Skill Does NOT Do

- Architect the solution (Fidgel's domain)
- Implement anything (Midgel's domain)
- Test anything (Kevin's domain)

This is PM validation. Product fit, not technical design.
