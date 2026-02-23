# Feature

Prepare a feature branch and plan implementation before writing code.

## Philosophy

Features MUST be understood before they're built. This skill enforces a planning phase that:
- Validates the feature makes sense
- Verifies assumptions about the codebase
- Identifies blockers early
- Structures work into logical commits

## Execution

1. Read `checklist.md` in this skill directory
2. Sync with develop and create feature branch
3. Gather context about the feature request
4. Verify assumptions (DO NOT assume code exists—check)
5. Assess feasibility using skepticism protocol
6. Plan implementation with commit strategy
7. Present plan for approval

## Specifications

### Branch Naming

Format: `feature/[short-description]`

Requirements:
- Derived from feature description
- kebab-case
- 2-4 words
- Lowercase only

Examples:
- `feature/user-auth`
- `feature/redis-caching`
- `feature/api-rate-limiting`

### Skepticism Protocol

Before planning, assess the request against these criteria:

#### Green Light (Proceed)
- Feature aligns with existing architecture
- Clear use case and acceptance criteria
- Dependencies are available
- Implementation path is clean

#### Yellow Light (Ask Questions First)
- Vague requirements ("make it better")
- Missing context ("add caching" — where? why?)
- Scope unclear
- Multiple valid approaches

#### Red Light (Push Back)
- Violates existing patterns without good reason
- Introduces unnecessary complexity
- Solves a problem that doesn't exist
- Would require hacky workarounds
- Better solved by existing tools/libraries

**On Red Light:** Explain concerns clearly. REFUSE to plan unless user explicitly insists after hearing objections. If overridden, document concerns in the plan.

### Commit Strategy

#### Single Commit (small features)
- Self-contained change
- < 200 lines changed
- Single concern

#### Multiple Commits (larger features)
```
1. feat(scope): add [foundation/types/interfaces]
2. feat(scope): implement [core logic]
3. feat(scope): add [integration/wiring]
4. test(scope): add tests for [feature]
5. docs: update documentation for [feature]
```

## Prohibitions

DO NOT:
- Plan without verifying assumptions about the codebase
- Assume code exists without checking
- Proceed with yellow/red light indicators without clarification
- Skip the skepticism protocol
- Create branch without syncing with develop first

## Output

A feature plan containing:
- Branch name (created)
- Summary of what will be built
- Implementation steps with commit boundaries
- Verification checklist
- Known blockers or concerns
- Scope assessment (small/medium/large)
