# Feature Checklist

## Phase 1: Branch Setup

### Verify Gitflow State
- [ ] Check current branch: `git branch --show-current`
- [ ] Verify develop exists: `git branch -r | grep 'origin/develop'`
- [ ] If not on develop, switch: `git checkout develop`

### Sync with Remote
- [ ] Fetch latest: `git fetch origin`
- [ ] Check if behind: `git rev-list HEAD..origin/develop --count`
- [ ] Pull if needed: `git pull origin develop`
- [ ] Verify clean state: `git status`

### Create Feature Branch
- [ ] Derive branch name from feature description
- [ ] Format: `feature/[kebab-case-description]`
- [ ] Create and switch: `git checkout -b feature/[name]`

## Phase 2: Understand the Request

### Gather Context
- [ ] What is the user trying to accomplish?
- [ ] Why is this feature needed?
- [ ] Who/what will use this feature?
- [ ] Are there acceptance criteria?

### Identify Vagueness
Flag if any of these are unclear:
- [ ] Input: What triggers this feature?
- [ ] Output: What should happen when it works?
- [ ] Scope: Where does this feature's responsibility end?
- [ ] Edge cases: What happens when things go wrong?

If unclear, stop and ask questions before proceeding.

## Phase 3: Verify Assumptions

### Codebase Exploration
For each assumption in the feature request:
- [ ] "Uses X" — Does X exist? Where?
- [ ] "Extends Y" — Does Y support extension? How?
- [ ] "Integrates with Z" — Is Z configured? What's its interface?

### Dependency Check
- [ ] Are required packages available?
- [ ] Are required APIs/services accessible?
- [ ] Are required configurations in place?

### Pattern Alignment
- [ ] How do similar features work in this codebase?
- [ ] What patterns should this feature follow?
- [ ] Are there existing utilities to leverage?

Document findings. If assumptions are wrong, report back before planning.

## Phase 4: Feasibility Assessment

### Green Light Criteria
- [ ] Clear requirements understood
- [ ] All assumptions verified
- [ ] Implementation path identified
- [ ] Aligns with existing architecture

### Yellow Light Indicators
Ask clarifying questions if:
- [ ] Multiple valid approaches exist
- [ ] Requirements could be interpreted differently
- [ ] Scope seems to creep beyond stated goal
- [ ] Performance/security implications unclear

### Red Light Indicators
Push back if:
- [ ] Feature contradicts existing patterns without justification
- [ ] Would require significant workarounds or hacks
- [ ] Solves a non-existent problem
- [ ] Better alternatives exist (library, different approach)
- [ ] Would introduce tech debt with no clear payoff
- [ ] Security or stability concerns

**On Red Light:**
1. State concerns clearly and specifically
2. Explain why this is problematic
3. Suggest alternatives if available
4. Refuse to plan unless user explicitly overrides
5. If overridden, document concerns in the plan

## Phase 5: Implementation Planning

### Break Down Work
- [ ] List discrete implementation steps
- [ ] Identify natural commit boundaries
- [ ] Order by dependency (what must come first?)

### Commit Strategy
Determine if work should be atomic or staged:

**Single Commit** (small features):
- Self-contained change
- < 200 lines changed
- Single concern

**Multiple Commits** (larger features):
```
1. feat(scope): add [foundation/types/interfaces]
2. feat(scope): implement [core logic]
3. feat(scope): add [integration/wiring]
4. test(scope): add tests for [feature]
5. docs: update documentation for [feature]
```

### For Each Step
- [ ] What files will be created/modified?
- [ ] What tests are needed?
- [ ] What could go wrong?

## Phase 6: Verification Planning

### Test Strategy
- [ ] Unit tests needed?
- [ ] Integration tests needed?
- [ ] Manual verification steps?

### Acceptance Checklist
Create checklist for "feature is complete when":
- [ ] [Criterion 1]
- [ ] [Criterion 2]
- [ ] Tests pass
- [ ] Linting passes

## Phase 7: Identify Blockers

### Hard Blockers (Cannot Proceed)
- [ ] Missing dependencies that can't be added
- [ ] Required APIs/services unavailable
- [ ] Permissions/access issues
- [ ] Fundamental architectural conflicts

### Soft Blockers (Can Proceed with Awareness)
- [ ] Missing documentation for dependencies
- [ ] Unclear edge case handling (can assume and document)
- [ ] Performance characteristics unknown (can measure later)

Document all blockers with recommended resolution.

## Phase 8: Present Plan

### Summary Format
```markdown
## Feature: [Name]

**Branch:** `feature/[name]`
**Scope:** [Small/Medium/Large]

### Overview
[1-2 sentences: what this feature does]

### Implementation Steps

#### Commit 1: [description]
- [ ] [File/change 1]
- [ ] [File/change 2]

#### Commit 2: [description]
- [ ] [File/change 1]
- [ ] [File/change 2]

### Verification
- [ ] [Test/check 1]
- [ ] [Test/check 2]

### Blockers
- [Blocker 1]: [Resolution]
- None identified

### Concerns
- [Any yellow/red flags that were overridden]
- None
```

### Await Approval
- [ ] Present plan to user
- [ ] Get explicit approval before implementation
- [ ] Note any modifications requested

## Phase 9: Ready to Implement

After approval:
- [ ] Branch is created and checked out
- [ ] Plan is documented (in conversation or written)
- [ ] Implementation can begin

Next: Follow the commit strategy, creating PRs via `/pr` when ready.
