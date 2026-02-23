# Mission Review Checklist

## Phase 1: Mission Document Assessment

### MISSION.md Exists
- [ ] MISSION.md is present in `.claude/`
- [ ] MISSION.md is non-empty

### Required Sections
- [ ] Purpose section exists and contains a clear statement
- [ ] "What This Package Contains" section exists
- [ ] "What This Package Does NOT Contain" section exists
- [ ] Success Criteria section exists
- [ ] Non-Goals section exists

### Section Quality — Where Does the Mission Lie?
- [ ] Purpose is a single, focused statement — or is it hedging?
- [ ] Contains list is concrete — or is it vague enough to mean anything?
- [ ] Exclusions list is concrete — or are they so broad they exclude nothing?
- [ ] Success criteria are step-by-step and testable — or are they aspirational?
- [ ] Non-goals are specific decisions — or are they obvious negatives nobody would pursue?

## Phase 2: Package Surface Inventory

### Exported API
- [ ] List all exported types
- [ ] List all exported functions
- [ ] List all exported constants and variables
- [ ] List all packages/subpackages

### Capabilities
- [ ] Describe what the package actually does (from code, not docs)
- [ ] Identify the core workflow a user follows
- [ ] Identify secondary/supporting capabilities

## Phase 3: Purpose Alignment — Where Does Implementation Contradict the Mission?

### Core Alignment
- [ ] Does the package's actual behavior match the stated purpose — or does it do something else?
- [ ] Does the package do significantly more than the purpose states? (scope creep)
- [ ] Does the package do significantly less than the purpose states? (broken promise)

### Feature Classification
For each exported capability:
- [ ] Classify as: aligned, adjacent, contradictory, or orphaned
- [ ] Document evidence for non-aligned classifications

### Adjacent Features — Unauthorized Scope
- [ ] List features related but not explicitly covered by mission
- [ ] For each: should the mission expand or the feature be removed?

### Contradictory Features — Violations
- [ ] List features that contradict non-goals or exclusions
- [ ] For each: is the feature wrong or is the mission outdated?

### Orphaned Features — Dead Weight
- [ ] List features that serve no apparent mission purpose
- [ ] For each: useful addition (update mission) or dead weight (remove)?

## Phase 4: Contains/Excludes Verification — What's Missing? What Shouldn't Be Here?

### "What This Package Contains"
For each listed item:
- [ ] Item actually exists in the package — or is it a phantom?
- [ ] Item is functional, not just present
- [ ] Item matches the description — or does it do something different?

### "What This Package Does NOT Contain"
For each listed exclusion:
- [ ] The excluded thing is genuinely absent — or did it sneak in?
- [ ] No workarounds or partial implementations of excluded items
- [ ] Exclusion is still a reasonable boundary

### Unlisted Items
- [ ] Identify anything the package contains that isn't listed in either section
- [ ] Classify each: belongs in contains, belongs in excludes, or shouldn't exist

## Phase 5: Success Criteria Verification — Which Criteria Fail?

For each success criterion:

### Achievability
- [ ] Can the criterion be achieved with the current implementation — or is it blocked?
- [ ] Are the described steps actually possible to follow?
- [ ] What missing functionality blocks the criterion?

### Walk-Through
- [ ] Attempt to follow each criterion step-by-step
- [ ] Document where the walk-through fails or gets stuck
- [ ] Each failure point is a finding

## Phase 6: Non-Goals Verification — Which Non-Goals Are Violated?

For each non-goal:

### Respected
- [ ] The implementation does not pursue this goal — or does it?
- [ ] No partial or accidental implementation of the non-goal
- [ ] No API surface that implies the non-goal is supported

### Relevance
- [ ] The non-goal is still relevant (not outdated)
- [ ] The non-goal reflects a deliberate design decision

## Phase 7: Philosophy Alignment — Where Does Code Break Zoobzio Principles?

### Dependency Policy
- [ ] Production dependencies are minimal — or is there bloat?
- [ ] Each dependency is justified — or is stdlib sufficient?
- [ ] Provider-specific deps are in submodules — or leaking into root?

### Type Safety
- [ ] Generics used where appropriate — or is there interface{} laziness?
- [ ] No `any` in public API where type parameters would work

### Boundaries
- [ ] Data transformations at identifiable boundaries — or scattered?
- [ ] Boundaries are explicit, not implicit

### Composition
- [ ] Interfaces are small and composable — or wide and rigid?
- [ ] Clear separation between processors, connectors, and values

### Errors
- [ ] Semantic errors for expected failures — or bare error strings?
- [ ] Errors carry context

### Context
- [ ] I/O operations accept `context.Context` — or missing?

## Phase 8: MISSION.md Currency — Where Does the Mission Document Lie?

### Is the Mission Current?
- [ ] Mission reflects the package as it exists today — or a past/future version?
- [ ] No sections describing aspirational features as present
- [ ] No sections describing removed features as present
- [ ] Success criteria achievable with current code
