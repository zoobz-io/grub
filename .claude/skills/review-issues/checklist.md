# Issue Review Checklist

## Phase 1: Inventory

### Open Issues
- [ ] Fetch all open issues via `gh issue list --state open`
- [ ] Count total open issues
- [ ] Note any issues without labels

## Phase 2: Structure Assessment — What's Missing?

For each open issue:

### Required Sections
- [ ] Objective section exists — or is the issue just a title?
- [ ] Context section exists — or is there no "why"?
- [ ] Acceptance Criteria exist — or is "done" undefined?
- [ ] Scope section exists — or is the boundary invisible?

### Objective Quality
- [ ] Objective is a single statement — or a rambling paragraph?
- [ ] Objective describes an outcome — or a task list?
- [ ] Objective is specific enough to evaluate completion — or is it vague?
- [ ] Objective is free of implementation details — or does it prescribe how?

### Acceptance Criteria Quality
- [ ] Each criterion is independently verifiable — or subjective?
- [ ] Criteria use checkbox format (`- [ ]`) — or prose?
- [ ] Criteria describe observable behavior — or feelings ("works correctly", "is fast")?
- [ ] All criteria together define "done" — or are there gaps?

### Scope Quality
- [ ] "In scope" items explicitly listed — or implied?
- [ ] "Out of scope" items explicitly listed — or absent?
- [ ] Scope doesn't contradict acceptance criteria
- [ ] Boundaries are clear enough to prevent scope creep

## Phase 3: Label Assessment — What's Invisible?

### Type Labels
- [ ] Issue has a type label (feature, bug, docs, infra) — or is it unlabeled?
- [ ] Type label is correct for the issue content

### Phase Labels
- [ ] Issues in active work have a phase label — or is state invisible?
- [ ] Phase label matches actual state of work

### Missing Labels
- [ ] Identify all issues without any labels
- [ ] Each is a finding — unlabeled issues are invisible to the workflow

## Phase 4: Duplicate and Overlap Detection — What's Wasted Work?

### Duplicates
- [ ] Identify issues that describe the same problem
- [ ] Identify issues that would produce the same code changes
- [ ] Each duplicate pair is a finding — recommend merge or close

### Overlaps
- [ ] Identify issues with partially overlapping scope
- [ ] Check for cross-references between overlapping issues
- [ ] Missing cross-references are findings — overlapping work without coordination

### Dependencies
- [ ] Identify issues that depend on other issues
- [ ] Check for explicit dependency linkage
- [ ] Missing dependency links are findings — builds will fail when prerequisites aren't met

## Phase 5: Actionability Assessment — What Will Cause Problems?

For each issue:

### Can Work Begin?
- [ ] Is there enough information to start architecture? If not, what's missing?
- [ ] Is there enough information to start implementation? If not, what's missing?
- [ ] Are there unanswered questions that will block progress?

### Will Review Succeed?
- [ ] Are acceptance criteria specific enough to review against?
- [ ] Can a reviewer objectively determine pass/fail?
- [ ] Is scope clear enough to know what's in and what's out of review?

### Will This Issue Cause Churn?
- [ ] Is scope likely to expand mid-build due to ambiguity?
- [ ] Are there implicit requirements that aren't stated?
- [ ] Will builders have to guess at intent?
