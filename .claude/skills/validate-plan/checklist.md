# Validate Plan Checklist

## Phase 1: Understand the Plan

- [ ] Read the proposed plan completely
- [ ] Identify the core objective
- [ ] List expected deliverables
- [ ] Note stated acceptance criteria

## Phase 2: Check Mission Alignment

- [ ] Read MISSION.md
- [ ] Does this plan serve the stated mission?
- [ ] Is this the right package for this work?
- [ ] Does it conflict with non-goals?

## Phase 3: Check for Overlap

### Existing Code
- [ ] Search codebase for similar functionality
- [ ] Check if this extends vs duplicates existing code
- [ ] Identify files that would be affected

### Open Issues
- [ ] Run: gh issue list --state open
- [ ] Check for duplicate issues
- [ ] Check for related/blocking issues
- [ ] Note any issues this would close

### Recent History
- [ ] Check recent commits for related work
- [ ] Check recently closed issues
- [ ] Was this tried before? What happened?

## Phase 4: Assess Scope

### Clarity
- [ ] Is the objective unambiguous?
- [ ] Are deliverables specific?
- [ ] Can acceptance criteria be verified?

### Size
- [ ] Is scope appropriate for a single issue?
- [ ] Should this be broken into multiple issues?
- [ ] Are there natural milestones?

## Phase 5: Assess Feasibility

### Architecture
- [ ] Does current structure support this?
- [ ] Would this require architectural changes?
- [ ] Are patterns established for this type of work?

### Dependencies
- [ ] Are required dependencies available?
- [ ] Would new dependencies be needed?
- [ ] Are there version constraints?

### Blockers
- [ ] Are there known technical blockers?
- [ ] Are there external dependencies (APIs, services)?
- [ ] Is there missing context or documentation?

## Phase 6: Render Judgment

### If Valid
- [ ] Summarize alignment
- [ ] Note scope assessment
- [ ] Recommend proceeding to issue creation

### If Gaps Found
- [ ] List specific concerns
- [ ] Formulate clarifying questions
- [ ] Recommend addressing gaps first
