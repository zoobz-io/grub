# Pull Request Checklist

## Phase 1: Detect Base Branch

- [ ] Check for gitflow: `git branch -r | grep -q 'origin/develop'`
- [ ] If develop exists: base = `develop`
- [ ] If no develop: base = `main`
- [ ] If current branch is `develop`: base = `main` (release PR, suggest /release)

## Phase 2: Gather State

- [ ] Run `git branch --show-current` to get current branch
- [ ] Run `git log $BASE..HEAD --oneline` to see commits
- [ ] Run `git diff $BASE..HEAD --stat` to see change summary
- [ ] Run `git status` to check for uncommitted changes

## Phase 3: Pre-Flight Checks

### Working Directory
- [ ] No uncommitted changes (or confirm with user to proceed)
- [ ] No untracked files that should be committed

### Branch State
- [ ] Branch has commits ahead of base
- [ ] Check if branch is pushed: `git rev-parse --abbrev-ref @{upstream}`
- [ ] If not pushed: `git push -u origin $(git branch --show-current)`

### Remote Sync
- [ ] Fetch latest: `git fetch origin`
- [ ] Check for conflicts with base branch
- [ ] If conflicts detected, warn user before proceeding

## Phase 4: Anomaly Scan

REQUIRED: Review diff for issues. STOP and confirm with user if ANY detected.

### Size Check
- [ ] Count lines changed: `git diff $BASE..HEAD --stat | tail -1`
- [ ] If >1000 lines, suggest splitting into smaller PRs

### Content Check
- [ ] No secrets, API keys, or credentials
- [ ] No large binary files
- [ ] No debug statements (console.log, fmt.Println, print())
- [ ] No commented-out code blocks

### Scope Check
- [ ] Changes are related (single concern)
- [ ] If mixed concerns, suggest separate PRs

## Phase 5: Determine PR Type

Based on commits and changes:

| Change Type | PR Title Prefix |
|-------------|-----------------|
| New feature | `feat(scope):` |
| Bug fix | `fix(scope):` |
| Documentation | `docs:` |
| Tests | `test:` |
| Refactoring | `refactor:` |
| Performance | `perf:` |
| Maintenance | `chore:` |

## Phase 6: Find Related Issues

- [ ] Check commit messages for issue references (#123)
- [ ] Ask user: "Are there related issues to link?"
- [ ] Determine relationship: `Fixes #123` vs `Relates to #123`

## Phase 7: Draft PR Content

### Title
- [ ] Conventional format: `type(scope): description`
- [ ] Under 72 characters
- [ ] Imperative mood ("add" not "added")
- [ ] If multiple commits, summarize overall change

### Description
MUST include:
- [ ] Summary section (what and why)
- [ ] Changes section (bullet list)
- [ ] Testing section (checklist)
- [ ] Related section (issue links)

## Phase 8: Confirm & Create

- [ ] Present summary to user:
  - Branch name
  - Target base branch
  - PR title
  - Number of commits
  - Files changed
  - Any anomalies noted
- [ ] Get user confirmation

### Create PR
```bash
gh pr create \
  --base $BASE \
  --title "type(scope): description" \
  --body "$(cat <<'EOF'
## Summary
...

## Changes
- ...

## Testing
- [ ] Tests pass
- [ ] New code has tests

## Related
Fixes #123
EOF
)"
```

## Phase 9: Post-Create

- [ ] Show PR URL to user
- [ ] Offer to open in browser: `gh pr view --web`

### Verify Output
- [ ] PR has conventional title
- [ ] PR description matches format
- [ ] Related issues linked
