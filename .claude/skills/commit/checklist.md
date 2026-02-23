# Commit Checklist

## Phase 1: Gather State

- [ ] Run `git status` to see staged and unstaged changes
- [ ] Run `git diff --cached --stat` to see staged change summary
- [ ] Run `git diff --cached` to review actual staged changes
- [ ] If nothing staged, ask user what to stage

## Phase 2: Anomaly Scan

REQUIRED: Review staged changes for red flags. STOP and confirm with user if ANY detected.

### Secrets & Credentials
- [ ] No API keys, tokens, or secrets in diff
- [ ] No .env files being committed
- [ ] No credentials.json, serviceAccountKey, or similar
- [ ] No private keys (.pem, .key files)
- [ ] No hardcoded passwords or connection strings

### File Hygiene
- [ ] No files >1MB (check with `git diff --cached --stat`)
- [ ] No unexpected binary files
- [ ] No generated files (node_modules/, dist/, vendor/, *.min.js)
- [ ] No IDE/editor files (.idea/, .vscode/settings.json, *.swp)
- [ ] No OS files (.DS_Store, Thumbs.db)

### Code Quality
- [ ] No debug statements (console.log, fmt.Println for debugging, print())
- [ ] No commented-out code blocks being added
- [ ] No TODO/FIXME being introduced without intent

### Commit Scope
- [ ] Changes are related (single logical unit)
- [ ] No unrelated fixes mixed in
- [ ] If multiple concerns detected, suggest splitting into separate commits

## Phase 3: Classify Change

Determine commit type based on what changed:

| Change | Type |
|--------|------|
| New functionality | `feat` |
| Bug fix | `fix` |
| Documentation only | `docs` |
| Test additions/changes | `test` |
| Code restructuring (no behavior change) | `refactor` |
| Performance improvement | `perf` |
| Tooling, deps, maintenance | `chore` |
| Build/CI changes | `build` |

## Phase 4: Determine Scope

- [ ] Identify affected area
- [ ] Format scope as lowercase, no spaces
- [ ] OMIT scope if change is broad

## Phase 5: Draft Message

Format: type(scope): description

### Verify Message Requirements
- [ ] Imperative mood ("add" not "added")
- [ ] Lowercase first letter
- [ ] No period at end
- [ ] Under 72 characters
- [ ] Describes what, implies why

## Phase 6: Confirm & Commit

- [ ] Present summary to user:
  - Files changed
  - Commit type and scope
  - Proposed message
  - Any anomalies noted
- [ ] Get user confirmation
- [ ] Run `git commit -m "<message>"`
- [ ] Show result with `git log -1 --oneline`

### Verify Prohibitions
- [ ] No Co-Authored-By trailer added
- [ ] No GPG signing added (unless user configured)
- [ ] Single-line message (unless body truly necessary)
