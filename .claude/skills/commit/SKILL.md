# Commit

Review staged changes, flag anomalies, and create a conventional commit.

## Execution

1. Read `checklist.md` in this skill directory
2. Complete anomaly scan — DO NOT skip this phase
3. Summarize changes and propose commit message
4. Confirm with user before committing

## Specifications

### Conventional Commit Format

All commits MUST follow this format: type(scope): description

| Type | When to Use | Version Bump |
|------|-------------|--------------|
| `feat` | New feature | minor |
| `fix` | Bug fix | patch |
| `docs` | Documentation only | none |
| `test` | Adding/updating tests | none |
| `refactor` | Code change that neither fixes nor adds | none |
| `perf` | Performance improvement | patch |
| `chore` | Maintenance, deps, tooling | none |
| `build` | Build system or external deps | none |

### Message Requirements

The commit message MUST:
- Use imperative mood ("add" not "added")
- Start with lowercase letter
- Have no period at end
- Be under 72 characters
- Describe what changed and imply why

### Scope

Scope MUST reflect the affected area:
- Package/module name: `feat(cache):`
- Component: `fix(auth):`
- File type: `docs:`, `test:`
- OMIT scope if change is broad: `chore: update dependencies`

## Anomaly Flags

STOP and confirm with user if ANY of these are detected:

| Category | Red Flags |
|----------|-----------|
| Secrets | API keys, tokens, credentials, .env files, private keys (.pem, .key) |
| Large Files | Any file >1MB |
| Binary Files | Unless explicitly expected |
| Generated Files | node_modules/, dist/, vendor/, *.min.js |
| IDE/OS Files | .idea/, .vscode/settings.json, *.swp, .DS_Store |
| Code Quality | Debug statements (console.log, fmt.Println), commented-out code |
| Scope | Unrelated changes mixed together |

## Prohibitions

DO NOT:
- Skip the anomaly scan
- Commit without user confirmation
- Add Co-Authored-By trailer (keep git log clean)
- Add GPG signing unless user has configured it
- Use multi-line message body unless truly necessary
- Commit unrelated changes in single commit

## Output

A single focused commit with:
- Conventional commit message matching format above
- Scope reflecting the affected area
- Clear description of what changed
- Clean trailer (no co-author attribution)
