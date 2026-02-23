# Pull Request

Create a pull request through the ROCKHOPPER protocol with proper title, description, and linked issues via `gh` CLI.

## Execution

1. Read `checklist.md` in this skill directory
2. Detect base branch (develop if gitflow, main otherwise)
3. Gather branch state and commit history
4. Scan for anomalies before opening PR
5. Draft title and description
6. Confirm with user before creating

## Specifications

### Base Branch Detection

Check for gitflow setup:
```bash
git branch -r | grep -q 'origin/develop'
```

| Condition | Base Branch |
|-----------|-------------|
| develop exists | `develop` |
| no develop | `main` |
| current branch is develop | `main` (release PR — suggest /release skill) |

### PR Title Format

MUST follow conventional commit style:
- `feat(scope): description`
- `fix(scope): description`
- `docs: description`

Requirements:
- Under 72 characters
- Imperative mood ("add" not "added")
- If multiple commits, summarize overall change

### PR Description Format

```markdown
## Summary
[1-3 sentences describing what and why]

## Changes
- [Key change 1]
- [Key change 2]
- [Key change 3]

## Testing
- [ ] Tests pass (`make test`)
- [ ] Linting passes (`make lint`)
- [ ] New code has tests

## Related
[Fixes #123 / Relates to #456]
```

### Anomaly Flags

STOP and confirm with user if ANY detected:

| Anomaly | Action |
|---------|--------|
| Uncommitted changes | Warn user, confirm proceed |
| Branch not pushed | Push with `-u` flag |
| Large diff (>1000 lines) | Suggest splitting |
| Secrets in diff | BLOCK until resolved |
| Merge conflicts with base | Warn user |

## Prohibitions

DO NOT:
- Create PR without user confirmation
- Create PR with secrets in diff
- Skip anomaly scan
- Use non-conventional title format

## Output

A pull request that:
- Has clear, conventional title
- Describes the change and its purpose
- Links related issues
- Is ready for review
