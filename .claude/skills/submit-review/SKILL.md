# Submit PR Review

Submit a complete PR review with file/line-scoped comments and a verdict through the MOTHER protocol.

## Purpose

This is the final act of the review workflow. Armitage has accumulated findings throughout the review and filtration phases. This skill constructs the review payload and submits it as a single GitHub PR review.

All review content is public documentation posted under the MOTHER identity. No agent names, no character voice, no internal structure references. Review the prohibited terms list in the comment-pr skill — the same rules apply here.

## Inputs

Armitage maintains these accumulated lists during Phases 4-5:

### Review Comments (line-scoped)

Each entry:
- `path` — file path relative to repo root
- `line` — line number in the diff
- `body` — MOTHER-formatted comment text

### Summary Items

Each entry:
- Finding ID
- Category (mission concern, broad observation, noted item)
- Text

### Verdict

- `APPROVE` — No High or Critical severity findings remain after disposition
- `REQUEST_CHANGES` — One or more High or Critical findings require action

## Constructing the Summary Body

```markdown
## Review Summary

[One sentence: what was reviewed and the scope of changes.]

### Findings

[Count of findings by category. Example:]
- Code: 3 findings (1 high, 2 medium)
- Tests: 2 findings (1 high, 1 low)
- Security: 1 finding (1 medium, confirmed)
- Coverage: 1 finding (1 medium)

### Observations

[Summary-level findings that do not attach to specific lines. Mission concerns, broad patterns, systemic issues. Omit section if none.]

### Assessment

[Overall assessment. One paragraph. What is the state of this code relative to what it claims to be.]
```

Clean review (no findings):

```markdown
## Review Summary

[What was reviewed.]

No actionable findings identified. The changes are consistent with the stated objectives.
```

## Execution

### Step 1: Validate Comments

For each accumulated review comment:
1. Verify `path` exists in the PR diff
2. Verify `line` is within the diff range for that file
3. Scan `body` against MOTHER prohibited terms list
4. If path or line fails validation, promote to summary body instead

Get the diff to verify:
```bash
gh api repos/{owner}/{repo}/pulls/{pr_number}/files --jq '.[].filename'
```

### Step 2: Construct Summary

Build the summary body from accumulated summary items per the format above. Scan the complete summary against MOTHER prohibited terms list.

### Step 3: Determine Verdict

- If any accepted finding has severity High or Critical → `REQUEST_CHANGES`
- Otherwise → `APPROVE`

### Step 4: Submit

With comments:
```bash
gh api repos/{owner}/{repo}/pulls/{pr_number}/reviews \
  --method POST \
  --input - <<EOF
{
  "event": "REQUEST_CHANGES",
  "body": "[summary body]",
  "comments": [
    {"path": "[file]", "line": [line], "body": "[comment]"},
    ...
  ]
}
EOF
```

Without comments (clean review):
```bash
gh api repos/{owner}/{repo}/pulls/{pr_number}/reviews \
  --method POST \
  -f event="APPROVE" \
  -f body="[summary body]"
```

### Step 5: Verify

Confirm the review was posted:
```bash
gh api repos/{owner}/{repo}/pulls/{pr_number}/reviews --jq '.[-1] | {id, state, body}'
```

## Self-Check

Before submission, verify:
- [ ] Every comment body passes MOTHER prohibited terms check
- [ ] Summary body passes MOTHER prohibited terms check
- [ ] No agent names, character voice, or process references anywhere
- [ ] Every comment path exists in the PR diff
- [ ] Every comment line is within the diff range
- [ ] Verdict matches severity of accepted findings
- [ ] Review reads as professional technical documentation authored by a single voice
