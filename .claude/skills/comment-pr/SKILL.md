# Comment on Pull Request

Post a comment or reply on a GitHub pull request through the ROCKHOPPER protocol.

## Purpose

All PR comments are public documentation posted under the ROCKHOPPER identity. They represent zoobzio — not individual agents, not internal workflow, not character voice. Every comment posted via this skill reads as professional technical communication.

## Language Rules

All comments MUST:
- Be neutral and professional in tone
- Read as documentation, not conversation
- Focus on facts: what, why, status
- Use third-person or passive voice

All comments MUST NOT:
- Reference agent names (Zidgel, Fidgel, Midgel, Kevin)
- Reference crew roles (Captain, Science Officer, First Mate, Engineer)
- Read as inter-agent dialogue
- Include character voice or personality
- Mention the crew, the penguins, or internal workflow structure
- Use first person ("I analyzed...", "We decided...")

### Prohibited Terms

These terms MUST NEVER appear in any PR comment:

| Prohibited | Why |
|-----------|-----|
| Zidgel, Fidgel, Midgel, Kevin | Blue team agent names |
| Captain, Science Officer, First Mate, Engineer | Blue team crew roles |
| Armitage, Case, Molly, Riviera | Red team agent names |
| MOTHER, ROCKHOPPER | Protocol names |
| red team, blue team, review team | Team structure |
| Colonel, cowboy, razor girl, illusionist | Character references |
| jack-in, filtration, mission criteria | Red team internal process |
| cyberspace, the matrix, Wintermute, Neuromancer | Fictional references |
| the crew, the team, our agents | Internal structure |
| Any internal workflow reference | Internal process |
| 3-2-1 Penguins, penguin, the ship | Source material references |

## Execution

1. Draft the comment content
2. Review against language rules — scan for every prohibited term
3. Rewrite any sentence that contains prohibited content
4. Post via appropriate `gh` command
5. Verify the comment was posted

## Comment Types

### Reviewer Response — Dismiss

When a reviewer comment is being dismissed with rationale:

```markdown
[Explain why the current approach is correct or intentional]

[Reference relevant design decisions, standards, or constraints if applicable]
```

Post the reply, then resolve the thread:
```bash
# Reply to a specific review comment
gh api repos/{owner}/{repo}/pulls/{pr}/comments/{comment_id}/replies -f body="[response]"

# Resolve the review thread (requires the thread's node ID)
gh api graphql -f query='mutation { resolveReviewThread(input: { threadId: "[thread_node_id]" }) { thread { isResolved } } }'
```

To find the thread node ID:
```bash
# List review threads on a PR
gh api graphql -f query='query { repository(owner: "{owner}", name: "{repo}") { pullRequest(number: {pr}) { reviewThreads(first: 50) { nodes { id isResolved comments(first: 1) { nodes { body } } } } } } }'
```

### Reviewer Response — Acknowledge

When a reviewer comment is being accepted and will be addressed:

```markdown
Valid point. This will be addressed in a follow-up commit.

[Brief description of what will change]
```

### Status Update

When reporting on workflow or CI status:

```markdown
## Status

[What happened and current state]

### Next Steps
- [What will be done]
```

## Formatting Standards

- Keep responses concise — address the specific point raised
- Use code blocks when referencing code
- Link to relevant lines or files when helpful
- Match the technical depth of the comment being responded to

## Self-Check

Before posting, verify:
- [ ] No agent names appear anywhere in the comment
- [ ] No crew roles appear anywhere in the comment
- [ ] No first-person voice ("I", "we", "our")
- [ ] Tone is neutral and professional
- [ ] Response addresses the specific point raised
- [ ] A stranger could read this and understand the rationale
