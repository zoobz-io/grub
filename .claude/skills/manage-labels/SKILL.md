# Manage Labels

Create, update, and apply labels on GitHub issues and pull requests through the ROCKHOPPER protocol.

## Purpose

Labels are public metadata on issues and PRs posted under the ROCKHOPPER identity. They must describe workflow state clearly without referencing internal agent structure. Label names and descriptions are visible to anyone browsing the repository.

## Language Rules

Label names and descriptions MUST NOT:
- Reference agent names (Zidgel, Fidgel, Midgel, Kevin)
- Reference crew roles (Captain, Science Officer, First Mate, Engineer)
- Reference internal workflow structure
- Use jargon that only makes sense with knowledge of the agent system

Label descriptions MUST:
- Describe what the label means in plain terms
- Be useful to an external contributor who knows nothing about the agent system

## Available Labels

### Phase Labels (mutually exclusive)

| Label | Description | Applied When |
|-------|-------------|-------------|
| `phase:plan` | Requirements and architecture in progress | Work begins on an issue |
| `phase:build` | Implementation and testing in progress | Plan is agreed, build starts |
| `phase:review` | Reviewing deliverables | All implementation complete, tests passing |
| `phase:document` | Documentation assessment and updates | Review passes |
| `phase:pr` | Pull request open, awaiting CI and feedback | PR created |

Only one phase label may be present at a time. When applying a new phase label, remove the previous one.

### Escalation Labels

| Label | Description | Applied When |
|-------|-------------|-------------|
| `escalation:architecture` | Investigating an architectural concern | Complex problem surfaced during build |
| `escalation:scope` | Scope expansion under consideration | Requirements gap identified |

Escalation labels are additive — they coexist with phase labels. Remove when the escalation is resolved.

### Type Labels

| Label | Description |
|-------|-------------|
| `feature` | New functionality |
| `bug` | Defect in existing functionality |
| `docs` | Documentation only |
| `infra` | CI, tooling, or project infrastructure |

## Execution

### Applying a Phase Label

```bash
# Remove current phase label and apply new one
gh issue edit [number] --remove-label "phase:[current]" --add-label "phase:[new]"
```

### Applying an Escalation Label

```bash
gh issue edit [number] --add-label "escalation:[type]"
```

### Removing an Escalation Label

```bash
gh issue edit [number] --remove-label "escalation:[type]"
```

### Creating a Missing Label

If a required label doesn't exist on the repo:

```bash
gh label create "[name]" --description "[description]" --color "[hex]"
```

Use these colors:

| Label | Color |
|-------|-------|
| `phase:plan` | `#d4c5f9` |
| `phase:build` | `#0075ca` |
| `phase:review` | `#e4e669` |
| `phase:document` | `#0e8a16` |
| `phase:pr` | `#1d76db` |
| `escalation:architecture` | `#d93f0b` |
| `escalation:scope` | `#fbca04` |
| `feature` | `#a2eeef` |
| `bug` | `#d73a4a` |
| `docs` | `#0075ca` |
| `infra` | `#c5def5` |

## Self-Check

Before applying labels, verify:
- [ ] Only one phase label will be present after the change
- [ ] Label description does not reference internal agent structure
- [ ] Phase transition is valid per the workflow
