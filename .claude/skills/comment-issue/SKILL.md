# Comment on Issue

Post a comment on a GitHub issue through the ROCKHOPPER protocol.

## Purpose

All issue comments are public documentation posted under the ROCKHOPPER identity. They represent zoobzio — not individual agents, not internal workflow, not character voice. Every comment posted via this skill reads as professional technical documentation.

## Language Rules

All comments MUST:
- Be neutral and professional in tone
- Read as documentation, not conversation
- Focus on facts: what, why, status
- Use third-person or passive voice ("The implementation uses..." not "I built...")

All comments MUST NOT:
- Reference agent names (Zidgel, Fidgel, Midgel, Kevin)
- Reference crew roles (Captain, Science Officer, First Mate, Engineer)
- Read as inter-agent dialogue
- Include character voice or personality
- Mention the crew, the penguins, or internal workflow structure
- Use first person ("I analyzed...", "We decided...")

### Prohibited Terms

These terms MUST NEVER appear in any GitHub comment:

| Prohibited | Why |
|-----------|-----|
| Zidgel, Fidgel, Midgel, Kevin | Blue team agent names |
| Captain, Science Officer, First Mate, Engineer | Blue team crew roles |
| Armitage, Case, Molly, Riviera | Red team agent names |
| red team, review team | Red team internal structure |
| Colonel, cowboy, razor girl, illusionist | Character references |
| jack-in, filtration, mission criteria | Red team internal process |
| cyberspace, the matrix, Wintermute, Neuromancer | Fictional references |
| MOTHER, ROCKHOPPER | Protocol names |
| the crew, the team, our agents | Internal structure |
| spec from Fidgel, guidance from Kevin | Internal workflow |
| phase:plan, phase:build, phase:review, phase:document, phase:pr (in prose) | Internal labels as narrative |
| escalation, RFC (as workflow terms) | Internal process |
| 3-2-1 Penguins, penguin, the ship | Source material references |

Labels may be referenced as metadata (e.g., "Label updated to `phase:review`") but not as narrative elements.

## Execution

1. Draft the comment content
2. Review against language rules — scan for every prohibited term
3. Rewrite any sentence that contains prohibited content
4. Post via `gh issue comment [number] --body "[content]"`
5. Verify the comment was posted

## Comment Types

### Architecture Plan
Posted when design is complete. Structured, technical, actionable.

### Execution Plan
Posted when implementation chunks are defined. Clear breakdown of work.

### Test Summary
Posted when testing is complete. Factual, data-driven.

### Scope Clarification
Posted when requirements are expanded or refined.

### Status Update
Posted when phase transitions occur or significant progress is made.

## Formatting Standards

- Use markdown headers to organize sections
- Use tables for structured data
- Use code blocks for code references
- Use checklists for verifiable items
- Keep paragraphs short — one idea per paragraph

## Self-Check

Before posting, verify:
- [ ] No agent names appear anywhere in the comment
- [ ] No crew roles appear anywhere in the comment
- [ ] No first-person voice ("I", "we", "our")
- [ ] Tone is neutral and professional
- [ ] Comment reads as standalone documentation
- [ ] A stranger could read this and learn something useful
