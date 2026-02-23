# Documentation Create

Create package documentation that teaches through structure—learn, guide, reference—with consistent tone and frontmatter for site publishing.

## Principles

1. **Clear and direct** — Writing MUST be technical but accessible
2. **Example-driven** — MUST show, then explain
3. **Purpose-fit** — Each doc type MUST serve a distinct need
4. **Cohesive voice** — Docs MUST feel like they belong together

## Execution

1. Read `checklist.md` in this skill directory
2. Determine which documentation files are needed (ask user if unclear)
3. Create files per specifications
4. Verify all frontmatter and cross-references

## Specifications

### Directory Structure

All files and directories MUST use numeric prefixes:

```
docs/
├── 1.learn/
│   ├── 1.overview.md
│   ├── 2.quickstart.md
│   ├── 3.concepts.md
│   └── 4.architecture.md
├── 2.guides/
│   ├── 1.testing.md       (required)
│   ├── 2.troubleshooting.md (required)
│   └── 3.[feature].md     (continue numbering)
├── 3.integrations/        (optional)
│   ├── 1.[tool].md
│   └── 2.[tool].md
└── 4.reference/
    ├── 1.api.md
    └── 2.types.md
```

### Naming Convention

Format: `[number].[name].md`
- Number controls order
- Name describes content
- Numbers MUST be sequential with no gaps

### Cross-References

All internal links MUST use full numbered paths:
- Correct: `[Quickstart](../1.learn/2.quickstart.md)`
- Wrong: `[Quickstart](../learn/quickstart.md)`

### Required Frontmatter

Every documentation file MUST have:

```yaml
---
title: Article Title
description: One-line description
author: [author]
published: YYYY-MM-DD
updated: YYYY-MM-DD
tags:
  - Tag1
  - Tag2
---
```

### File Purposes

| File | Purpose | Reader Goal |
|------|---------|-------------|
| Overview | Entry point—60-second decision | "Is this for me?" |
| Quickstart | Zero to productive | "How do I use this?" |
| Concepts | Mental models | "How should I think about this?" |
| Architecture | Internals | "How does it work?" |
| Guides | Feature deep-dives | "How do I do X?" |
| Integrations | Ecosystem connections | "How does this fit with Y?" |
| Reference | API documentation | "What's the signature for Z?" |

See checklist for specific content requirements per file type.

## Prohibitions

DO NOT:
- Use non-numbered file/directory names
- Use relative paths without numbers in cross-references
- Omit frontmatter from any file
- Create files without required sections per type

## Output

Documentation files that:
- Answer the reader's question for that doc type
- Cross-reference using full numbered paths
- Feel cohesive when read alongside other package docs
- Include complete frontmatter for site publishing
