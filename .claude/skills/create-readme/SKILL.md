# README Create

Create a README that captures the library's essence through code, not marketing.

## Principles

1. **Essence over problem** — Lead with what makes this library unique
2. **Show, don't frame** — Code speaks louder than "The Problem / The Solution"
3. **Voice from nature** — Let the library's character dictate tone
4. **Structure as scaffolding** — Sections provide rhythm, not script

## Execution

1. Read `checklist.md` in this skill directory
2. Complete all Discovery tasks before writing
3. Complete all Synthesis tasks and confirm with user
4. Write README per specifications below
5. Verify output against all requirements

## Specifications

### Badges

The README MUST include exactly these 8 badges immediately after the title. Replace `[package]` with the actual package name. DO NOT modify URLs, colors, styles, or badge providers.

| # | Badge | Markdown |
|---|-------|----------|
| 1 | CI Status | `[![CI Status](https://github.com/zoobzio/[package]/workflows/CI/badge.svg)](https://github.com/zoobzio/[package]/actions/workflows/ci.yml)` |
| 2 | Coverage | `[![codecov](https://codecov.io/gh/zoobzio/[package]/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobzio/[package])` |
| 3 | Report Card | `[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/[package])](https://goreportcard.com/report/github.com/zoobzio/[package])` |
| 4 | CodeQL | `[![CodeQL](https://github.com/zoobzio/[package]/workflows/CodeQL/badge.svg)](https://github.com/zoobzio/[package]/security/code-scanning)` |
| 5 | Go Reference | `[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/[package].svg)](https://pkg.go.dev/github.com/zoobzio/[package])` |
| 6 | License | `[![License](https://img.shields.io/github/license/zoobzio/[package])](LICENSE)` |
| 7 | Go Version | `[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/[package])](go.mod)` |
| 8 | Release | `[![Release](https://img.shields.io/github/v/release/zoobzio/[package])](https://github.com/zoobzio/[package]/releases)` |

### Structure

The README MUST contain these sections in this order:

| # | Section | Requirements |
|---|---------|--------------|
| 1 | Header | Title, badges (all 8), tagline + one supporting sentence |
| 2 | Hook | Library-specific name (NOT "Overview"/"Introduction"), 10-15 line code example showing core insight |
| 3 | Install | `go get` command, prerequisites if any |
| 4 | Quick Start | Complete runnable example with imports, demonstrates core workflow |
| 5 | Capabilities | Feature table with links to documentation |
| 6 | Why [Name]? | 3-5 bullet points with concrete benefits |
| 7 | Ecosystem | Library-specific name, related projects (OMIT section if none exist) |
| 8 | Documentation | Links organized as Learn / Guides / Reference |
| 9 | Contributing | One sentence + link to CONTRIBUTING.md |
| 10 | License | Single line: "MIT License - see [LICENSE](LICENSE)" |

### Hook Section Naming

The hook section MUST have a library-specific name that captures the core insight.

PROHIBITED names: "Overview", "Introduction", "How It Works", "Getting Started", "About"

Examples of compliant names:
- "One Annotation" (for a metadata library)
- "Define Once, Query Anywhere" (for a schema library)
- "The Pipeline" (for a processing library)

### Code Blocks

Each code block serves a distinct purpose:

| Block | Purpose | Characteristics |
|-------|---------|-----------------|
| Hook | Show the insight | Attention-grabbing, captures the "aha moment" |
| Quick Start | Show the workflow | Complete, runnable, demonstrates core usage |
| Capability | Show the variation | Focused snippets for individual features |

DO NOT duplicate the same example across blocks.

## Prohibitions

DO NOT:
- Use alternative badge providers or styles
- Omit any of the 8 required badges
- Use generic hook section names
- Include API reference tables (belongs in docs/reference)
- Duplicate examples across Hook and Quick Start
- Add feature laundry lists (belongs in docs)
- Frame with "The Problem / The Solution"
- Write in template voice (README MUST feel unique to THIS library)

## Output

A README.md that:
- Contains all 8 badges with exact URLs specified
- Could only describe THIS library
- Opens with code, not marketing copy
- Provides clear navigation to deeper documentation
