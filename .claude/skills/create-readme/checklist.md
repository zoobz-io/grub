# README Create Checklist

## Phase 1: Discovery

### Package Understanding
- [ ] Read main entry points (exports, public API)
- [ ] Examine go.mod for module path and dependencies
- [ ] Review doc.go and existing comments
- [ ] Study test files for usage patterns
- [ ] Check examples/ directory if present

### Essence Discovery
- [ ] Identify the core insight ("aha moment")
- [ ] Identify what distinguishes this from alternatives
- [ ] Identify target audience and their existing knowledge

### Code Selection
- [ ] Select hook example: ~10-15 lines capturing core insight
- [ ] Select quick start example: complete runnable workflow with imports
- [ ] Verify hook and quick start demonstrate DIFFERENT aspects (no duplication)

### Ecosystem Mapping
- [ ] List related packages in zoobzio org
- [ ] List packages that depend on or extend this one
- [ ] Note integration points with external tools

### Capabilities Inventory
- [ ] List primary features (one line each)
- [ ] Map features to documentation locations
- [ ] Flag experimental or advanced features

## Phase 2: Synthesis

- [ ] Draft tagline (essence in one phrase)
- [ ] Confirm tagline with user
- [ ] Draft hook section name (MUST be library-specific, NOT generic)
- [ ] Confirm hook section name with user
- [ ] Draft "Why [Name]?" bullet points (3-5 concrete benefits)
- [ ] Confirm bullet points with user

## Phase 3: Writing

### Execute
- [ ] Write README per SKILL.md specifications

### Verify Badges
- [ ] Badge 1 present: CI Status (GitHub Actions URL)
- [ ] Badge 2 present: codecov (codecov.io graph URL)
- [ ] Badge 3 present: Go Report Card (goreportcard.com URL)
- [ ] Badge 4 present: CodeQL (GitHub Actions URL)
- [ ] Badge 5 present: Go Reference (pkg.go.dev URL)
- [ ] Badge 6 present: License (shields.io github/license URL)
- [ ] Badge 7 present: Go Version (shields.io go-mod-go-version URL)
- [ ] Badge 8 present: Release (shields.io github/v/release URL)
- [ ] All badge URLs match specification exactly (only [package] replaced)
- [ ] No shields.io substitutions for non-shields.io badges

### Verify Structure
- [ ] Section 1: Header with title, all 8 badges, tagline + supporting sentence
- [ ] Section 2: Hook with library-specific name (not generic)
- [ ] Section 3: Install with go get command
- [ ] Section 4: Quick Start with complete runnable example
- [ ] Section 5: Capabilities table with doc links
- [ ] Section 6: Why [Name]? with 3-5 bullet points
- [ ] Section 7: Ecosystem with library-specific name (or omitted if no related projects)
- [ ] Section 8: Documentation links (Learn / Guides / Reference)
- [ ] Section 9: Contributing (one sentence + link)
- [ ] Section 10: License (single line)

### Verify Code Blocks
- [ ] Hook block shows the insight (attention-grabbing)
- [ ] Quick Start block shows the workflow (complete, runnable)
- [ ] No duplicate examples between Hook and Quick Start

### Verify Prohibitions
- [ ] No alternative badge providers used
- [ ] No generic hook names used
- [ ] No duplicate examples between Hook and Quick Start
- [ ] No API reference tables in README
- [ ] No "Problem / Solution" framing
- [ ] No feature laundry lists
- [ ] README has unique voice (not template-sounding)
