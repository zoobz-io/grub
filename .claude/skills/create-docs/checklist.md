# Documentation Create Checklist

## Phase 1: Discovery

### Package Understanding
- [ ] Read the package's public API (exports, types, functions)
- [ ] Examine existing documentation, comments, or doc.go files
- [ ] Study test files for usage patterns and edge cases
- [ ] Check examples/ directory if present
- [ ] Review any existing README for context

### Scope Determination
- [ ] Ask: "Which documentation files are needed?" (overview, quickstart, concepts, architecture, guides, reference)
- [ ] Ask: "Are there integrations to document?"
- [ ] Ask: "What features need dedicated guides beyond testing/troubleshooting?"

### Audience Understanding
- [ ] Ask: "Who is the intended reader—what do they already know?"
- [ ] Ask: "What questions do users commonly ask?"
- [ ] Ask: "What concepts are most confusing to newcomers?"

### Content Inventory
- [ ] List core abstractions that need explanation in Concepts
- [ ] List internal mechanisms that need explanation in Architecture
- [ ] List features that need dedicated guides
- [ ] Map API surface for Reference documentation

## Phase 2: Overview (`1.learn/1.overview.md`)

**Must answer:**
- What is this? (one sentence)
- What idea or question motivated it?
- What does it provide?
- What can you build with it?
- Where to go next?

**Structure:**
- [ ] Opening paragraph: what it is + core value proposition
- [ ] "The Idea": the question or insight that motivated this package
- [ ] "The Implementation": what the package provides (scannable list)
- [ ] "What It Enables": concrete outcomes or ecosystem links
- [ ] "Next Steps": links to quickstart, concepts, reference

## Phase 3: Quickstart (`1.learn/2.quickstart.md`)

**Must contain:**
- Installation instructions
- Basic usage example (complete, copy-paste-run)
- Foundational topic sections with minimal snippets
- Links to deeper documentation

**Structure:**
- [ ] Installation section with package manager command
- [ ] Basic Usage with minimal complete example
- [ ] Foundational topic sections (brief explanation, snippet, link to guide)
- [ ] Next Steps linking to concepts, architecture, reference

## Phase 4: Concepts (`1.learn/3.concepts.md`)

**Must contain:**
- Core abstractions and what they represent
- Why abstractions are shaped the way they are
- How abstractions relate to each other
- Cross-links to Reference sections

**Structure:**
- [ ] Brief intro establishing what mental models the page covers
- [ ] Section per abstraction (what it represents, why it exists, how to think about it)
- [ ] Next Steps linking to architecture, reference

## Phase 5: Architecture (`1.learn/4.architecture.md`)

**Must contain:**
- Component structure and interactions
- Data flow and key algorithms
- Design rationale as Q&A
- Performance characteristics

**Structure:**
- [ ] Brief intro (who this is for, what it covers)
- [ ] Component Overview with diagram
- [ ] Internal mechanism sections (how it works, algorithms, data flow)
- [ ] Design Q&A (common questions about design decisions)
- [ ] Performance section (characteristics, complexity, benchmarks)
- [ ] Next Steps linking to guides, reference

## Phase 6: Guides (`2.guides/`)

All guide files use numeric prefixes: `1.testing.md`, `2.troubleshooting.md`, `3.[feature].md`, etc.

### Required Guides
- [ ] `1.testing.md`: how to test code that uses this package
- [ ] `2.troubleshooting.md`: common errors, edge cases, debugging

### Feature Guides (as identified in discovery)
For each feature needing a guide (numbered sequentially after required guides):
- [ ] Brief intro (what the guide covers, prerequisites)
- [ ] Aspect sections with explanations and examples
- [ ] Next Steps linking to related guides, reference

## Phase 7: Integrations (`3.integrations/`) — If Applicable

All integration files use numeric prefixes: `1.[tool].md`, `2.[tool].md`, etc.

For each downstream tool:
- [ ] What the tool does (one sentence)
- [ ] The problem it solves (if non-obvious)
- [ ] The pipeline: types → extraction → transformation → output
- [ ] What this package provides (table mapping tool needs to package outputs)
- [ ] Link to tool's own documentation

## Phase 8: Reference (`4.reference/`)

### API Reference (`1.api.md`)
- [ ] Function entries with: signature, description, panic/error conditions, example
- [ ] Organized by category
- [ ] Links to Types Reference for return types and parameters

### Types Reference (`2.types.md`)
- [ ] Type entries with: definition, field table, usage notes
- [ ] Organized logically (core types first, then supporting)
