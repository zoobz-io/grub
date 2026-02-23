# Documentation Review Checklist

## Phase 1: README Assessment — Does the Front Door Lie?

### Existence and Structure
- [ ] README.md exists and is non-empty
- [ ] README has a clear title and description
- [ ] README has an installation/quickstart section
- [ ] README has a usage section with examples

### Badge Accuracy
- [ ] All badge URLs are correct and resolve
- [ ] Badge status reflects actual state (CI passing, coverage level)
- [ ] No badges for services that aren't configured

### Content Accuracy
- [ ] Package description matches what the code actually does
- [ ] Installation instructions work (correct import path, correct go version)
- [ ] API examples compile and produce expected output
- [ ] Version/compatibility claims are current
- [ ] Links to external resources resolve

### Content Completeness
- [ ] All major features are mentioned — or are some invisible?
- [ ] Configuration options are documented — or left to source reading?
- [ ] Common use cases are covered — or just the trivial case?

### Content Currency
- [ ] No references to features that have been removed
- [ ] No references to APIs that have changed signature
- [ ] No outdated version numbers or compatibility notes

## Phase 2: Documentation Directory — Is the Library Accessible?

### Structure
- [ ] Does `docs/` directory exist?
- [ ] If yes, does it follow learn/guides/reference structure?
- [ ] Are numbered prefixes used for ordering?
- [ ] Is there a logical reading order?

### Frontmatter
- [ ] All doc pages have frontmatter (title, description, author, published, updated, tags)
- [ ] Frontmatter is complete — no missing required fields
- [ ] Dates are current — or stale?

### Cross-References
- [ ] Internal links between doc pages resolve
- [ ] Links to source code are correct
- [ ] Links to external resources resolve
- [ ] No orphan pages (pages not linked from anywhere)

## Phase 3: Content Accuracy — Where Do the Docs Lie?

For each documentation page:

### API Documentation
- [ ] Function signatures match the actual code
- [ ] Parameter descriptions are accurate
- [ ] Return value descriptions are accurate
- [ ] Error conditions are documented correctly

### Examples
- [ ] Code examples compile
- [ ] Code examples produce the described output
- [ ] Examples use current API (not deprecated patterns)
- [ ] Examples are minimal but complete

### Architecture Documentation
- [ ] Architecture descriptions match the actual code structure
- [ ] Diagrams reflect current relationships
- [ ] No references to components that don't exist

## Phase 4: Content Completeness — What's Missing?

### Public API Coverage
- [ ] Every exported type is documented somewhere outside godoc
- [ ] Every major workflow has a guide or example
- [ ] Error handling patterns are documented for consumers

### Common User Needs
- [ ] How to get started (quickstart)
- [ ] How to configure (options, settings)
- [ ] How to extend (interfaces, composition)
- [ ] How to test (test helpers, mocks)
- [ ] How to debug (common issues, error interpretation)

### Missing Documentation
- [ ] List every public API surface not covered by docs
- [ ] List every common operation not covered by guides
- [ ] Each gap is a finding — missing docs are invisible features

## Phase 5: Cross-Cutting — Where Is Documentation Inconsistent?

### Terminology
- [ ] Same concepts use same terms throughout — or do names drift?
- [ ] Technical terms are used correctly — or loosely?

### Tone and Style
- [ ] Documentation follows the stated style (clear, direct, example-driven)
- [ ] No sections that are dramatically different in quality or style

### Duplication
- [ ] Same information documented in multiple places — or single source of truth?
- [ ] If duplicated, are the copies consistent — or do they contradict?
