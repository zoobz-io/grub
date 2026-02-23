# Ecosystem

Understand how this package fits within the zoobzio ecosystem and identify downstream impacts.

## Purpose

Before architecting changes, understand:
- How this package is consumed by other zoobzio packages
- What depends on the APIs being modified
- What breaking changes would affect
- How changes ripple through the ecosystem

## The Zoobzio Ecosystem

All zoobzio packages live at **github.com/zoobzio**. Check there for:
- Packages that import this one
- Patterns used across packages
- Shared conventions and standards

## Execution

1. Identify the public API surface of this package
2. Search github.com/zoobzio for consumers
3. Understand how consumers use the API
4. Identify impact of proposed changes
5. Document ecosystem context

## Discovery

### Find Consumers

Search GitHub for imports of this package:

```bash
# Search zoobzio org for imports
gh search code "github.com/zoobzio/[this-package]" --owner zoobzio
```

### Examine Consumer Usage

For each consumer:
- What functions/types do they use?
- How do they use them?
- Would proposed changes break them?

### Check Shared Patterns

Look at sibling packages for:
- Common conventions
- Shared infrastructure patterns
- Consistency expectations

## Output

```markdown
## Ecosystem Context

### Consumers
| Package | Uses | Impact of Changes |
|---------|------|-------------------|
| [package] | [what they use] | [none/minor/breaking] |

### Shared Patterns
- [Pattern observed across packages]

### API Surface at Risk
- [Function/type that consumers depend on]

### Recommendations
- [Guidance for maintaining compatibility]
- [Migration path if breaking]

### External Considerations
- [Non-zoobzio consumers if known]
```

## Breaking Change Protocol

If changes would break consumers:

1. Document what breaks
2. Identify all affected packages
3. Propose migration path
4. Flag in architecture plan
5. Consider versioning strategy

## What This Skill Does NOT Do

- Make the decision to break or not (Zidgel's call)
- Implement compatibility shims (Midgel's domain)
- Test consumer compatibility (Kevin's domain)

This provides context. Decisions follow.
