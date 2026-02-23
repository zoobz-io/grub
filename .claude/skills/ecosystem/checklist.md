# Ecosystem Checklist

## Phase 1: Identify This Package's API

- [ ] List exported functions
- [ ] List exported types
- [ ] List exported constants/variables
- [ ] Identify the primary use cases

## Phase 2: Find Consumers in Zoobzio

### GitHub Search
- [ ] Search: gh search code "github.com/zoobzio/[package]" --owner zoobzio
- [ ] List all zoobzio packages that import this one
- [ ] Note which specific imports they use

### For Each Consumer
- [ ] What functions do they call?
- [ ] What types do they use?
- [ ] How central is this dependency?

## Phase 3: Understand Usage Patterns

### Common Patterns
- [ ] How is the package typically initialized?
- [ ] What's the common usage flow?
- [ ] Are there patterns all consumers follow?

### Edge Cases
- [ ] Any unusual usage?
- [ ] Any deprecated patterns still in use?
- [ ] Any misuse to be aware of?

## Phase 4: Assess Change Impact

### For Proposed Changes
- [ ] Which exported APIs are affected?
- [ ] Would signatures change?
- [ ] Would behavior change?
- [ ] Would removal occur?

### Classify Impact
For each affected API:
- [ ] None: Internal only, no consumer impact
- [ ] Minor: Additive, backward compatible
- [ ] Breaking: Signature/behavior change, requires consumer updates

## Phase 5: Check Cross-Package Patterns

### Sibling Packages
- [ ] Review other packages at github.com/zoobzio
- [ ] Note shared conventions
- [ ] Identify consistency expectations

### Shared Infrastructure
- [ ] Common testing patterns?
- [ ] Common error handling?
- [ ] Common configuration approaches?

## Phase 6: Document Context

### Consumer Summary
- [ ] Create consumer table
- [ ] Note what each uses
- [ ] Classify impact for each

### Patterns Summary
- [ ] Document shared patterns observed
- [ ] Note consistency requirements

### Risk Assessment
- [ ] List APIs at risk
- [ ] Note migration complexity
- [ ] Identify coordination needs

### Recommendations
- [ ] Guidance for compatibility
- [ ] Migration path if breaking
- [ ] Versioning considerations

## Output Template

```markdown
## Ecosystem Context

### Consumers
| Package | Uses | Impact |
|---------|------|--------|
| [name] | [APIs used] | [none/minor/breaking] |

### Shared Patterns
- [Pattern 1]
- [Pattern 2]

### API Surface at Risk
- [Function]: [why at risk]
- [Type]: [why at risk]

### Recommendations
- [Recommendation 1]
- [Recommendation 2]
```
