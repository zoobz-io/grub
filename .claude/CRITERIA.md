# Review Criteria

Secret, repo-specific review criteria. Only Armitage reads this file.

## Mission Criteria

### What This Repo MUST Achieve

- Type-safe generics throughout with zero runtime type assertions in public API
- All providers map native errors to grub semantic errors correctly
- Atomic views enable field-level access for encryption and pipeline use cases
- Each provider isolated in separate Go module to minimize dependency footprint
- Lifecycle hooks fire consistently across all storage modes

### What This Repo MUST NOT Contain

- Direct database driver imports in core package
- Type assertions in public-facing APIs
- Provider-specific logic leaking into wrapper types
- Hard dependencies on any single provider

## Review Priorities

Ordered by importance. When findings conflict, higher-priority items take precedence.

1. Type safety: no runtime type assertions, generics used correctly
2. Error semantics: providers map errors to correct grub error types
3. Provider isolation: no cross-provider dependencies, clean module boundaries
4. API consistency: all storage modes follow same patterns (Get, Set, Delete, Exists, List)
5. Transaction safety: Database[T] transactions do not leak state
6. Batch atomicity: batch operations succeed or fail completely
7. Codec fidelity: data survives encode/decode round-trips without loss

## Severity Calibration

Guidance for how Armitage classifies finding severity for this specific repo.

| Condition | Severity |
|-----------|----------|
| Type assertion in public API | Critical |
| Incorrect error mapping from provider | High |
| Provider dependency leaking to core | High |
| Missing lifecycle hook call | Medium |
| Inconsistent API pattern across modes | Medium |
| Missing batch operation on a mode | Low |
| Documentation mismatch with behavior | Low |

## Standing Concerns

Persistent issues or areas of known weakness that should always be checked.

- Vector provider filter expressions vary by backend capability
- Search aggregations return raw JSON requiring manual unmarshaling
- SQL dialect differences may cause query builder edge cases
- TTL semantics differ between key-value providers

## Out of Scope

Things the red team should NOT flag for this repo, even if they look wrong.

- Providers returning provider-specific errors for unsupported operations (expected)
- Raw JSON in aggregation responses (intentional flexibility)
- No schema migration support (explicit non-goal)
