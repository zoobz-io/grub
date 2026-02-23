# Zoobzio Philosophy

How we build software.

## Dependencies

### Production: Minimal by Default

Most of the time, we don't need code other people wrote.

Before adding a dependency:
- Can stdlib do this?
- Can a zoobzio package do this?
- Is this worth the maintenance burden?

When we do need external code, it's a deliberate decision with clear justification.

### Test: Separate from Production

Test dependencies don't ship. Keep them isolated:
- Build tags: `//go:build testing`
- Test helpers in `testing/`
- Never import test utilities in production code

A package with zero production dependencies can have test dependencies. That's fine. They don't affect users.

### Providers: Isolated Submodules

When a package supports multiple backends (redis, postgres, s3), each provider is a submodule. Users import only what they use. The core package stays minimal.

## Type Safety

Use generics. Avoid `interface{}`. Catch errors at compile time.

Type parameters capture intent, enable IDE support, and cost nothing at runtime.

## Boundaries

Data transformations happen at boundaries. Make them explicit.

- Receiving external input
- Loading from storage
- Storing to persistence
- Sending external output

Each boundary is a chance to validate, transform, or redact.

## Composition

One interface per abstraction level. Everything implements it. Things nest.

Immutable processors, mutable connectors. Values are simple and testable. Connectors manage state.

## Errors

Errors are data. They carry context: what failed, where, why.

Semantic errors (`ErrNotFound`, `ErrDuplicate`) are consistent across implementations.

## Observability

Give things identity. Emit signals. Enable correlation without requiring external infrastructure.

## Context

Every I/O operation accepts `context.Context`. Timeouts, cancellation, and scoping are universal.
