# Security Review

Identify security vulnerabilities, attack vectors, and the gap between actual security posture and the appearance of security.

## Principles

1. **The appearance of security is not security** — Validation that doesn't cover all paths. Auth checks that can be bypassed. Crypto that uses the wrong algorithm. Find the performance.
2. **Every input is hostile** — Until proven otherwise, every boundary is an injection vector.
3. **Dependencies are attack surface** — Every external package is code you didn't audit running in your process.
4. **When in doubt, report it** — A finding that requires cross-domain validation to confirm is still a finding worth reporting. Case and Molly will validate from their domains.

## Execution

1. Read `checklist.md` in this skill directory
2. Run `govulncheck ./...` for known dependency vulnerabilities
3. Run `gosec ./...` for static security analysis
4. Manual review per domain checklist
5. Produce raw findings report

**NOTE:** This report goes to Case + Molly for cross-domain validation, NOT directly to Armitage. They confirm findings from structural and testing perspectives before anything reaches Armitage.

## Domains

### Input Handling
- Injection vectors (SQL, command, template, path traversal)
- Input validation completeness (all paths, not just the obvious ones)
- Deserialization of untrusted data
- File upload/path handling
- Integer overflow/underflow at boundaries

### Authentication & Authorization
- Authentication bypass vectors
- Authorization check completeness (every protected resource, every path)
- Session/token handling
- Privilege escalation paths
- Default credentials or bypass conditions

### Cryptography
- Algorithm choices (weak, deprecated, misused)
- Key management (hardcoded, weak generation, improper storage)
- Random number generation (crypto/rand vs math/rand)
- Hash collision resistance
- TLS configuration

### Information Leakage
- Error messages that reveal internals (stack traces, file paths, SQL)
- Debug output in production paths
- Logging sensitive data (credentials, tokens, PII)
- Timing side channels in auth comparisons

### Dependency Security
- Known vulnerabilities via `govulncheck`
- Supply chain concerns (typosquatting, abandoned packages)
- Transitive dependency risk
- Dependency freshness and maintenance status

### Concurrency
- Race conditions with security implications
- TOCTOU vulnerabilities (time-of-check-time-of-use)
- Shared state mutations under concurrent access
- Lock ordering violations that could enable deadlock-based DoS

### Configuration
- Default credentials or secrets
- Insecure defaults (permissive CORS, disabled auth, debug mode)
- Missing security headers/controls
- Development settings in production paths
- Hardcoded secrets or API keys

## Output

### Finding Format

Each finding uses a sequential ID and structured format:

```markdown
## Raw Findings

| ID | Domain | Severity | Confidence | Location | Description |
|----|--------|----------|------------|----------|-------------|
| SEC-001 | [domain] | [critical/high/medium/low/info] | [high/medium/low] | [file:line] | [what's wrong] |

### SEC-001: [Title]

**Domain:** [Input | Auth | Crypto | Leakage | Dependency | Concurrency | Config]
**Severity:** [Critical | High | Medium | Low | Informational]
**Confidence:** [High | Medium | Low]
**Location:** [file:line range]
**Description:** [What the vulnerability is]
**Attack Vector:** [How it could be exploited]
**Evidence:** [Code snippet, tool output, or structural observation]
**Recommendation:** [How to fix it]
```

### Confidence Levels

- **High** — Tool confirmed, code path verified, exploitation is straightforward
- **Medium** — Structural concern, code path exists but exploitation requires specific conditions
- **Low** — Theoretical concern, may not be applicable in this context

Low-confidence findings warrant investigation. Case and Molly validate from their domains — structural reachability and test coverage respectively.
