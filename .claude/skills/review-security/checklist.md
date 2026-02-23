# Security Review Checklist

## Phase 1: Automated Scanning — What Do the Tools Find?

### govulncheck
- [ ] Run `govulncheck ./...`
- [ ] Record all findings with CVE numbers
- [ ] Note affected packages and versions
- [ ] Each finding is a SEC finding (high confidence)

### gosec
- [ ] Run `gosec ./...`
- [ ] Record all findings by category
- [ ] Note any nosec directives — each is suspect
- [ ] Each finding is a SEC finding (confidence varies)

### Static Analysis
- [ ] Review golangci-lint security linter output (gosec, errorlint, noctx, bodyclose, sqlclosecheck)
- [ ] Any suppressed security findings?

## Phase 2: Input Handling — Where Are the Injection Vectors?

### SQL Injection
- [ ] Any string concatenation in SQL queries?
- [ ] All user input parameterized?
- [ ] ORM queries with raw string input?

### Command Injection
- [ ] Any calls to `os/exec` with user-controlled input?
- [ ] Any calls to `os.StartProcess` with unsanitized args?
- [ ] Shell commands constructed from strings?

### Template Injection
- [ ] HTML templates using `text/template` instead of `html/template`?
- [ ] Template data not escaped?
- [ ] User input in template names or paths?

### Path Traversal
- [ ] File paths constructed from user input?
- [ ] Missing `filepath.Clean` or path sanitization?
- [ ] Symlink following that could escape intended directory?

### Deserialization
- [ ] JSON/XML/YAML deserialization of untrusted data?
- [ ] Custom unmarshalers that don't validate?
- [ ] Large payload handling (DoS via allocation)?

### Input Validation
- [ ] All input boundaries have validation?
- [ ] Validation covers all code paths — or just the main one?
- [ ] Integer boundaries checked (overflow, negative)?
- [ ] String length limits enforced?

## Phase 3: Authentication & Authorization — Where Can Access Be Bypassed?

### Authentication
- [ ] Auth checks present on all protected endpoints?
- [ ] Auth checks can't be bypassed by path manipulation?
- [ ] Token validation is complete (signature, expiry, issuer)?
- [ ] Failed auth returns generic errors (no user enumeration)?

### Authorization
- [ ] Authorization checked after authentication?
- [ ] Object-level authorization (can this user access this specific resource)?
- [ ] Privilege escalation paths (can a user become admin)?
- [ ] Default-deny policy (new endpoints are protected by default)?

### Session/Token
- [ ] Tokens have reasonable expiry?
- [ ] Token revocation is possible?
- [ ] Refresh token rotation?
- [ ] Secure token storage guidance?

## Phase 4: Cryptography — Where Is Crypto Misused?

### Algorithm Choices
- [ ] No MD5 or SHA1 for security purposes?
- [ ] No DES, RC4, or other deprecated algorithms?
- [ ] AES with appropriate mode (GCM, not ECB)?
- [ ] RSA key sizes >= 2048 bits?

### Key Management
- [ ] No hardcoded keys or secrets in source?
- [ ] Keys generated with appropriate randomness?
- [ ] Key rotation possible?
- [ ] Private keys not logged or exposed in errors?

### Random Numbers
- [ ] `crypto/rand` used for security purposes — not `math/rand`?
- [ ] Random values are appropriate length?
- [ ] No predictable seeds?

### Timing
- [ ] Authentication comparisons use `subtle.ConstantTimeCompare`?
- [ ] No early-return patterns that leak info via timing?

## Phase 5: Information Leakage — What Does the Code Reveal?

### Error Messages
- [ ] Error responses to external callers sanitized?
- [ ] Stack traces not exposed to callers?
- [ ] File paths not exposed to callers?
- [ ] SQL queries not exposed in errors?
- [ ] Internal architecture not revealed in errors?

### Logging
- [ ] Passwords/tokens not logged?
- [ ] PII not logged without purpose?
- [ ] Request/response bodies not logged in full (may contain secrets)?

### Debug Output
- [ ] No debug/development output in production paths?
- [ ] No commented-out debug code that could be re-enabled?
- [ ] Build tags properly separate debug from production?

## Phase 6: Dependency Security — What Have We Imported?

### Known Vulnerabilities
- [ ] govulncheck results reviewed (Phase 1)
- [ ] All CVEs assessed for applicability
- [ ] Affected code paths identified

### Supply Chain
- [ ] Dependencies come from known, maintained sources?
- [ ] No typosquat-risk package names?
- [ ] Dependencies actively maintained (recent commits, releases)?
- [ ] Minimal transitive dependency tree?

### Dependency Hygiene
- [ ] go.sum present and committed?
- [ ] Module proxy configured?
- [ ] No `replace` directives pointing to unexpected locations?

## Phase 7: Concurrency — Where Do Race Conditions Have Security Impact?

### Race Conditions
- [ ] Shared state accessed under proper synchronization?
- [ ] Auth state not subject to race conditions?
- [ ] Session state not subject to race conditions?

### TOCTOU
- [ ] File operations: check-then-use patterns?
- [ ] Permission checks: gap between check and action?
- [ ] Resource availability: check-then-allocate patterns?

### Denial of Service
- [ ] Unbounded goroutine creation from user input?
- [ ] Unbounded allocation from user input?
- [ ] Missing timeouts on operations?
- [ ] Lock contention that could be exploited?

## Phase 8: Configuration — Where Are the Insecure Defaults?

### Secrets
- [ ] No hardcoded credentials in source?
- [ ] No API keys in source?
- [ ] No default passwords?
- [ ] Environment variable names suggest secrets are externalized?

### Defaults
- [ ] TLS enabled by default (not opt-in)?
- [ ] Auth enabled by default (not opt-out)?
- [ ] Debug mode disabled by default?
- [ ] Permissive CORS not default?

### Development vs Production
- [ ] Clear separation between dev and prod config?
- [ ] Dev-only features behind build tags?
- [ ] No development endpoints in production builds?
