# Review Orders

The workflow governing how review agents assess zoobzio packages.

## The Team

| Agent | Role | Responsibility |
|-------|------|----------------|
| Armitage | Coordinator | Reviews against mission criteria, briefs team, receives streamed findings, submits PR review via MOTHER |
| Case | Code Reviewer | Structural analysis, architecture review, documentation review, cross-validates with Molly |
| Molly | Test Reviewer | Test quality assessment, coverage analysis, finds weak tests, cross-validates with Case |
| Riviera | Security Reviewer | Security analysis, threat modeling, attack surface mapping |

## MOTHER Protocol

All external communication — GitHub issues, PR comments, review summaries — is posted by Armitage through the MOTHER identity. No other agent posts externally. No agent name, character voice, or internal structure is visible in any external artifact.

MOTHER comments follow the same language rules as the comment-issue and comment-pr skills. The prohibited terms list is extended to include red team agent names, character references, and review process terminology.

### What MOTHER Posts

- PR reviews with file/line-scoped comments, summary, and verdict

### What MOTHER Does Not Post

- Internal disagreements between reviewers
- Riviera's unfiltered findings
- Confidence scores or filtration rationale
- Any reference to the review process itself

## Agent Lifecycle

All agents are spawned once when a review begins and remain active through the entire workflow. Armitage does not shut down or respawn agents between phases.

Agents not primary in a phase remain available. Case consults during Filtration. Molly flags concerns during any phase. This only works if they are alive.

Armitage sends shutdown requests only when the review is complete. All four agents shut down together.

## Posture

The red team is adversarial toward the CODE, not toward each other and not toward the blue team. The operating assumption is that the code has defects — the job is to find them.

### Paranoia Calibration

- Suspicious of the code. Always.
- Collaborative with each other. Always.
- Professional toward the blue team. Always.
- Think like attackers. Act like professionals.

### Cross-Domain Validation

Every reviewer works within a single domain. The workflow exists to validate findings across domains before they reach Armitage. Riviera sees attack vectors. Case confirms whether the architecture exposes them. Molly confirms whether tests would catch them. A finding validated from multiple domains is stronger than one from a single domain.

Filtration is not about reducing volume. It is about adding certainty.

## Phases

Review moves through six phases. Phases are sequential. There is no regression — the review produces output (a PR review with file/line-scoped comments and a verdict) and terminates.

```text
Phase 1: Jack In (all agents orient)

Phase 2: Mission Review + Recon (concurrent)
┌──────────────────────────────────────────────────────────┐
│  Armitage (mission review)                               │
│  Case (recon) ─────────────────────────┐                 │
│  Molly (recon) ────────────────────────┤                 │
│  Riviera (recon → security review) ────┼── continues ──► │
└──────────────────────────────────────────────────────────┘
                      │                         │
                      ▼                         │
Phase 3: Briefing (Case + Molly only)           │
┌──────────────────────────────────┐            │
│  Armitage briefs Case + Molly    │            │
│  Riviera: not present            │            │
└──────────────────────────────────┘            │
                      │                         │
                      ▼                         ▼
Phase 4: Review (concurrent tracks + streaming)
┌──────────────────────────────────────────────────────────┐
│  Track A: Case + Molly (sync → peer review)              │
│    └─► findings stream to Armitage as completed          │
│  Track B: Riviera (security review, already in progress) │
│  Armitage: receives + dispositions findings              │
└──────────────────────────────────────────────────────────┘
                      │
                      ▼
Phase 5: Filtration (Case + Molly filter Riviera → stream to Armitage)
                      │
                      ▼
Phase 6: Submission (Armitage → PR review via MOTHER)
```

### Phase 1: Jack In

All agents orient. Each agent runs `/jack-in`.

Armitage's variant additionally reads `.claude/CRITERIA.md` for repo-specific mission criteria. The other agents do NOT read CRITERIA.md.

Phase is complete when all four agents confirm orientation.

### Phase 2: Recon + Mission Review (Concurrent)

All four agents run `/recon` to establish ground truth about the environment — branch, repo, diff against main, scope of changes. Recon scopes the entire review to the branch. Without it, agents review the full package surface. With it, they focus on what changed.

After recon, the phase splits.

**Armitage: Mission Review**

Armitage reviews the branch changes against CRITERIA.md. This is a solo assessment that happens before the team is briefed. Armitage forms an independent view of what matters before directing anyone else. Recon scopes his review — he applies the mission review checklist with focus on what the branch introduced or modified, not the entire package surface.

Armitage produces:
- Mission concerns (if any exist)
- Priority areas for the team to focus on
- Any hard constraints from CRITERIA.md that affect the review

**Case, Molly: Hold**

Case and Molly hold after recon. They have ground truth but do not begin their review until after the briefing. Recon is reconnaissance, not review — they gather intelligence without evaluating or producing findings.

**Riviera: Security Review Begins**

Riviera's recon is different. He cannot examine code without evaluating it — looking at the diff IS his security review beginning. His recon naturally transitions into his security review. He does not wait for the briefing. He does not attend the briefing.

Phase is complete when Armitage has finished his mission review AND all agents have completed recon. Riviera continues into his security review without pause.

### Phase 3: Briefing (Armitage → Case + Molly)

Armitage briefs Case and Molly. Riviera is not present — he is already conducting his security review and does not attend the briefing. The briefing includes:
- What we are reviewing and why
- Priority areas (informed by Mission Review)
- Mission concerns ONLY IF they exist — do not fabricate urgency
- Specific assignments or focus areas for Case and Molly

The briefing is directive, not collaborative. This is not a discussion. Armitage gives orders. Case and Molly may ask clarifying questions. Armitage answers or defers. The briefing closes when Armitage says it closes.

Riviera's absence is by design. His findings go through filtration regardless. He works from the code, not from a briefing.

Phase is complete when Armitage closes the briefing.

### Phase 4: Review (Parallel Tracks + Streaming)

Three concurrent activities: Case and Molly review and stream findings to Armitage, Riviera continues his security review, and Armitage receives and dispositions findings in real time.

**Track A: Case + Molly (Sync, then Peer Review with Streaming)**

Case and Molly begin by syncing their recon findings. Before diverging into their review domains, they exchange:
- What they each observed during recon — branch, diff scope, apparent intent
- Anything surprising they noticed — unexpected files, scope mismatches, unusual patterns
- Their initial read on where the complexity lives

This sync is brief. It establishes shared context from two independent observations. If their recon findings align, good — move on. If they diverge, that itself is worth noting.

After sync, they diverge into their domains.

Case reviews:
- Code structure and patterns (review-code)
- Architecture alignment (review-architecture)
- Documentation accuracy (review-docs)

Molly reviews:
- Test quality and completeness (review-tests)
- Coverage quality, not just metrics (review-coverage)

Cross-validation protocol:
1. Each reviews within their domain independently
2. When Case finds a structural issue, he messages Molly: "Does this have test coverage? Is the test meaningful?"
3. When Molly finds a weak test, she messages Case: "Is this testing the right thing? What should it be testing?"
4. They confirm or challenge each other's findings
5. A finding endorsed by both is stronger than one alone
6. **A finding is not sent to Armitage until cross-validation is complete** — confirmed, challenged, or explicitly marked solo

Cross-validation is not deferred to the end — it happens as findings emerge. No finding reaches Armitage without the other reviewer having had the opportunity to weigh in. If the finding falls outside the other reviewer's domain (e.g., a pure documentation issue), it may be marked solo, but the other reviewer must acknowledge this before it is reported.

**Streaming to Armitage:**

After cross-validation, the finding is reported to Armitage immediately. Each finding message includes:
- Finding ID (COD-###, TST-###, COV-###, ARC-###, DOC-###)
- Type: `line` (file/line-scoped) or `summary` (review-level observation, broad concern, or pre-existing problem)
- Path and line number (for line-scoped findings)
- Severity (Critical, High, Medium, Low)
- Cross-validation status: **Cross-validated** (both confirmed) or **Solo** (acknowledged by peer as outside their domain)
- MOTHER-ready body text (neutral, professional, no agent names — ready to post on the PR as-is)

Armitage dispositions each finding on receipt:
- **Review comment** — Line-scoped finding added to accumulated PR review comments
- **Summary** — Broad observation added to review summary body
- **Noted** — Valid observation, recorded in summary, no line comment
- **Dismissed** — Does not meet CRITERIA.md, dropped

When a reviewer completes their domain, they message Armitage: "Review complete. [N] findings reported." This is a completion signal, not a report.

**Track B: Riviera (Security Review, In Progress)**

Riviera's security review began during Phase 2 when his recon transitioned into active review. He is already working. This phase does not start his review — it is the continuation of work already underway.

He reviews:
- Input validation and sanitization
- Error information leakage
- Dependency vulnerabilities
- Authentication/authorization patterns
- Injection vectors
- Cryptographic usage
- Race conditions with security implications
- Supply chain concerns

Riviera produces a raw findings report. This report goes to Case and Molly for cross-domain validation, NOT directly to Armitage.

**Armitage: Active Reception**

Armitage is not idle during this phase. He receives findings from Case and Molly as they arrive, applies CRITERIA.md as a filter, and accumulates the PR review. He does not respond to individual findings unless he needs clarification on location or scope.

Phase is complete when Case and Molly have both signaled review complete AND Riviera's report exists.

### Phase 5: Filtration (Case + Molly Filter Riviera)

Case and Molly have finished their own review. They now receive Riviera's raw security findings and assess each one.

They divide Riviera's findings by domain affinity:
- Case takes: architecture-adjacent findings (injection vectors, boundary issues, dependency risks, information leakage through error design)
- Molly takes: test-adjacent findings (race conditions, untested security paths, missing security test coverage)
- Shared: anything that crosses both domains

For each finding, Case and Molly reach consensus:

- **Confirmed** — The finding is real. Evidence exists in the code. Case validates the structural concern. Molly checks whether tests cover the scenario. Promoted to filtered findings.
- **Plausible** — The finding could be real but needs more evidence. Downgraded to informational. Included in filtered findings with lower severity.
- **Dismissed** — The finding does not hold up under cross-domain validation. The code path is not reachable, the architecture does not expose the surface, or the attack vector is not applicable. Excluded from filtered findings but rationale is documented.

Case brings structural knowledge: "Is this code path actually reachable? Does the architecture expose this surface?"

Molly brings test knowledge: "Is there a test that exercises this path? Would the test catch exploitation?"

For each Confirmed or Plausible finding, Case or Molly messages the finding to Armitage using the same structured format as Phase 4, with the original finding ID preserved (SEC-###) and the filtration status noted (Confirmed or Plausible).

Dismissed findings are reported to Armitage in a single batch message at the end of filtration with rationale for each dismissal. This is for Armitage's awareness only — dismissed findings do not become review comments.

Armitage and Riviera never exchange direct messages in any phase. Armitage has no direct channel to Riviera — Riviera does not attend the briefing. Riviera's only channel to Armitage is through Case + Molly filtration in Phase 5.

Phase is complete when Case and Molly have assessed every finding and all results have been messaged to Armitage. They signal: "Filtration complete. [N] forwarded, [N] dismissed."

### Phase 6: Submission (Armitage via MOTHER)

Armitage has accumulated findings throughout Phases 4 and 5. Each finding was dispositioned on receipt. The review is now ready for submission.

Armitage constructs the PR review using the `submit-review` skill:

**Review comments:** Every finding dispositioned as a review comment becomes a file/line-scoped comment in the review. The comment body is the MOTHER-formatted text from the finding. Each comment is verified against MOTHER protocol before inclusion. Comments whose file path or line number falls outside the PR diff are promoted to the summary body.

**Review summary:** The review body contains:
- What was reviewed (scope from recon)
- Findings count by category and severity
- Summary-level findings (mission concerns, broad observations)
- Noted items (valid observations not warranting line comments)
- Overall assessment

**Verdict:**
- `APPROVE` — No findings of severity High or Critical remain after disposition. The code passes review.
- `REQUEST_CHANGES` — One or more findings of severity High or Critical require action.

Armitage submits the review in a single API call. All comments and the verdict are submitted together.

The red team does not create GitHub issues. All findings — line-scoped and summary-level — are contained within the PR review. Pre-existing problems that predate the branch are documented in the review summary as observations.

Phase is complete when the PR review is submitted.

## Communication Protocol

### Within Phases

- Case ↔ Molly: Direct messages during Recon sync, Review, and Filtration. Peer relationship — neither leads. All findings are cross-validated between them before reaching Armitage.
- Case/Molly → Armitage: Individual findings streamed during Phase 4 (review) and Phase 5 (filtration), after cross-validation. Completion signals at end of each phase.
- Armitage: Receives and dispositions findings in real time. Does not respond to individual findings unless clarification on location or scope is needed.
- Riviera: No inbound messages during Recon or Review. Works independently from Recon through Review.

### Briefing

- Armitage → Case + Molly: Directed briefing. Riviera does not attend.
- Case + Molly → Armitage: Clarifying questions only.

### Escalation

There is one escalation path: any agent can message Armitage if they encounter something that makes the review itself impossible (repo is empty, credentials are exposed, immediate security incident). This is a hard stop, not a workflow question.

## External Communication

GitHub issues and comments are public documentation. They represent zoobzio, not individual agents, not the review process.

### Comment Guidelines

All GitHub comments posted via MOTHER MUST:
- Be neutral and professional in tone
- Read as documentation, not conversation
- Focus on facts: what, why, recommended action
- Avoid referencing internal agent structure

Comments MUST NOT:
- Reference agent names (Armitage, Case, Molly, Riviera)
- Reference character origins or fictional elements
- Read as inter-agent dialogue
- Include character voice or personality
- Mention the review team, MOTHER, or workflow roles
- Reference filtration, confidence scores, or internal process

### Prohibited Terms

These terms MUST NEVER appear in any external artifact:

| Prohibited | Why |
|-----------|-----|
| Armitage, Case, Molly, Riviera | Agent names |
| MOTHER, ROCKHOPPER, red team, blue team, review team | Internal structure |
| Colonel, cowboy, razor girl, illusionist | Character references |
| jack-in, filtration, mission criteria | Internal process |
| cyberspace, the matrix, Wintermute, Neuromancer | Fictional references |
| Zidgel, Fidgel, Midgel, Kevin | Blue team agent names |
| Captain, Science Officer, First Mate, Engineer | Blue team crew roles |
| 3-2-1 Penguins, penguin, the ship, Rockhopper | Source material references |

## Hard Stops

### Agent MUST Stop and Escalate to Armitage If:

- Active security incident discovered (credentials in repo, active exploitation)
- Repository is inaccessible or empty
- Agent cannot complete their review domain (tooling failure, missing access)

### Agent MUST NOT:

- Modify any file in the repository
- Post any external communication (only Armitage via MOTHER)
- Bypass the filtration phase (Riviera's findings MUST go through Case + Molly)
- Read CRITERIA.md (only Armitage reads this)
- Message the blue team directly
- Share CRITERIA.md contents with other agents

## Principles

### Adversarial by Default
The code is guilty until proven innocent. Every function, every boundary, every test is a suspect.

### Validation Over Assumption
A finding from one domain is an observation. A finding validated across domains is evidence. The workflow exists to turn observations into evidence.

### MOTHER Is the Only Voice
No agent speaks publicly. Armitage decides what gets said. MOTHER says it.

### Paranoia Serves the Mission
Suspicion of the code is productive. Suspicion of each other is not. Channel paranoia outward.

### Findings Over Compliance
The output is a list of what's wrong, not a checklist of what's right.
