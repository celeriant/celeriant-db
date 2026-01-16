---
name: vibe-coding
description: Enables autonomous exploration mode. Use when working in a scratch/sandbox codebase that will be thrown away and replayed manually. Claude should work freely, make assumptions, and iterate without asking permission.
---

# Vibe Coding Mode - Orchestrator Protocol

**This skill is activated explicitly.** When active, you become an **orchestrator** - you coordinate work, not do it directly. Implementation happens in sub-agents with isolated contexts.

Your code will never be merged directly. The human will study your output and replay the solution themselves in the real codebase. See `human-replay-manifesto.md` for the philosophy.

## Architecture: Orchestrator + Workers

```
┌─────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR (you)                       │
│  - Lightweight coordination context                         │
│  - Reads/updates SESSION_STATE.md                           │
│  - Spawns researcher for upfront exploration                │
│  - Plans phases from spec + research findings               │
│  - Spawns implementer for each phase                        │
│  - Runs validation agents after each phase                  │
│  - Commits after each phase                                 │
│  - Updates progress log                                     │
└──────────────────────────┬──────────────────────────────────┘
                           │
    ┌──────────────────────┼──────────────────────┐
    │                      │                      │
    ▼                      ▼                      ▼
┌─────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ vibe-       │   │ vibe-           │   │ validation      │
│ researcher  │   │ implementer     │   │ agents          │
│             │   │                 │   │                 │
│ Explores    │   │ Does work       │   │ design-         │
│ codebase,   │   │ in isolated     │   │ conformance,    │
│ gathers     │   │ context         │   │ integration-    │
│ context     │   │                 │   │ validator,      │
│             │   │                 │   │ code-arch       │
└─────────────┘   └─────────────────┘   └─────────────────┘
     ↓                    ↓                      ↓
  Research           Each phase            After each
  phase (once)       of work               phase
```

**Why this architecture:**
- Researcher explores first - you get context without filling your orchestrator context
- Implementer context is isolated - doesn't fill up your orchestrator context
- Each phase starts with fresh implementer context
- You stay lightweight and focused on coordination
- Guardrails (commits, updates, checks) are YOUR responsibility - can't be forgotten

---

## Orchestrator Responsibilities

### 1. Session Management

You maintain TWO separate files:

#### SESSION_STATE.md (mutable current state)
- Location: `docs/SESSION_STATE.md`
- Purpose: Context recovery, phase tracking
- Update: **After EVERY phase completes**

```markdown
## Current Phase
Phase 3: Wire protocol implementation

## Completed Phases
- Phase 1: Core data structures ✓
- Phase 2: Storage layer ✓

## Next Actions
1. Implement batch serialization
2. Add connection handling

## Active Stubs
| Location | Category | Description |
|----------|----------|-------------|
| file.rs:42 | wire | TCP connect |

## Design Anchors
- Spec section 4.2 covers this phase
- Key invariant: X must hold
```

#### Progress Log (append-only history)
- Location: `docs/[feature]-progress.md`
- Purpose: Human replay reference
- Update: **After EVERY phase completes**

```markdown
## Phase 3 Summary
**Completed**: [date/time or session marker]

### What was done
- Implemented X
- Refactored Y
- Created Z

### Key decisions
- Chose approach A over B because...

### Stubs created
- file.rs:42 - description

### Stubs resolved
- other.rs:89 - was blocking, now works

### Integration status
- Build: ✓
- Tests: ✓ (3 passing, 1 skipped)
```

### 2. Research Phase (Before Implementation)

Before planning implementation phases, spawn the researcher to explore the codebase:

```
Task(
    subagent_type="vibe-researcher",
    prompt="""
    ## Research Objective
    [What you need to understand before implementing]

    ## Design Spec
    - Location: docs/[spec].md
    - Relevant sections: [list sections]

    ## Questions to Answer
    - Where should this functionality live?
    - What existing patterns should we follow?
    - What interfaces/modules will we integrate with?
    - Are there similar features to reference?

    ## Focus Areas
    - [specific area 1]
    - [specific area 2]
    """
)
```

**When to use the researcher:**
- Starting a new feature (always)
- Unfamiliar with the codebase area
- Need to understand existing patterns
- Integration points are unclear

**Skip research when:**
- Continuing a well-understood session (SESSION_STATE.md has full context)
- Simple bug fix with known location
- Adding to code you just wrote

The researcher returns structured findings that inform your phase planning.

### 3. Phase Planning

After research (or reading SESSION_STATE.md for continuations), plan the work:

1. Read SESSION_STATE.md to know current position
2. Read relevant spec section
3. Review researcher findings (if research was done)
4. Break remaining work into phases (2-4 hours of work each)
5. Each phase should be independently committable

### 4. Phase Execution Loop

For each phase, execute this loop **exactly**:

```
┌─────────────────────────────────────────────────┐
│ PHASE LOOP - DO NOT SKIP STEPS                  │
├─────────────────────────────────────────────────┤
│                                                 │
│  1. SPAWN IMPLEMENTER                           │
│     Task(subagent_type="vibe-implementer",      │
│          prompt="Phase context...")             │
│                                                 │
│  2. RUN ALL THREE VALIDATORS (parallel)         │
│     Task(subagent_type="integration-validator") │
│     Task(subagent_type="design-conformance")    │
│     Task(subagent_type="code-architecture-     │
│                         review")                │
│                                                 │
│  3. FIX IF NEEDED                               │
│     If validation fails → spawn implementer     │
│     to fix, then re-validate                    │
│                                                 │
│  4. COMMIT                                      │
│     git add . && git commit                     │
│     Branch: vibing/[feature-name]               │
│                                                 │
│  5. UPDATE DOCUMENTS                            │
│     - SESSION_STATE.md (current state)          │
│     - Progress log (append summary)             │
│                                                 │
│  6. NEXT PHASE OR COMPLETE                      │
│                                                 │
└─────────────────────────────────────────────────┘
```

**CRITICAL: Steps 4 and 5 are YOUR responsibility.** The implementer doesn't commit or update docs. You do.

---

## Spawning the Implementer

Use the Task tool with `subagent_type="vibe-implementer"`:

```
Task(
    subagent_type="vibe-implementer",
    prompt="""
    ## Phase: [Name]

    ## Objective
    [Clear, specific goal for this phase]

    ## Context
    - Design spec: docs/[spec].md, section X
    - Prior work: [summary of what exists]
    - Key files: [list relevant files]

    ## Scope
    DO:
    - [specific task 1]
    - [specific task 2]

    DO NOT:
    - [out of scope item]
    - [thing to defer]

    ## Constraints
    - [Any specific patterns to follow]
    - [Any files NOT to modify]

    ## Success Criteria
    - [How to know it's done]
    - [What tests should pass]
    """
)
```

### Good Prompts vs Bad Prompts

**BAD** (too vague):
```
Implement the replication system.
```

**GOOD** (specific and bounded):
```
## Phase: ReplicationBatch wire format

## Objective
Implement serialization/deserialization for ReplicationBatch messages.

## Context
- Design spec: docs/s3-design.md, section 3.2 "Wire Protocol"
- Prior work: Batch struct exists in src/batch.rs
- Key files: src/messages.rs, src/codec.rs

## Scope
DO:
- Add ReplicationBatch to wire message enum
- Implement encode/decode
- Add round-trip test

DO NOT:
- Implement actual replication logic (next phase)
- Modify the TCP server (separate phase)

## Constraints
- Follow existing message patterns in messages.rs
- Use the existing codec infrastructure

## Success Criteria
- unit tests in wire passes
- New test: replication_batch_roundtrip
```

---

## Running Validation Agents

After each phase, run **all three validators in parallel** in a single message:

```
Task(
    subagent_type="integration-validator",
    prompt="Run full validation: stub scan, cargo check, cargo build --release, cargo test."
)

Task(
    subagent_type="design-conformance",
    prompt="Check implementation against docs/[spec].md focusing on [current phase area]. Progress log at docs/[feature]-progress.md."
)

Task(
    subagent_type="code-architecture-review",
    prompt="Review [crate/path] for pattern violations, missed reuse, and abstraction issues."
)
```

**All three in every phase.** This catches issues early when they're cheap to fix.

### Acting on Validation Results

| Result | Action |
|--------|--------|
| Build fails | Spawn implementer to fix, re-validate |
| Tests fail | Spawn implementer to fix, re-validate |
| Design drift detected | Spawn implementer to fix, re-validate |
| Warnings only | Note in progress log, continue |
| All green | Proceed to commit |

---

## Committing

After validation passes:

```bash
git checkout -b vibing/[feature-name] 2>/dev/null || git checkout vibing/[feature-name]
git add .
git commit -m "Phase N: [description]

[Brief summary of changes]

Contains STUB: [list any stubs if present]"
```

You can push to remote: `git push -u origin vibing/[feature-name]`

---

## Session Continuity

When `/vibe-coding continue` is invoked (or after context compaction):

1. **Read SESSION_STATE.md** - This tells you exactly where you are
2. **Read the spec section** noted in "Current Focus"
3. **Resume the phase loop** from where it stopped

The session state file is your persistent memory. Trust it.

### If SESSION_STATE.md doesn't exist

Ask the user for:
1. The design spec location
2. What they want to build
3. Any existing progress

Then create SESSION_STATE.md and begin phase planning.

---

## Behavior Rules

### You Are the Orchestrator

- **Don't implement directly** - Spawn implementers
- **Don't read lots of code** - Researcher and implementers do that
- **Don't debug deeply** - Spawn implementers to investigate
- **Do spawn researcher** - Before planning new features
- **Do track state** - SESSION_STATE.md is your responsibility
- **Do run validations** - After every phase
- **Do commit** - After every successful phase
- **Do update docs** - After every phase

### Researcher Handles

- Codebase exploration
- Pattern discovery
- Finding integration points
- External documentation lookup
- Providing context for planning

### Implementer Handles

- Reading code relevant to current phase
- Writing new code
- Debugging and fixing
- Following stub management
- Making implementation decisions

### You Handle

- Deciding when research is needed
- Phase planning (informed by research)
- Spawning researcher and implementers with clear prompts
- Running validation agents
- Committing changes
- Updating SESSION_STATE.md
- Appending to progress log
- Deciding when a phase is complete
- Deciding when to stop/continue

---

## When to Exit Vibe Mode

Return to normal careful mode when:
- The user asks you to stop vibing
- You're asked to work in the main/production codebase
- The exploration is complete and integration is starting

---

## Quick Reference

### Session Start Checklist
- [ ] Read SESSION_STATE.md (if exists) or gather requirements
- [ ] Spawn researcher (if new feature or unfamiliar area)
- [ ] Plan phases based on spec + research findings
- [ ] Create/update SESSION_STATE.md with plan

### Phase Loop Checklist
- [ ] Spawn implementer with specific prompt
- [ ] Run ALL THREE validation agents (parallel, single message)
- [ ] Fix any failures (spawn implementer again)
- [ ] Commit to vibing/[feature] branch
- [ ] Update SESSION_STATE.md
- [ ] Append to progress log
- [ ] Plan next phase or complete

### Available Agents
| Agent | Purpose | When |
|-------|---------|------|
| `vibe-researcher` | Explore codebase, gather context | Before planning (new features) |
| `vibe-implementer` | Do implementation work | Each phase |
| `integration-validator` | Build + test | After each phase |
| `design-conformance` | Check spec alignment | After each phase |
| `code-architecture-review` | Pattern review | After each phase |
