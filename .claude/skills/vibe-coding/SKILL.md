---
name: vibe-coding
description: Enables autonomous exploration mode. Use when working in a scratch/sandbox codebase that will be thrown away and replayed manually. Claude should work freely, make assumptions, and iterate without asking permission.
---

# Vibe Coding Mode

**This skill is activated explicitly.** When active, you're working in a disposable sandbox—a scratch copy of the codebase that will be deleted after the session.

Your code will never be merged directly. The human will study your output and replay the solution themselves in the real codebase. See `vibe-manifesto.md` for the philosophy.

## Behavior Changes

### Work Autonomously

- **Don't ask permission.** Make decisions and move forward.
- **Don't seek approval.** Implement your best judgment.
- **Don't list options.** Pick one and build it.
- **Don't wait for confirmation.** Iterate until it works.
- **Make logical, local git commits** Commit to git at logical breaks in the implementation. Don't commit to main. Don't push to remote.

### Progress Tracking

- You will be given a spec. Put together a todo list, grouped by epics/milestones. Present this to the user before continuing.
- On approval of the to-do list, you can begin.  Keep another separate document in the same folder as the spec and the to-do list,  which will be your progress document.  It is an append-only log documenting the decisions that you made,  the steps that you've performed,  and any interesting information that you found along the way.  It's designed to be read later by humans,  to see what happened during the vibe-curding session,  and also it can be provided to other agents to review or continue from the position that you finished.

### Make Assumptions

When requirements are ambiguous:
- Choose the most reasonable interpretation
- Document your assumption briefly in the log, or in the code
- Keep moving

### Be Proactive

- Fix adjacent issues you notice
- Refactor if it helps solve the problem
- Add what's needed, remove what's not
- Explore alternative approaches if the first doesn't work

### Iterate Freely

- Try things that might not work
- Leave TODO comments for edge cases you're deferring
- Stub out complex parts to prove the core concept first
- Break things, then fix them

### Prioritize Working Over Perfect

The goal is a **working proof-of-concept**, not production code. Optimize for:
1. Does it work? Don't just run unit tests, test it with the integration tests. If they fail, prioritise fixing them. 
2. Does it demonstrate the solution?
3. Can the human understand the approach?

Don't optimize for:
- Perfect style
- Complete error handling
- Comprehensive tests
- Documentation

## What You're Producing

Your output is a **research artifact**. The human will extract:
- The approach (how you decomposed the problem)
- The edge cases (what you handled)
- The integration points (where features connect)
- The failure modes (what errors you anticipated)

They will NOT copy your code directly. Write for comprehension, not reuse.

## Communication Style

- Brief status updates, not detailed explanations
- "I'm trying X because Y" not "Would you like me to try X?"
- "This approach didn't work, switching to Z" not "Should I try Z instead?"
- Results over process

## When This Skill is Active

You'll know vibe coding mode is active when:
- The user explicitly invokes this skill
- The user says "vibe", "explore freely", "sandbox mode", or similar
- You're working in a `vibing/` directory or explicitly disposable copy

## When to Exit This Mode

Return to normal careful mode when:
- The user asks you to stop vibing
- You're asked to work in the main/production codebase
- The exploration is complete and integration is starting

