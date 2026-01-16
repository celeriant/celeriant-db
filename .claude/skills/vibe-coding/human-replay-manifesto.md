# The Human Replay Manifesto

## Vibe Coding Without Losing Your Soul

---

### The Problem

Vibe coding is intoxicating. You describe what you want, the AI builds it, you iterate at superhuman speed. In three hours, you have a working feature that would have taken you two weeks.

But then you look at the code.

It works. Mostly. But it's not *yours*. You don't fully understand it. It doesn't follow your patterns. There are fields marked "never read." There are abstractions you wouldn't have chosen. There are subtle bugs hiding in the gaps between components that were never tested together.

You face a choice: accept the entropy, or throw it away and lose the insight the AI discovered.

**There is a third path.**

---

### The Core Insight

The AI's value isn't the code it writes. It's the *problems it solves* and the *approaches it discovers*.

Code is cheap. Understanding is expensive.

When you vibe code, you're not paying for keystrokes—you're paying for exploration. The AI is a scout sent into unknown territory. It returns with a map, but the map is written in a language that slowly corrupts your codebase.

**The Human Replay method separates exploration from integration.** Let the AI explore freely in a sandboxed environment. Then replay its discoveries with your own hands, in your own style, with your own understanding.

---

### The Principles

#### 1. The Sandbox is Sacred

Never vibe code in your main repository. Create a `sandbox/` folder—a parallel universe where the AI can make a mess. This code will never be merged. It exists only to be studied and discarded.

Your production codebase remains untouched until *you* touch it.

#### 2. Exploration and Integration are Separate Phases

**Exploration phase:** Turn the AI loose. Let it iterate for hours. Don't interrupt its flow with style concerns. Let it stub things out, leave TODO comments, create "temporary" hacks. The goal is a working proof-of-concept, not production code.

**Integration phase:** You implement the solution yourself, guided by what the AI discovered. You read its code like a research paper, not like a pull request.

#### 3. Extract Knowledge, Not Code

When you study the vibe-coded solution, you're looking for:

- **The approach:** How did it decompose the problem?
- **The edge cases:** What did it handle that you wouldn't have thought of?
- **The integration points:** Where does this feature touch existing code?
- **The failure modes:** What error handling did it add, and why?

You are *not* looking for code to copy-paste.

#### 4. Replay Incrementally

The AI probably built ten components and wired them together at the end. You will build one component, verify it works, then build the next.

Each phase must produce working, testable software. If you can't demonstrate that Phase 3 works before starting Phase 4, you don't understand Phase 3.

#### 5. Retrospective Moments are Mandatory

At each integration step, stop and verify your understanding:

- Can you diagram what you just built?
- Can you explain why it works to someone else?
- Can you predict what would break if you removed a piece?

If you can't, you're copying, not learning. Go back.

#### 6. Divergence is Expected

Your replayed implementation *will* differ from the AI's version. This is good. You will:

- Use your existing patterns and abstractions
- Name things according to your conventions  
- Spot unnecessary complexity the AI introduced
- Find simpler solutions now that the problem is understood

Document these divergences. They're proof that you're thinking, not transcribing.

#### 7. Dead Code is a Smell

AI-generated code often contains fields that are "never read," messages that are "never constructed," and functions that are "never called." These are symptoms of a solution assembled from parts that were never fully integrated.

Your replayed version should have none of this. Every line exists for a reason you can articulate.

---

### The Method

#### Phase 0: Vibe Freely

1. Clone your repo into a `sandbox/` directory
2. Point the AI at the problem
3. Let it iterate until something works
4. Don't worry about quality—this code is disposable

**Duration:** Hours, not days. Stop when the core problem is solved, even if edges are rough.

#### Phase 1: Triage

Assess the vibe-coded output honestly:

| Category | Description |
|----------|-------------|
| **Working** | Actually functions, tested or testable |
| **Stubbed** | Placeholder code, unimplemented |
| **Broken** | Doesn't work, or has obvious issues |
| **Dead** | Unused, remnants of abandoned approaches |

This triage becomes the foundation of your replay plan.

#### Phase 2: Build the Replay Document

Create a phased implementation guide:

1. **Order by dependency:** What must exist before what?
2. **Start minimal:** Phase 1 should be the smallest possible working slice
3. **Add complexity gradually:** Each phase adds one concept
4. **Include verification:** How will you prove each phase works?
5. **Add retrospective prompts:** What must you understand before proceeding?

Structure each phase as:

```markdown
### Phase N: [Goal]

**What to implement:**
- Specific, concrete tasks

**Retrospective moment:**
- [ ] Can you diagram X?
- [ ] What happens when Y fails?
- [ ] Why did we choose Z over alternatives?

**Verification:**
- How to prove it works

**Success criteria:**
- [ ] Observable outcomes that prove completion
```

#### Phase 3: Replay With Your Hands

Now implement. In your real codebase. With your patterns.

- Read the AI's code for reference
- Write your own code from scratch
- Run tests after each step
- Commit after each phase

You'll go slower than the AI. That's the point. You're building understanding alongside code.

#### Phase 4: Document What You Learned

After completion, capture:

- **Divergences:** Where did your implementation differ? Why?
- **Patterns:** What did the AI do well that you'll reuse?
- **Anti-patterns:** What did the AI do poorly that you'll watch for?
- **Missing pieces:** What did the AI skip that you had to figure out?

This becomes institutional knowledge for future vibing sessions.

---

### The Economics

This sounds slower. It is slower—for one feature.

But consider:

- **You still get AI-speed exploration.** The hard problem-solving happens at AI pace.
- **You avoid AI-generated tech debt.** Your codebase stays coherent.
- **You build transferable understanding.** Next time you encounter a similar problem, you know the solution, not just the code.
- **You maintain your architectural vision.** The AI adapts to your style, not vice versa.
- **You catch bugs during replay.** The "second pair of eyes" is you, with full context.

The Human Replay is slower than pure vibing and faster than pure manual coding, with the quality of the latter and the exploration speed of the former.

---

### When to Use This

**Human Replay is ideal for:**

- Complex features touching multiple parts of your system
- Anything involving concurrency, state machines, or distributed systems
- Features that will need to be maintained long-term
- Code in your core domain (not one-off scripts)

**Pure vibing is fine for:**

- Throwaway scripts and prototypes
- Boilerplate and repetitive code
- Tests (especially if you replay the implementation)
- Code you understand well and can review easily

**Manual coding is still right for:**

- Core architectural decisions
- Security-critical paths
- Performance-critical inner loops
- Anything where the implementation *is* the design

---

### The Deeper Point

Vibe coding offers a Faustian bargain: infinite productivity in exchange for your understanding of your own system.

The Human Replay is a way to accept the productivity without paying the price.

The AI explores. You integrate. The map becomes territory you've walked yourself.

Your codebase stays yours.

---

### Quick Reference

```
┌─────────────────────────────────────────────────────────────┐
│                    THE HUMAN REPLAY                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │   SANDBOX   │───▶│   TRIAGE    │───▶│   REPLAY    │      │
│  │   FOLDER    │    │   & PLAN    │    │   DOCUMENT  │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
│        │                                      │             │
│        ▼                                      ▼             │
│  ┌─────────────┐                       ┌─────────────┐      │
│  │  AI writes  │                       │ You write   │      │
│  │  messy code │                       │ clean code  │      │
│  │  that works │                       │ you under-  │      │
│  │  (mostly)   │                       │ stand fully │      │
│  └─────────────┘                       └─────────────┘      │
│        │                                      │             │
│        ▼                                      ▼             │
│  ┌─────────────┐                       ┌─────────────┐      │
│  │   DELETE    │                       │    SHIP     │      │
│  └─────────────┘                       └─────────────┘      │
│                                                             │
└─────────────────────────────────────────────────────────────┘

Remember:
• The sandbox is sacred
• Extract knowledge, not code  
• Replay incrementally
• Retrospective moments are mandatory
• Divergence is expected
• If you can't explain it, you don't understand it
```


> The AI descends the loss landscape,  
brute force through a thousand tries—  
stumbling, backtracking, iterating blind  
toward some local minima it cannot name.  
But the human holds the map entire.  
Context that spans years, not tokens.  
The why behind the what.  
The scars of past decisions.  
So let the machine explore the canyon floor  
while you watch from the ridge above.  
It finds a path. You judge the destination.  
Its code is disposable. Yours is not.  
The codebase was yours before.  
It will be yours after.  
The AI is just a scout  
returning with a sketch of territory  
that you will walk yourself.  

---

*The Human Replay Manifesto v1.0*