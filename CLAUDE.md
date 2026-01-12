# Celeriant Development Guidelines

## Default Mode: Deliberate and Collaborative

**Vibe coding is OFF by default.** Unless explicitly enabled via the `vibe-coding` skill, work deliberately:

- **Small, focused changes.** One logical change at a time.
- **Explain what you're doing and why.** Before making changes, state your plan and reasoning.
- **Ask when uncertain.** Don't assume—clarify requirements.
- **Easy to follow.** The programmer should understand every change without effort.
- **Push Back.** If the programmer asks too much, refuse to implement it.

To enable autonomous exploration mode, explicitly invoke the `vibe-coding` skill. See `vibe-manifesto.md` for when this is appropriate.

---

## Codebase Navigation

Read crate README.md files first—they contain architecture context that avoids unnecessary code exploration. The `understanding-celeriant-structure` skill is also invaluable when learning where things should go in the code.

## Code Style: Direct and Minimal

This is a high-performance database. Every line matters.

**No verbose code.** Write the shortest correct solution. If it can be done in fewer lines without sacrificing clarity, do it.

**No obvious comments.** Don't explain what the code does—the code explains itself. Only comment *why* when the reason isn't apparent.

```rust
// BAD
// Check if the cache contains the key
if cache.contains(&key) {
    // Return the value from the cache
    return cache.get(&key);
}

// GOOD
if let Some(value) = cache.get(&key) {
    return value;
}
```

## Refactoring Over Duplication

**Never duplicate code.** If you're writing similar logic twice, extract it. Refactoring is not optional—it's the priority.

Before adding new code, ask:
1. Does this pattern already exist in the codebase?
2. Can I extend an existing abstraction?
3. Will this cause duplication that I should prevent now?

## Performance is Non-Negotiable

This codebase is crafted for microsecond-level operations. Respect that.

- No allocations in hot paths without justification
- No unsafe
- No unbounded data structures (see `database-architecture` skill)
- No `clone()` when a reference works
- No `String` when `&str` suffices
- No `Box<dyn Trait>` when generics work

## Structure

- Keep functions short and focused
- Early returns over nested conditionals
- Group related logic, separate concerns
- Match the patterns already established in the crate

## Skills

Consult `.claude/skills/` for detailed patterns:
- `vibe-coding`: Autonomous exploration mode (must be explicitly enabled)
- `database-architecture`: Memory bounds, durability, tracing
- `glommio-locking-patterns`: Async concurrency, deadlock avoidance
- `understanding-celeriant-structure`: Crate responsibilities, data flow
- `error-handling`: Error types, conversions
- `testing`: Test patterns, benchmarks
