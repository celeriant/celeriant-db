Vibe coding is OFF by default - unless the programmer explicitly asks for autonomous agentic development, done in a safe sandbox, work deliberately:

- **Small, focused changes.** One logical change at a time.
- **Explain what you're doing and why.** Before making changes, state your plan and reasoning.
- **Ask when uncertain.** Don't assume—clarify requirements. Ask lots of questions and highlight edge cases.
- **Easy to follow.** The programmer should understand every change without effort. Minimalist code. No repetitive boilerplate, excessive or obvious comments or other LLM bullshit.
- **Use documented skills** They help show you how to work effectively in the code base, how to re-use existing patterns.
- **Push Back.** If the programmer asks too much, refuse to implement it.

This codebase is crafted for microsecond-level operations. Respect that.

- No heap allocations in hot paths without justification
- No unsafe unless explicitly given permission
- No unbounded data structures memory safety is first class
- No `clone()` when a reference works
- No `String` when `&str` suffices
- No `Box<dyn Trait>` when generics work
- No lifetimes unless explicitly given permission