# Assess a split

- Group error variants by operation. Use disjoint groups as evidence of separate responsibilities.
- Prefer narrower errors when callers need to distinguish outcomes. Avoid splitting solely to remove unused variants.
- Load `error-handling` before changing error types or wire mappings.
- Compare field ownership and mutation patterns. Propose a split when independent responsibilities need different lifetimes or access.
- Retain interior mutability for shared queues, one-time initialization, and counters read through shared references.
- Fix unnecessary shared access instead of adding interior mutability solely to avoid `&mut self`.
- Use `Cell` or `RefCell` for executor-local state. Choose thread-safe primitives when state crosses threads.
- Load `glommio-locking-patterns` before holding interior borrows near await points.
- Present the evidence and caller impact before expanding an agreed refactor into a split.
