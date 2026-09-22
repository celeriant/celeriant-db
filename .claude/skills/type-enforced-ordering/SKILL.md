---
name: type-enforced-ordering
description: Building and refactoring safe, honest functions. Enforce call ordering and cleanup when designing stateful APIs, receipt tokens, or Drop implementations.
---

# Enforce ordering

## Choose the mechanism

- Merge operations that always run consecutively.
- Return a receipt when a later operation requires an earlier result.
- Use typestate for a small, fixed set of phases with costly ordering failures.
- Use RAII guards for scope-bound cleanup. It's not just about memory, the pattern can be used for a lot of things.
- Return typed errors for invalid runtime states when static enforcement adds excessive complexity.

## Design receipts

- Restrict construction with private fields.
- Carry the data established by the issuing operation.
- Consume one-shot receipts by value; omit `Clone` and `Copy`.
- Check issuer identity and freshness when required. A token from `a.prepare()` can otherwise be passed to `b.commit(token)`.
- Treat `#[must_use]` as a diagnostic, not proof of consumption. Define behavior for abandoned receipts; avoid panic-based cleanup during unwind.

## Design typestate

- Consume the old state during transitions.
- Expose operations only on valid states.
- Use `&mut self` for operations that remain in the same state.
- Define failure ownership explicitly. Return the previous state only if its invariants still hold; otherwise close the resource or return a failed state.
- Account for partial I/O and cancellation before permitting retries.
- Use an enum at collection boundaries when states must coexist.
- Share state-independent methods through a sealed marker trait when needed.

## Design cleanup

- Use guards for locks, pool checkouts, counters, gauges, and temporary resources.
- Return a pooled resource only when it is still usable; discard it when the guard observed a fault.
- Provide an explicit fallible or async close when cleanup must report failure or await I/O. Keep `Drop` as a synchronous fallback.
- Keep destructors short. Avoid blocking I/O and locks that can stall an executor.
- Preserve memory safety even when destructors never run, including forgotten values, cycles, and process termination.
- Distinguish dropping a future from detaching its task. Do not assume detached or leaked work releases resources promptly.
- Account for reverse declaration order for locals and declaration order for fields. Do not depend on closure capture drop order.
