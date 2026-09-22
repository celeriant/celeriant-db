---
name: data-ownership
description: Choose ownership when introducing globals, borrowed struct fields, clones, shared pointers, or store handles.
---

# Data ownership

## Pass dependencies explicitly

- Pass values that tests or callers may need to vary.
- Use `const` for fixed values.
- Restrict globals to process-wide infrastructure, such as allocators and tracing or metrics registration.
- Initialize process-wide configuration before starting work. Use `OnceLock` for shared initialization with inputs; use `LazyLock` when initialization needs none.
- Isolate tests that require different global registrations. Pass deterministic seeds into workloads.
- Use `OnceCell` for thread-local ownership; do not place it in a shared `static`.

## Borrow within operations

- Accept references when ownership is unnecessary. Prefer elided signatures such as `fn host(address: &str) -> Result<&str, Error>`.
- Follow CLAUDE.md's permission requirement before adding explicit lifetime parameters.
- Avoid storing borrows in long-lived structs. Check how a stored borrow constrains holders and async tasks.
- Reserve borrowed adapters for scoped operations, such as iterators, guards, and readers. Consider an owning adapter when callers must store or return it.

## Choose stored ownership

Consider these options in order:

1. Remove the field; pass a borrow to the operation.
2. Own the value when it belongs to the struct.
3. Share immutable data through `Arc<T>`, or `Rc<T>` when sharing stays within one thread. Arc is not required in thread per core.
4. Use an index or handle when a central store already owns the data. Preserve handle validity across removal and reuse.

Measure allocation and atomic costs before introducing them on a hot path. Load `build-method:performance-discipline` for that work.
