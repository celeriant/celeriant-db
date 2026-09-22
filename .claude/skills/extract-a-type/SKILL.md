---
name: extract-a-type
description: Refactor a named Rust type or its surrounding code with an agreed scope. Triggers on "refactor X", "clean up X", "tidy this file", "dedupe this", "pull X into its own file", "this type is doing too much". Use simplify for changeset-wide cleanup and code-review for bug hunting.
---

# Extract a type

## Diagnose

1. Inspect `git status --short` and existing diffs. Preserve unrelated work. Obtain approval for a WIP commit and its contents before creating one.
2. Run `cargo check --workspace --all-targets`. Report baseline failures and resolve them separately before refactoring.
3. Find construction sites, field accesses, helpers, and distinctive error strings with `rg`. Search the whole workspace.
4. Read each affected site. Record its path, current behavior, and proposed behavior change in a drift table. Mark unchanged behavior as `none`.
5. Identify existing test coverage. For non-mechanical changes without coverage, propose characterization tests against current behavior first. Permit compiler-only verification for mechanical moves, renames, and import rewrites.

## Agree on scope

Present the site count, drift table, proposed changes, and verification plan before editing. Obtain agreement unless the user already authorized that scope.

- Clarify the intended outcome when ambiguous.
- Report incidental bugs separately.
- For more than roughly 20 sites or 3 crates, propose batches: add the new form, delegate from the old form, migrate callers, then remove compatibility code.
- Separate refactors, contract changes, and bug fixes into distinct commits. Preserve existing behavioral assertions during refactoring.

## Refactor

1. Reuse existing pure transformations. Keep I/O, clocks, and environment access at the boundary. Reject an intermediate layer whose body forwards to an existing function without a caller that requires it.
2. Check dependency direction before moving code. Preserve deliberate crate separation; a lean consumer must not gain a heavy dependency. Keep equivalent helpers that a deliberate boundary requires; remove only the incorrect copy.
3. Migrate identical call sites together. Handle sites with distinct surrounding logic individually. Compile between batches.
4. Move cohesive field operations onto the type. Read [receivers.md](references/receivers.md) when choosing receivers or changing visibility.
5. Keep unit-test helpers behind `cfg(test)`. Keep integration helpers in shared test modules or a support crate accessible to their consumers.
6. Remove dead imports and unused dependencies. Use `cargo machete` when available; inspect feature-gated code, tests, and benches before deleting dependencies.
7. Update documentation and compile-checked examples that teach the old API.

Keep formatting local. Do not run a workspace-wide formatter.

## Verify

- Run `cargo check --workspace --all-targets` with no new warnings.
- Run affected tests, including characterization tests established before the refactor.
- Execute tests for each changed behavior recorded in the drift table, including successful cases that must remain valid.
- Inspect the final diff against the agreed scope. Report deviations and unresolved failures.

## Load guidance when needed

| Condition | Load |
|---|---|
| Error enum changes | `error-handling` |
| Test work | `testing` |
| Hot-path changes | `build-method:performance-discipline` |
| Config defaults or API breakage | `defaults-and-breakage` |
| Call ordering or cleanup | `type-enforced-ordering` |
| Globals or ownership choices | `data-ownership` |
| Interior borrows near await points | `glommio-locking-patterns` |
| Possible responsibility split | [split-signals.md](references/split-signals.md) |
