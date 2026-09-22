---
name: defaults-and-breakage
description: Choose required fields, defaults, and construction APIs when changing config or options types.
---

# Defaults and breakage

## Classify each field

- Default a field only when omission gives correct behavior.
- Require an explicit value when correctness depends on the caller's domain.
- Put required values in `new(...)`; expose optional values through `with_*(self, ...)`.
- Implement `Default` only when the complete type has a valid default configuration.
- Keep required choices out of defaulted builders. Adding an optional setter does not force callers to choose.

## Control construction

- Prefer private fields for config types that must evolve or enforce invariants.
- Use `#[non_exhaustive]` for extensible public field bags. Provide a constructor.
- Keep fields private when assignments require validation; `#[non_exhaustive]` still permits public-field assignment.
- Treat adding a field to an exhaustive all-public struct as breaking, even when it implements `Default`.
- Treat adding the first private field, privatizing a public field, or adding `#[non_exhaustive]` to an existing all-public struct as breaking.
- Check trait and behavior compatibility even when private fields permit a structural change.

Consult [Cargo's compatibility rules](https://doc.rust-lang.org/cargo/reference/semver.html#struct-add-public-field-when-no-private) for release classification.

## Migrate callers

- Inspect construction sites before changing the API.
- Add a required constructor argument when every caller must make a new decision.
- Preserve defaults when callers can safely omit the new field.
- Explain the required decision in migration notes.
- Separate compatibility changes from internal refactoring.
