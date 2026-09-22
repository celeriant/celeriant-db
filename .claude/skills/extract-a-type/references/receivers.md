# Choose receivers

- Use `&self` for reads.
- Use `self` for ownership transfer, conversions, and consuming builders.
- Use `&mut self` for mutation requiring exclusive access.
- Keep stateful operations explicit; do not hide prerequisites in flags checked by later methods.
- Load `type-enforced-ordering` when correctness depends on call order.
- Reject invalid runtime states with typed errors. Refuse reuse of a resource left in an unknown state rather than retrying on it.

# Preserve compatibility

- Separate adding methods from privatizing fields.
- Retain delegating accessors during staged migrations; remove them after callers migrate.
- Load `defaults-and-breakage` before changing config construction.

# Keep control flow readable

- Use simple iterator chains when execution order remains apparent.
- Replace nested closure factories, runtime-assembled pipelines, and unnecessary recursion with explicit steps.
