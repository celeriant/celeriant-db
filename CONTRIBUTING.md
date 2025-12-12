
# Workspace Setup

The project uses a workspace setup. To add a dependency:

```
cargo add serde --features derive,rc --package celeriant_wire
```

But you need to manually add it to Cargo.toml [workspace.dependencies] and change crates to serde.workspace = true


# Running benchmarks

Can take a while. Selectively run a benchmarks based on what you are working on. Save benchmark data to git.

```
cargo bench --package celeriant_wire --benches -- --save-baseline celeriant_wire
critcmp --export celeriant_wire > ./celeriant_wire/benches/celeriant_wire.json
```