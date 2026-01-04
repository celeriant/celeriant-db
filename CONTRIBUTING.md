
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

# Run only write_benchmark
cargo bench --package celeriant_shard --bench write_benchmark -- --save-baseline write_baseline
critcmp --export write_baseline > ./celeriant_shard/benches/celeriant_shard_write.json

# Run only aggregate count
cargo bench --package celeriant_shard --bench aggregate_count_benchmark -- --save-baseline aggregate_count_baseline
critcmp --export write_baseline > ./celeriant_shard/benches/celeriant_shard_write.json

# Run only exists_benchmark
cargo bench --package celeriant_shard --bench exists_benchmark -- --save-baseline exists_baseline
critcmp --export exists_baseline > ./celeriant_shard/benches/celeriant_shard_exists.json

# Run only the fsync_delay group from write_benchmark
cargo bench --package celeriant_shard --bench write_benchmark -- "write_fsync_delay"

# Run only cache_impact tests
cargo bench --package celeriant_shard --bench write_benchmark -- "write_cache_impact"

# Run only single_aggregate tests across all groups
cargo bench --package celeriant_shard --bench write_benchmark -- "single_aggregate"

```