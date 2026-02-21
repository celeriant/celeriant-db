# Celeriant CLI

A command-line interface and terminal UI for interacting with the Celeriant event store.

## Installation

```bash
cargo build --release -p celeriant_cli
```

## Usage

### TUI Mode (Interactive)

Simply run without any command to enter interactive mode:

```bash
celeriant_cli
# or with a custom server
celeriant_cli --server 192.168.1.100:10000
```

### CLI Mode

Use subcommands for direct operations:

#### Aggregate Details
```bash
celeriant_cli aggregate-details --org 1 --type 1 --id 1

# With correlation ID
celeriant_cli aggregate-details --org 1 --type 1 --id 1 --correlation-id 42
```

Output includes batch index range, max event index, deleted status, allow-recreate/allow-index-continuation flags, last timestamp, and last client ID.

#### Read Events
```bash
# Read all events from batch 1
celeriant_cli read --org 1 --type 1 --id 1 --from 1

# Read specific range
celeriant_cli read --org 1 --type 1 --id 1 --from 5 --to 10

# Filter by event types
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --event-types 1,2,3

# Exclude a specific client
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --exclude-client 999

# Include only events from a specific client
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --include-client 42

# Filter by server timestamp range (unix millis)
celeriant_cli read --org 1 --type 1 --id 1 --from 1 \
    --min-timestamp 1700000000000 --max-timestamp 1700099999000

# JSON output
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --format json
```

#### Write Events
```bash
# Write inline data
celeriant_cli write --org 1 --type 1 --id 1 \
    --client-id 1 --event-type 1 \
    --data '{"message": "Hello, World!"}' \
    --allow-create

# Write from file
celeriant_cli write --org 1 --type 1 --id 1 \
    --client-id 1 --event-type 1 \
    --file ./event-data.json \
    --allow-create

# With snappy compression
celeriant_cli write --org 1 --type 1 --id 1 \
    --client-id 1 --event-type 1 \
    --data '{"large": "payload"}' \
    --compression snappy

# With zstd compression
celeriant_cli write --org 1 --type 1 --id 1 \
    --client-id 1 --event-type 1 \
    --data '{"large": "payload"}' \
    --compression zstd

# Optimistic concurrency (only write if at batch 5)
celeriant_cli write --org 1 --type 1 --id 1 \
    --client-id 1 --event-type 1 \
    --data '{}' --expected-index 5

# Enforce client idempotency
celeriant_cli write --org 1 --type 1 --id 1 \
    --client-id 1 --event-type 1 \
    --data '{}' --enforce-idempotency

# With user ID and correlation ID
celeriant_cli write --org 1 --type 1 --id 1 \
    --client-id 1 --user-id 99 --event-type 1 \
    --data '{}' --correlation-id 123
```

#### Trim Start
```bash
# Keep events from batch 10 onwards (deletes batches 1-9)
celeriant_cli trim --org 1 --type 1 --id 1 \
    --client-id 1 --keep-from 10
```

#### Delete Aggregate
```bash
# Simple delete
celeriant_cli delete --org 1 --type 1 --id 1 --client-id 1

# Delete and allow recreation from a clean slate
celeriant_cli delete --org 1 --type 1 --id 1 \
    --client-id 1 --allow-recreate

# Delete and allow recreation continuing from last batch/event indexes
celeriant_cli delete --org 1 --type 1 --id 1 \
    --client-id 1 --allow-recreate --allow-index-continuation

# Delete with optimistic concurrency check
celeriant_cli delete --org 1 --type 1 --id 1 \
    --client-id 1 --expected-index 42
```

## Environment Variables

- `CELERIANT_SERVER` - Default server address (default: `127.0.0.1:10000`)

## Client Identity

On first TUI launch, the client generates and stores a persistent Ed25519 keypair using `celeriant_crypto`. The numeric client ID is derived from this keypair and used for all write operations in TUI mode.

Key storage location follows OS conventions via `directories::ProjectDirs`:
- Linux: `~/.local/share/celeriant_cli/`
- macOS: `~/Library/Application Support/com.celeriant.celeriant_cli/`
- Windows: `%APPDATA%\celeriant\celeriant_cli\data\`

The client ID and storage path are printed on TUI startup.

## TUI Screens

The TUI is organised into the following screens:

### Home
Entry point. When disconnected shows Connect/Change Server options. When connected provides access to Enter Aggregate, List, Organisation Watch, and Disconnect.

### Connect
Edit the server address and establish a connection. Press `e` or `i` to edit, `Enter` to connect.

### Enter Aggregate
Enter an org ID, aggregate type ID, and aggregate ID to navigate to an aggregate context.

### Aggregate Context
Per-aggregate hub showing batch range, max event index, and deleted status. From here you can navigate to Read Events, Write Event, Watch, Trim Start, and Delete.

### Read Events
Enter a from/to batch range and press `x` to fetch. Results are scrollable.

### Write Event
Shows the persistent client ID (derived from keypair, read-only). Enter event type and data (JSON/text or a file path). Press `x` to submit.

### Watch Screen
Real-time event monitoring for a single aggregate. Configure event types (0-5) and polling latency, then press `x` to start streaming. Events auto-scroll as they arrive. Press `s` to stop.

### Organisation Watch Screen
Real-time monitoring scoped to an organisation. Configure org ID, aggregate types (optional), event types, and latency. Supports watching all aggregate types simultaneously. Press `x` to start, `s` to stop.

### List Screen
Interactive listing of organisations, aggregate types, or aggregates. Leave Organisation ID empty to list all orgs; fill it and leave Aggregate Type empty to list types; fill both to list aggregates with size, batch count, event count, and last-updated timestamp.

## TUI Keyboard Shortcuts

### All screens (Normal mode)
| Key | Action |
|-----|--------|
| `q` | Go back / Quit (from Home) |
| `Esc` | Go back |
| `?` or `F1` | Show help |
| `Ctrl+C` | Force quit |

### Home / Aggregate Context
| Key | Action |
|-----|--------|
| `↑/↓` or `j/k` | Navigate menu |
| `Enter` | Select item |
| `r` | Refresh aggregate info (Aggregate Context only) |

### Watch / Organisation Watch
| Key | Action |
|-----|--------|
| `e` or `i` | Edit configuration (only when stopped) |
| `x` | Start watching |
| `s` | Stop watching |
| `↑/↓` or `j/k` | Scroll events |
| `g/G` | Jump to start/end |
| `PageUp/PageDown` | Scroll by page |

### Read Events / Write Event / List
| Key | Action |
|-----|--------|
| `e` or `i` | Enter edit mode |
| `x` | Execute (read/write/list) |
| `↑/↓` or `j/k` | Scroll results |
| `g/G` | Jump to start/end |
| `PageUp/PageDown` | Scroll by page |

### Edit Mode (any input screen)
| Key | Action |
|-----|--------|
| `Tab` | Next input field |
| `Shift+Tab` | Previous input field |
| `Enter` | Confirm / execute |
| `Esc` | Exit edit mode |

## Event Type Reference

| Code | Name |
|------|------|
| 0 | DELETE |
| 1 | WRITE |
| 2 | READ |
| 3 | TRIM_START |
| 4 | DETAILS |
| 5 | CREATE |

## Output Formats

The `read` command supports multiple output formats via `--format`:

- `table` - Human-readable table format (default for most commands)
- `json` - Full JSON output, suitable for scripting
- `compact` - Condensed table format

## Scripting Examples

### Bash: Export all events to JSON
```bash
#!/bin/bash
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --format json > events.json
```

### Bash: Monitor aggregate metadata in a loop
```bash
#!/bin/bash
while true; do
    celeriant_cli aggregate-details --org 1 --type 1 --id 1
    sleep 5
done
```

### Bash: Batch write events from files
```bash
#!/bin/bash
for file in ./events/*.json; do
    celeriant_cli write --org 1 --type 1 --id 1 \
        --client-id 1 --event-type 1 \
        --file "$file" --allow-create
done
```

### jq: Process JSON output
```bash
# Get all event batch indices
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --format json \
    | jq '.event_batches[].event_batch_index'

# Filter events by type
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --format json \
    | jq '.event_batches[].events[] | select(.event_type_major == 1)'
```
