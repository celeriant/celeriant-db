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

#### List Organisations
```bash
celeriant_cli list-orgs
celeriant_cli list-orgs --format json
celeriant_cli list-orgs --created-after 1700000000000
```

#### List Aggregates
```bash
celeriant_cli list-aggregates --org 1
celeriant_cli list-aggregates --org 1 --type 2 --format json
```

#### Check Aggregate Exists
```bash
celeriant_cli exists --org 1 --type 1 --id 1
```

#### Read Events
```bash
# Read all events
celeriant_cli read --org 1 --type 1 --id 1 --from 1

# Read specific range
celeriant_cli read --org 1 --type 1 --id 1 --from 5 --to 10

# Filter by event types
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --event-types 1,2,3

# Exclude client
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --exclude-client 999

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

# With compression
celeriant_cli write --org 1 --type 1 --id 1 \
    --client-id 1 --event-type 1 \
    --data '{"large": "payload"}' \
    --compression snappy
```

#### Trim Start
```bash
# Keep events from batch 10 onwards (delete 1-9)
celeriant_cli trim --org 1 --type 1 --id 1 --keep-from 10
```

#### Delete Aggregate
```bash
celeriant_cli delete --org 1 --type 1 --id 1
```

#### Update Cache Limits
```bash
celeriant_cli update-cache --max-size 1073741824  # 1GB
```

## Environment Variables

- `CELERIANT_SERVER` - Default server address (default: `127.0.0.1:10000`)

## TUI Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `↑/↓` or `j/k` | Navigate lists |
| `Enter` | Select / Confirm |
| `Esc` or `q` | Go back / Quit |
| `g/G` | Jump to start/end of list |
| `r` | Refresh current view |
| `e` or `i` | Enter edit mode |
| `Tab` | Next input field |
| `Shift+Tab` | Previous input field |
| `x` | Execute operation |
| `Space` | Toggle checkbox options |
| `?` or `F1` | Show help |
| `Ctrl+C` | Force quit |

## Output Formats

The CLI supports multiple output formats:

- `table` (default) - Human-readable table format
- `json` - Full JSON output for scripting
- `compact` - Condensed table format

## Scripting Examples

### Bash: Export all events to JSON
```bash
#!/bin/bash
ORG=1
TYPE=1
AGG_ID=1

celeriant_cli read --org $ORG --type $TYPE --id $AGG_ID \
    --from 1 --format json > events.json
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

### Bash: Monitor aggregate size
```bash
#!/bin/bash
while true; do
    celeriant_cli exists --org 1 --type 1 --id 1 2>/dev/null | grep Size
    sleep 5
done
```

### jq: Process JSON output
```bash
# Get all event batch indices
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --format json \
    | jq '.event_batches[].event_batch_index'

# Filter events by type
celeriant_cli read --org 1 --type 1 --id 1 --from 1 --format json \
    | jq '.event_batches[].events[] | select(.event_type == 1)'
```
