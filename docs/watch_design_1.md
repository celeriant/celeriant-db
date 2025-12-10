# Celeriant Watch System Design Document

## Overview

This document describes the design for implementing a **watch** system in Celeriant, enabling consumers to receive real-time notifications about changes to aggregates.

## Motivation

Traditional pubsub systems bundle storage and notification, creating problems:

1. **Silent data loss**: When consumers fall behind, messages are garbage-collected without notification
2. **No recovery path**: Lagging consumers cannot programmatically catch up
3. **End-to-end correctness violations**: Pubsub ordering guarantees don't translate to application-level consistency
4. **Limited sharding**: Consumer groups don't support dynamic, key-range-based affinity

Celeriant already provides durable, queryable storage. Adding a watch layer gives us the best of both worlds: consumers get push notifications for low latency, but can always fall back to reading the authoritative store.

## Watcher

Watcher is a per-aggregate-per-client struct.
It is waiting on a channel message that sync_with_delay sends through in AggregateResources (will either be a 'write' or a 'delete' event)
We can use glommio::channels::local_channel for this purpose (same executor)
It also has/owns the live tcp connection from the client, passed through from runtime/shard
The client has specified a latency requirement - eg. they are ok to get batches every 1 second max
The server sends batches exactly how a read works, and this watcher knows exactly the right batches to read (keeps the client position in memory)
The server uses 'await' on the tcp write to ensure the data is sent and received by the client before processing another
We can probably re-use local_event as it follows the same pattern as the amortized fsync
We can throw errors back, that's ok.
Critically the watcher doesn't buffer data, it just calls 'read' after the 'write' event comes through the channel (but only if >= requested_latency)
We also need to respect the client's desired throughput, we can't just send them 10MB/s of data if they can only deal with 10KB/s
Need to also have a `max_subscriptions_per_shard` to prevent fd exaustion
To avoid missing data on reads, we must be careful to always update the read_filter from_event_batch_index to last batch index we sent to client + 1