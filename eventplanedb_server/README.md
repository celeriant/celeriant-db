TODO: Update readme for TCP server

# Glommio Thread Affinity Sharded Server Starter-Kit

A high-performance sharded TCP server built with Rust and Glommio that shows off connection migration between CPU-pinned executor shards. Built this to explore some patterns for distributed systems and event sourcing architectures.

## What's This About

This server demonstrates a few interesting concepts:

- CPU-pinned shards where each shard runs on its own dedicated core
- Live TCP connection migration between shards using file descriptor passing
- Full mesh communication between shards using lock-free channels
- Per-shard caching to avoid synchronization headaches
- Proper error handling for production-ish workloads

The main goal here is to demonstrate that for a given input, the request is always routed to the *same* shard for processing.

## Architecture

The server spins up one shard per CPU core. Each shard can accept connections on the same port (thanks to SO_REUSEPORT) and route requests to the appropriate shard based on a simple hash. If a connection needs to move to a different shard, the entire TCP connection gets transferred over.

```
Client connects to any shard -> Request gets routed to correct shard -> Connection migrates if needed
```

Pretty neat way to avoid the usual coordination overhead you get with shared state.

Clients don't need a new TCP connection for each request. Multiple requests can be sent over the same connection, each time the server migrates the entire connection to the correct shard for processing.

## Why Would You Want This

This pattern works well for:

- Event sourcing systems where related events need to hit the same partition
- Stateful services that benefit from data locality
- High-throughput applications where you want to minimize cross-thread coordination
- Any system where you can deterministically route requests to specific workers

The Fibonacci example is just for demo purposes. In practice you'd probably use this for something like processing financial transactions, game state updates, or database operations where partition affinity matters.

## Running It

You'll need git, Rust 1.70+ and a Linux box (Glommio requirement).

```bash
git clone https://github.com/utilitydelta-io/glommio-sharded-affinity-server-starter
cd glommio-sharded-affinity-server-starter
cargo run -p server --release
```

Then connect with netcat and send some numbers:

```bash
nc localhost 10000
42
1337
55
```

Each number gets processed by the shard responsible for that value, with results cached locally.

You can also run the client console app:

```bash
cargo run -p client --release
```

## Performance

RaspberryPi 5 - both client + server running locally:

Total time: 30.01s
Total requests: 4650396
Requests per second: 154971.0
Average response time: 0.32ms

155k requests per second on a $140 AUD RaspberryPi 5!

That's all on localhost... On LAN (Cat6e):

Total time: 30.01s
Total requests: 1983762
Requests per second: 66099.8
Average response time: 0.76ms

Half the speed due to the network hop, but still 66k requests per second.

## How It Works

The interesting bit is the connection migration. When a request comes in on the wrong shard:

1. We extract the raw file descriptor from the TCP stream
2. Send it over a channel to the target shard
3. Use `std::mem::forget()` to transfer ownership
4. The target shard reconstructs the TCP stream and continues processing

This lets clients maintain persistent connections while still getting proper request routing. The mesh channel setup ensures any shard can talk to any other shard without any locking primitives.

## Implementation Notes

- File descriptor passing only works after successful channel send, so no risk of leaking connections
- Each shard maintains its own LRU cache to avoid cache invalidation across threads  
- CPU pinning helps with cache locality and predictable performance
- Error handling includes backpressure management and graceful connection closure

The code is structured to be readable rather than maximally optimized. There's definitely room for improvement on the performance side if you wanted to use this in anger.

## Code Structure

- `main()` sets up the executor pool and mesh channels
- `process_tcp_stream()` handles the connection routing logic
- `process_synchronously_on_shard()` does the actual work with caching
- `compute_fibonacci()` is just a CPU-intensive placeholder

## Contributing

This is mostly a proof of concept, but if you spot bugs or have ideas for improvements, feel free to open an issue. Could definitely use some proper benchmarks.

## License

MIT License.
