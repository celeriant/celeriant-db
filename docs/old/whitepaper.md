EventPlaneDB: A Novel Architecture for High-Throughput Event Sourcing Using S3 for Distributed Coordination

1.0 Introduction: Addressing the Tooling Deficit in Event Sourcing

Event sourcing is a powerful architectural pattern, yet its adoption is frequently hampered by a persistent tooling problem. Teams often repurpose existing technologies, such as relational databases or streaming platforms, which introduces significant architectural friction. These general-purpose systems were not designed for the specific demands of append-only, high-throughput event logs, leading to operational complexity, performance bottlenecks, and design compromises. This gap between the pattern's requirements and the available tools has created a need for a more specialized foundation.

EventPlaneDB emerges as a purpose-built solution to this challenge. It is a distributed write-ahead log (WAL) substrate designed from the ground up to solve the write-side complexities of Command Query Responsibility Segregation (CQRS) and event sourcing. By focusing exclusively on providing a fast, correct, and durable log, it avoids the feature creep that compromises the performance and simplicity of other systems.

This paper's thesis is that EventPlaneDB's novel architecture, which strategically leverages Amazon S3 for coordination in place of traditional consensus protocols like Raft or Paxos, offers a compelling combination of high performance, strong durability, and superior operational simplicity. This design choice sidesteps an entire category of distributed systems complexity while delivering the guarantees required for mission-critical event-sourced applications. The following sections will analyze the limitations of conventional approaches and detail the architectural principles that enable EventPlaneDB to provide a new and more effective primitive for building event-driven systems.

2.0 The Architectural Imperative: Justifying a New Primitive

The choice of a foundational data store is one of the most critical decisions in an event-sourced system. Because every state change is captured as an immutable event, the underlying log becomes the system's ultimate source of truth. Common choices for this layer, while familiar, often introduce performance bottlenecks and operational burdens that undermine the benefits of the pattern. A careful analysis of these trade-offs reveals the need for a new, specialized primitive.

The limitations of conventional approaches highlight a recurring theme: adapting general-purpose tools for a highly specific workload forces developers to contend with architectural mismatches.

* PostgreSQL: While offering transactions and optimistic concurrency control, PostgreSQL was not designed for the relentless, small-write, append-only workloads characteristic of event sourcing. This mismatch manifests as significant operational pain points, including write-ahead log (WAL) amplification, constant pressure on the VACUUM process to reclaim space, and challenging replication lag that can compromise read-side consistency.
* Kafka: As a premier streaming platform, Kafka is often considered, but its core abstraction is ill-suited for event sourcing. It lacks fundamental guarantees like per-aggregate ordering without careful and complex partitioning schemes. It provides no built-in mechanism for optimistic concurrency control, its consumer groups are often opaque broker magic, and it provides no server-side filtering by aggregate or event type. Most critically, its default durability setting (which does not fsync to disk before acknowledgement) is dangerously weak for a system of record.
* Generic RDBMS with Event Tables: This common starting point is functional but inefficient. The approach is heavy, burdened by chatty commit protocols not designed for serverless environments and the unnecessary overhead of B-tree indexes and query planners that provide no value for a simple append-only log.
* EventStoreDB: Though a purpose-built competitor, its design conflates write-side and read-side concerns by including projections in its core. This design choice complicates its role as a pure log. Furthermore, its open-source version is crippled, and its foundation on the .NET runtime can introduce operational friction for teams standardized on other ecosystems.

EventPlaneDB's core design philosophy is a direct response to these challenges. It is engineered to "solve the write side of CQRS. Nothing more." This disciplined focus makes it a high-performance primitive, not a complete, all-in-one platform. By stripping away extraneous features like query languages and read-model projections, it delivers exceptional performance for its primary task: durably recording events. This architectural clarity provides the rationale for its existence and sets the stage for its unique design.

3.0 Core Architectural Principles

EventPlaneDB's architecture is founded on three key design pillars that work in concert to deliver its unique blend of performance, scalability, and operational simplicity. These principles are not merely implementation details but fundamental choices that define the system's character and justify its departure from conventional distributed database designs.

3.1 Thread-Per-Core, Share-Nothing Design

Inspired by high-performance systems like ScyllaDB and TigerBeetle, EventPlaneDB employs a thread-per-core, share-nothing architecture. Each server process runs a dedicated executor on each available CPU core, and each core is responsible for managing a distinct subset of event aggregates. This model eradicates the need for locks, mutexes, or other forms of cross-thread coordination on the performance-critical hot path. By partitioning responsibility at the lowest level, the system can scale linearly with the number of cores, as write operations for different aggregates can be processed in parallel without contention.

3.2 Per-Aggregate Leadership

Leadership in EventPlaneDB operates at the fine-grained level of an individual event aggregate, not at the coarse level of a node. This means that in a multi-node cluster, leadership for thousands of different aggregates can be distributed evenly across all available nodes and their cores. This approach has two profound benefits. First, it naturally distributes the write load across the entire cluster, preventing any single node from becoming a bottleneck. Second, it dramatically limits the blast radius of a failure. If a node serving as a leader for a subset of aggregates fails, only those specific aggregates are affected, while the rest of the system continues operating without interruption.

3.3 S3 as the Control Plane

The system's most novel architectural decision is its use of Amazon S3 as an external coordination service, completely replacing the need for an internal consensus algorithm like Raft or Paxos. This is made possible by two key features of the S3 API: its strong read-after-write consistency guarantees and its support for conditional write operations using headers like If-Match and If-None-Match. These conditional writes provide the compare-and-swap semantics necessary to build distributed primitives like locks and leases. By delegating the complex and error-prone task of achieving consensus to a highly available service, EventPlaneDB gains immense operational simplicity and resilience because, put simply, "S3 is more available than your cluster will ever be." Crucially, S3 effectively acts as the "third vote" or tie-breaker, which allows the system to bypass the typical odd-numbered quorum requirements of Raft/Paxos and run robust two-node clusters.

Together, these principles form a cohesive architectural vision that directly informs the system's high-availability and durability strategy, which is detailed in the replication model.

4.0 The S3-Coordinated Replication Model: A Deep Dive

This section dissects the mechanics of EventPlaneDB's replication strategy, which is central to its high-availability and durability guarantees. By leveraging the architectural principles of per-aggregate leadership and an S3-based control plane, the system achieves consensus-like guarantees of safety and liveness without incurring the complexity of implementing a traditional consensus algorithm.

4.1 Rationale: Bypassing Consensus Complexity

The decision to forgo consensus algorithms like Raft and Paxos was a deliberate rejection of an entire class of operational burdens. These protocols, while powerful, are notoriously complex to implement correctly and impose rigid operational constraints, such as the requirement for an odd-numbered quorum of nodes to tolerate failures. Instead, EventPlaneDB treats S3 as an external, strongly consistent coordination service. This is a strategic trade-off: in exchange for a dependency on S3, the system gains significant implementation simplicity, enhanced resilience, and the flexibility to run viable two-node clusters.

4.2 Lease-Based Leadership via Conditional Writes

Leadership for each event aggregate is granted via a time-bound lease, which is stored as a small JSON object in S3. A node acquires leadership by performing a conditional write to this S3 object. By using S3's If-Match (for renewals) or If-None-Match (for initial acquisition) headers, the system can guarantee that at most one node holds a valid lease for a specific aggregate at any given time. This mechanism provides the mutual exclusion required for safe leader election.

To prevent writes from "zombie leaders"—nodes that have been partitioned but have not yet realized their lease has expired—every lease contains a monotonically increasing lease_index. This value acts as a fencing token. The mechanics are simple and effective: all replication messages from the leader to its followers must carry the current lease_index. Followers, in turn, will reject any messages bearing a stale token, thereby preventing split-brain writes and ensuring strict data consistency.

4.3 Synchronous Replication and Degraded Mode Fallback

Once a leader has acquired a lease, it ensures durability through synchronous replication. A client write is only acknowledged after the data has been committed to the leader's local disk and replicated to all active followers. This "replicate-to-all" strategy is chosen over a quorum-based approach for a multi-faceted rationale. First, there is no latency penalty; replication occurs concurrently over parallel TCP connections, so the total time is determined by the slowest follower, not the sum. Second, it follows the precedent set by Kafka's acks=all mode, offering the strongest and most intuitive durability guarantee. Third, follower unavailability is treated as a rare edge case not worth optimizing for at the cost of weakening durability in the common case. Fourth, the system's resilience hinges on its Degraded Mode fallback: if a follower is unavailable, the leader writes the batch to S3, preserving both durability and availability. Finally, the "all-or-nothing" guarantee is simply easier to reason about than a quorum-based model that requires subsequent repair mechanisms.

4.4 The End-to-End Write Path

A durable write operation in EventPlaneDB follows a precise, multi-step sequence designed for correctness, performance, and crash safety:

1. Lease Validation: The leader node verifies it holds a valid, unexpired lease for the target aggregate.
2. Persist Pending Write Marker: Before modifying data files, the leader writes a small "pending write" marker to disk. In the event of a crash, this marker allows the startup process to safely roll back the incomplete operation.
3. Local Commit and fsync: The write is added to an in-memory batch. Once the batch is full or a time window expires, it is written and fsync'd to the local disk for durability.
4. Parallel Replication & Degraded Mode Fallback: The committed batch is simultaneously sent to all active follower nodes. If any follower fails to acknowledge within a timeout, the leader writes the batch to S3 for that follower.
5. Client ACK & Delete Marker: Once all followers have either acknowledged the write or had their data successfully staged in S3, the leader sends a success acknowledgement to the client and deletes the pending write marker.

This carefully orchestrated process demonstrates how EventPlaneDB's architectural choices translate into a robust and performant system for durable event storage.

5.0 Performance Analysis and Benchmarks

The architectural principles of a thread-per-core design and an S3-coordinated control plane translate directly into measurable high throughput and low latency for event sourcing workloads. The system is optimized to amortize the fixed costs of disk I/O, enabling it to process a high volume of small, independent writes efficiently.

Benchmark results on a single 16-core node with NVMe storage demonstrate the system's capabilities:

Mode	Throughput	Notes
Durable (fsync before ACK)	310,000 writes/sec	Amortized fsync via batching
Async (fsync in background)	2,500,000 writes/sec	For when durability can lag

The source of this high performance in durable mode lies in its batching strategy. EventPlaneDB collects incoming writes into batches within a configurable time window (e.g., 1ms), allowing it to commit hundreds of events with a single fsync operation. Since the physical cost of an NVMe fsync is relatively fixed at approximately 100 microseconds regardless of data size, this approach effectively amortizes the I/O cost across all writes in the batch.

For context, a direct performance comparison is revealing. On identical hardware, Apache Kafka with its default settings (which does not fsync before acknowledgement) achieves approximately 40,000 writes per second. In contrast, EventPlaneDB achieves 310,000 durable writes per second, demonstrating a significant performance advantage for its target workload while providing stronger safety guarantees. To complete the performance picture, the system targets a p99 latency of less than 10 milliseconds for durable writes, ensuring a responsive experience for client applications. These metrics underscore the effectiveness of its specialized design.

6.0 System Characteristics and Target Use Cases

EventPlaneDB's focused design philosophy—to be a primitive, not a platform—results in a clear and intentional separation of concerns. The system provides a specific set of powerful features while deliberately omitting others, making it an ideal component for certain application patterns and an unsuitable choice for others.

Core Features:

* Per-aggregate total ordering: Events within any given aggregate are guaranteed to be strictly and sequentially ordered.
* Optimistic concurrency control: Writes can specify an expected version (batch index) to prevent lost updates.
* Client idempotency: The system supports client-provided event indexes for safe, automatic deduplication of writes.
* Event type filtering: Readers can request only events of specific types, with server-side bloom filter acceleration.
* Compression: Per-batch compression using Zstd, Snappy, Brotli, or Gzip.
* In-memory read cache: Recent events are served directly from memory for low-latency reads.
* Explicit offsets: Clients retain full control over their read position.
* Replication: Data is replicated synchronously to all followers or to S3 as a fallback, ensuring high availability and durability.
* Lease-based leadership: Leadership is managed per-aggregate with automatic failover, distributing load and limiting failure domains.

Intentional Omissions:

As a core design choice aimed at minimalism and performance, EventPlaneDB does not include:

* A query language
* Secondary indexes
* Built-in projections or read models
* Managed consumer groups
* Automatic offset management
* Transactions that span across different aggregates
* Data tiering (yet)
* A hosted service (yet)
* An Admin UI (yet)
* Fine-grained permissions (yet)

This minimalism ensures the system remains a lean, high-performance WAL substrate that can be composed with other specialized tools for querying and processing.

6.1 Target Use Cases

EventPlaneDB is ideally suited for scenarios that require a "fast, correct log per 'thing'." Its architecture excels in the following application patterns:

* Event-sourced microservices: Providing the durable write-side persistence for domain models.
* Financial systems and ledgers: Ensuring a strictly ordered, immutable log of transactions for each account.
* Audit trails: Creating a secure, append-only record of all significant actions within a system.
* IoT event ingestion: Managing high-throughput event streams from millions of individual devices.

6.2 When Not to Use EventPlaneDB

The system's focused nature makes it an anti-pattern for certain use cases. It is not a good fit for applications that:

* Require ad-hoc querying over event data on the write side.
* Need ACID transactions across multiple aggregates.
* Desire a fully managed, broker-driven consumption model with consumer groups, as provided by platforms like Kafka.
* Demand a complete, all-in-one event sourcing platform rather than a composable primitive.

This clear delineation of scope allows architects to make informed decisions, leveraging EventPlaneDB where its strengths provide maximum value.

7.0 Conclusion: A New Foundation for Event-Sourced Systems

EventPlaneDB presents a compelling value proposition by directly addressing the tooling deficit in the event sourcing ecosystem. It is a specialized, high-performance WAL substrate that solves the write-side problem of CQRS and event sourcing without the performance and operational compromises inherent in general-purpose databases and streaming platforms. Its architecture is a testament to the power of focused design, delivering exceptional throughput and strong durability by doing one thing exceedingly well.

The system's central innovation—the use of Amazon S3's strong consistency primitives to orchestrate distributed leadership—is a pragmatic and powerful departure from tradition. By replacing complex internal consensus algorithms, EventPlaneDB achieves a remarkable degree of operational simplicity and resilience, making distributed event storage more accessible and manageable.

Currently in an "Alpha" stage, EventPlaneDB is already demonstrating its potential in production environments. With a clear roadmap to implement features such as data tiering to object storage, a hosted service, an Admin UI, and fine-grained access control, it is positioned to become a foundational primitive for the next generation of robust, scalable, and performant event-driven applications.
