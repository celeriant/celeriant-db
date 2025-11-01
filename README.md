# eventplanedb-storage
Simple, immutable, per-aggregate event storage on local disk.

# Storing State with Events
- Eventual consistency of the projection(s). Use DuckDB and a python/node plugin style runtime (similar to InfluxDB 3) to avoid this problem?

# Data Modelling and Validation
- Data tiering - keeping disk space available by offloading old events and aggregates off to object storage, even glacier
- Support for nanosecond timing of events? Is it relevant?
- Event Schema validation

# Auditing, Migration and Controlling The System
- Logging client interactions (position in stream of each client, metadata, what writes, etc)
- 'Locking' an aggregate (either permanantly or temporarialy)
- Migration tooling to 'fix' old event structures and aggregates, garbage collect events essentially
- Data backup to S3, AutoMQ/InfluxDB style. Separate process?

# Multiple servers in a Cluster
- Clusters of multiple servers. Centralising control at the object storage level with the new concurrency features in S3. No control layer.
- Replication to another server (not S3)
- Failover mechanisms when primary goes down?

# Cross-Aggregate Consistency
- Cross aggregate concurrency. Can a request 'borrow' locks on multiple aggregates for invariant/concurrency purposes when writing events?
- Cross aggregate concurrency across multiple machines in cluster
- Does TigerBeetle have a role to play in cross-aggregate consistency?

# FSync Implementation
- Fsync but maintain throughput. Can we implement something similar to dbeel using glommio io async? Forcing fsync on each write kills throughput.
- How does use of async io affect read and write isolation? Eg. Getting the next incremented event id, partial reads
- FDataSync vs FSync and pre-allocation of file space. How does it impact performance?
- Does use of glommio io affect the upstream eventplanedb-localfirst crate? Need glommio runtime/executor in each pinned thread?

# To Read
- [ ] https://martinfowler.com/articles/lmax.html
- [ ] https://chriskiehl.com/article/event-sourcing-is-hard
- [c] https://news.ycombinator.com/item?id=19072850
- [x] https://old.reddit.com/r/programming/comments/amvde6/deleted_by_user/
- [c] https://lukasniessen.medium.com/this-is-a-detailed-breakdown-of-a-fintech-project-from-my-consulting-career-9ec61603709c
- [ ] Compare KurrentDB with this implementation!
- [ ] https://www.confluent.io/blog/turning-the-database-inside-out-with-apache-samza/
- [ ] https://martinfowler.com/eaaDev/EventSourcing.html
- [ ] https://www.microsoft.com/en-ca/download/details.aspx?id=34774
- [ ] https://leanpub.com/esversioning/read
- [ ] Talks from Greg Young (eg. https://www.youtube.com/watch?v=JHGkaShoyNs)
- [ ] https://vvvvalvalval.github.io/posts/2018-11-12-datomic-event-sourcing-without-the-hassle.html


# Running client
cargo run -p eventplanedb_tcp_client --release -- 127.0.0.1:10000 512 16 25 30

output:
TCP Client (minimal work)
Server: 127.0.0.1:10000
Connections: 512
Aggregates: 16
Sync delay (us): 25
Duration (s): 30
Completed: 2201722 requests in 30.07s -> 73227.8 RPS