Create a markdown design doc to implement replication. 

The intended design is to run a single leader + multiple follower, at the aggregate level. We don't ack back to clients until the leader has fsync'd AND the data is replicated to all followers.

We don't want to implement a distributed concensus protocol like raft. Instead, we will rely on S3, with it's new optimistic concurrency control support (conditional writes). To become a leader of an aggregate, a node must write a 'lease' for itself and 'win' the lease via conditional write success. A lease will be granted for a duration.

Other nodes won't challenge the lease until it is about to expire. The current writer will proactively renew the lease before it expires, but only if it is still receiving active writes from clients. Otherwise it'll go dormant and allow the lease to expire. This way we save costs on S3, only performing work on active aggregates.

The lease file is per-aggregate and contains:
{
    "lease_index": "incremented u64",
    "node_id": "u128 unique identifier of node trying to gain lease",
    "lease_expiry": "timestamp in millis when lease will expire",
    "event_batch_index": "current event batch index position at time of lease",
    "requested_by_client": "u128 unique identifier of client trying to write to node"
}

Nodes won't write if their lease expires. They will become followers. Followers disallow writes by clients, returning an error and letting the client know which node is the leader for retry.

Proactive release of lease by leader on shutdown - if leader is shutting down for maintenance, it will proactively release the lease.

What happens if the leader node can't replicate to all followers? In this scenario, the leader node must replicate the batch to S3, similar to how AutoMQ works. This is done because the follower that is unavailable may become the next leader. If this happens, it needs to catch up on the latest batches for that aggregate. There are no guarantees that the previous leader is available (for example, rolling updates) so those batches must be retrieved from S3. The downside of this design is increased latency for writes and increased cost - S3 is now part of the hot path. This is classified as 'degraded service mode'.

Overall the main concept of this design is to delegate logic typically implemented by raft to a S3 control plane. It has to be a single S3 region to support conditional writes. An interesting side affect of this design is that it's possible to run a two node cluster.

We must maintain the high performance of the hot path in terms of high throughput 300k+ writes/second and low latency (<10ms) by caching data and avoiding constant access of data in the S3 Control Plane.

How do leaders know who to replicate to? The control plane maintains a single file which contains cluster membership:
{
    members: [
        {
            "id": "u128 unique id of node",
            "address": "ip:port of node",
            "is_active": "node may be inactive due to maintenance",
        }
    ]
}

Nodes proactively check they agree on time. This is because it's critical to avoid clock skew as we use time-based locks for gaining leadership. We also need a safety margin of time between actions such as lease renewal as we must accept a certain amount of clock skew and network latency.

Replication is done via TCP, but with a glommio specific client. The happy path of write is done first, and we provide the event batch (not just the events). The follower may error if they are missing previous batches. In this case, the leader will retry, providing the additional required batches. It may take multiple requests depending on the size of the batches as we cannot exceed the maximum message payload size.

When writing on the leader, there are scenarios where the data is written locally and fsync'd, but replication AND S3 replication all fail. In this scenario we must revert the local write and error back to the client.

Nodes in the cluster now need an ID. This should be generated at startup and stored in the data directory. We also need to write that ID to each event batch, together with the lease index number. It should be added to the metadata as well.

We don't need crypto, there is a trust boundary in the cluster and we accept all nodes play fair.