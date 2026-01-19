/// The role of this node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterRole {
    /// Standalone node - no replication
    Standalone,
    /// Leader node - replicates to follower
    Leader,
    /// Follower node - receives replication from leader
    Follower,
}
