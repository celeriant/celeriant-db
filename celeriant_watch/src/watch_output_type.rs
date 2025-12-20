use celeriant_msg::response::responses::WatchResponse;

/// The types of actions the shard needs to take when
/// interacting with a client over a live tcp connection
#[derive(Debug, Clone)]
pub enum WatchOutputType {

    /// We have batched up a bunch of events and require
    /// immediate sending to client
    Response(WatchResponse),

    /// Nothing has happened over the heartbeat period
    /// Yielding to allow sending a heartbeat back to client
    Heartbeat,

    /// Clean close requested as we have encountered
    /// a channel error or another error forcing close
    Done,

    /// Indicates we are accumulating events, but
    /// not ready to flush to client yet or heartbeat
    Continue,
}