use celeriant_wire::{network::wire_error::WireError};

#[derive(Debug)]
pub enum ReadWireDataError {
    UnknownMessageType(u32),
    ReadHeaderFailure(WireError),
    ReadBodyFailure(WireError),
}