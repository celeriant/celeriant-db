use eventplanedb_structures::request::Request;

use crate::leasing_channel::{LeaseRequestMsg, LeaseResponseMsg};


pub struct Msg {
    pub fd: i32,
    pub value: Option<Request>,
    pub message_version: u32,
    pub require_shutdown: bool,
    pub lease_request: Option<LeaseRequestMsg>,
    pub lease_response: Option<LeaseResponseMsg>,
}