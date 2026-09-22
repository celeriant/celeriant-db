//! Scripted fake Celeriant servers for the black-box routing contract tests.
//! Protocol handling mirrors `pool.rs`'s in-crate `write_server`.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_msg::response::responses::{ErrorResponse, WriteResponse};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
use tokio_util::compat::TokioAsyncReadCompatExt;

const MAX: u64 = 64 * 1024 * 1024;

#[derive(Clone)]
pub enum Answer {
    Ok,
    NotLeader(String),
    /// Answers earlier requests, then reads the nth one fully and closes
    /// the socket without answering it.
    CloseAfterNth(usize),
    /// The first request waits for `release`; later ones answer straight away.
    HoldFirst(Arc<AtomicBool>),
}

pub struct Fake {
    pub addr: SocketAddr,
    pub accepts: Arc<AtomicUsize>,
    pub requests: Arc<AtomicUsize>,
    generation: Arc<AtomicUsize>,
    live: Arc<AtomicUsize>,
}

impl Fake {
    pub fn accepts(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }

    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// Drops every accepted socket, as a restarted or idle-closing server would,
    /// and returns once the peer has been sent the FIN.
    pub async fn close_all(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        while self.live.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

pub fn bind() -> TcpListener {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    listener
}

/// An address nothing listens on: bound to claim it, then dropped.
pub fn dead_address() -> String {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string()
}

pub fn spawn(answer: Answer) -> Fake {
    serve(bind(), answer)
}

/// `DictCodec` holds a `RefCell`, so each server owns a thread, a runtime and a `LocalSet`.
pub fn serve(listener: TcpListener, answer: Answer) -> Fake {
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let generation = Arc::new(AtomicUsize::new(0));
    let live = Arc::new(AtomicUsize::new(0));
    let (accepts_t, requests_t) = (accepts.clone(), requests.clone());
    let (generation_t, live_t) = (generation.clone(), live.clone());

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            while let Ok((socket, _)) = listener.accept().await {
                accepts_t.fetch_add(1, Ordering::SeqCst);
                let (answer, requests) = (answer.clone(), requests_t.clone());
                let (generation, live) = (generation_t.clone(), live_t.clone());
                live.fetch_add(1, Ordering::SeqCst);
                tokio::task::spawn_local(async move {
                    let born = generation.load(Ordering::SeqCst);
                    tokio::select! {
                        _ = connection(socket, answer, requests) => {}
                        _ = closed(&generation, born) => {}
                    }
                    live.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
    });

    Fake { addr, accepts, requests, generation, live }
}

async fn closed(generation: &AtomicUsize, born: usize) {
    while generation.load(Ordering::SeqCst) == born {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn connection(socket: tokio::net::TcpStream, answer: Answer, requests: Arc<AtomicUsize>) {
    let mut stream = socket.compat();
    let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).unwrap();
    loop {
        let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else {
            return;
        };
        let Ok(request) = ClientRequest::read_from_header(header, &mut stream, &codec).await else {
            return;
        };
        let nth = requests.fetch_add(1, Ordering::SeqCst) + 1;
        let asked = request.correlation_id();

        if let Answer::HoldFirst(release) = &answer {
            while nth == 1 && !release.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        let resp = match &answer {
            Answer::CloseAfterNth(n) if nth >= *n => return,
            Answer::CloseAfterNth(_) => ClientResponse::Write(WriteResponse {
                correlation_id: asked,
                max_aggregate_version: Some(1),
            }),
            Answer::NotLeader(leader) => ClientResponse::GenericError(ErrorResponse {
                correlation_id: asked,
                error_code: celeriant_msg::error_codes::WRITE_NOT_LEADER,
                error_message: format!("{{\"leader_address\":\"{leader}\"}}"),
            }),
            Answer::Ok | Answer::HoldFirst(_) => ClientResponse::Write(WriteResponse {
                correlation_id: asked,
                max_aggregate_version: Some(1),
            }),
        };
        if ClientResponse::write_response(
            &mut stream, &resp, false, &codec, MAX, PROTOCOL_VERSION_V2,
        )
        .await
        .is_err()
        {
            return;
        }
    }
}

pub fn event(payload: Vec<u8>) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(payload),
        iv: None,
    }
}

pub fn write_request(aggregate_id: u128, payload: Vec<u8>) -> WriteRequest {
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, aggregate_id),
        SingleAggregateWrite {
            events: vec![event(payload)],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );
    WriteRequest { correlation_id: None, client_id: 1, user_id: None, writes }
}

/// Deterministic pseudo-random bytes: compression must not shrink them.
pub fn incompressible(len: usize) -> Vec<u8> {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}
