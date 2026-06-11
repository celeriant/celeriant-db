// Per-node aggregate_details probe: org 2 / type 1 / id from argv.
use celeriant_bench::PoolBuilder;
use celeriant_msg::request::requests::AggregateDetailsRequest;
use celeriant_wal::aggregate_key::AggregateKey;

#[tokio::main]
async fn main() {
    let id: u128 = std::env::args().nth(1).unwrap().parse().unwrap();
    for host in ["192.168.88.214", "192.168.88.213"] {
        let pool = PoolBuilder {
            address1: &format!("{host}:10000"),
            address2: &format!("{host}:10000"),
            server_name: Some(host),
            ca_cert: "deploy/rpi-cluster/certs/client-ca.crt",
            client_cert: "deploy/rpi-cluster/certs/client.crt",
            client_key: "deploy/rpi-cluster/certs/client.key",
            plaintext: false,
            max_connections: 1,
        }.build().await.unwrap();
        let d = pool.aggregate_details(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(2, 1, id),
        }).await;
        match d {
            Ok(d) => println!("{host}: deleted={} min={} max={} seq={}", d.is_deleted, d.min_aggregate_version, d.max_aggregate_version, d.max_event_seq),
            Err(e) => println!("{host}: ERR {e}"),
        }
    }
}
