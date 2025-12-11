use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::{process_requests::Request, request::requests::ExistsRequest};
use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect("0.0.0.0:10000").await?;
    let request = Request::Exists(ExistsRequest {
        aggregate_key: AggregateKey::new(1, 1, 1),
        correlation_id: None
    });
    let response = client.send_request(&request, CompressionType::None).await?;
    println!("{:?}", response);

    //Change aggregate
    let request = Request::Exists(ExistsRequest {
        aggregate_key: AggregateKey::new(1, 1, 2),
        correlation_id: None
    });
    let response = client.send_request(&request, CompressionType::None).await?;
    println!("{:?}", response);

    Ok(())
}
