use std::{path::PathBuf, vec};

use clap::{Parser, Subcommand};
use eventplanedb_client::{AuthData, EventPlaneDBClient};
use eventplanedb_crypto::Crypto;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional name to operate on
    name: Option<String>,

    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// does testing things
    Test {
        /// lists test values
        #[arg(short, long)]
        list: bool,
    },
    RandomWorkload {
        /// Server URL to connect to
        server: String,

        /// Aggregate ID to use for the workload
        aggregate_id: String,

        /// Share key to use for authentication
        #[arg(long)]
        sharekey: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match &cli.command {
        Some(Commands::Test { list }) => {
            if *list {
                println!("Printing testing lists...");
            } else {
                println!("Not printing testing lists...");
            }
        }
        Some(Commands::RandomWorkload {
            server,
            aggregate_id,
            sharekey,
        }) => {
            println!("Server: {}", server);
            println!("Aggregate ID: {}", aggregate_id);
            if let Some(key) = sharekey {
                println!("Using sharekey: {}", key);
            }
            // Additional logic for workload generation would go here
            run_workload(server, aggregate_id, sharekey).await;
        }
        None => {}
    }
}

async fn run_workload(server: &str, aggregate_id: &str, share_key: &Option<String>) {
    let keypair = Crypto::generate_keypair(None).unwrap();
    let nonce = Crypto::generate_nonce().unwrap();
    let sign = Crypto::sign_nonce(&keypair.private_key_base64, &nonce).unwrap();

    let auth_data = AuthData {
        public_key: keypair.public_key_base64.clone(),
        nonce,
        sign,
        bearer_token: None,
    };

    let mut from_server_id: i64 = 0;

    if let Some(share_key) = share_key {
        println!("Using sharekey: {}", share_key);
        let event_batches = EventPlaneDBClient::read_events(server, &auth_data, aggregate_id, from_server_id, Some(share_key.to_string()), false)
            .await
            .unwrap();
        if (!event_batches.is_empty()) {
            from_server_id = event_batches[event_batches.len() - 1].si;
            println!("Read {} event batches from server", event_batches.len());
        }
    } else {
        let initial_event = eventplanedb_client::ServerEvent {
            //current time ms
            ed: chrono::Utc::now().timestamp_millis(),
            tp: 1,
            vu: Some(vec![64]),
            iv: None,
            vi: None,
            vf: None,
            vd: None,
            vb: None,
            sv: None,
            by: None,
        };
        EventPlaneDBClient::write_events(server, &auth_data, aggregate_id, true, vec![initial_event])
            .await
            .unwrap();

        let share_response = EventPlaneDBClient::share(
            server,
            &auth_data,
            aggregate_id,
            eventplanedb_client::AccessLevel::Contributor,
            false,
            None,
            None,
            0,
        )
        .await
        .unwrap();

        println!("Share key created: {}", share_response.share_key);
    }

    while (true) {
        let color_events = generate_color_events(50);

        let nonce = Crypto::generate_nonce().unwrap();
        let sign = Crypto::sign_nonce(&keypair.private_key_base64, &nonce).unwrap();

        let auth_data = AuthData {
            public_key: keypair.public_key_base64.clone(),
            nonce,
            sign,
            bearer_token: None,
        };

        EventPlaneDBClient::write_events(server, &auth_data, aggregate_id, true, color_events)
            .await
            .unwrap();
    }
}

fn generate_color_events(count: usize) -> Vec<eventplanedb_client::ServerEvent> {
    let mut events = Vec::new();
    for _ in 0..count {
        let x_pos: u64 = (rand::random::<u8>() % 64).into();
        let y_pos: u64 = (rand::random::<u8>() % 64).into();
        let r: u64 = rand::random::<u8>() as u64;
        let g: u64 = rand::random::<u8>() as u64;
        let b: u64 = rand::random::<u8>() as u64;
        let a: u64 = rand::random::<u8>() as u64;

        let random_color: u64 = (r << 24) | (g << 16) | (b << 8) | a;

        let color_event = eventplanedb_client::ServerEvent {
            //current time ms
            ed: chrono::Utc::now().timestamp_millis(),
            tp: 0,
            vu: Some(vec![x_pos, y_pos, random_color]),
            iv: None,
            vi: None,
            vf: None,
            vd: None,
            vb: None,
            sv: None,
            by: None,
        };
        events.push(color_event);
    }
    events
}
