//! Verifies the subfolder round-trip invariant against real AWS S3.
//!
//! `list_objects` / `head_object` must return paths *relative to the subfolder*
//! so callers can feed them straight back to `get_object` without the prefix
//! doubling up. The MinIO-backed tests already exercise this, but real S3 has
//! slightly different listing semantics, so this test is the load-bearing one.
//!
//! Run with:
//!   eval "$(aws configure export-credentials --format env-no-export)"
//!   export CELERIANT_REAL_S3_BUCKET=your-bucket
//!   export CELERIANT_REAL_S3_REGION=us-east-1   # optional, defaults to us-east-1
//!   cargo test -p celeriant_sidecar --release --test real_s3_subfolder -- --ignored --nocapture
//!
//! The test creates objects under a random per-run subfolder
//! (`celeriant-roundtrip-{uuid}/`) and deletes them on the way out. Costs are
//! a handful of PUT/GET/LIST/DELETE requests.

use bytes::Bytes;
use celeriant_sidecar::request::{PutCondition, Request};
use celeriant_sidecar::response::Response;
use celeriant_sidecar::s3_config::S3Config;
use celeriant_sidecar::store::{SidecarStore, SidecarStoreTrait};
use celeriant_sidecar::store_config::StoreConfig;

fn env_or<F: FnOnce() -> String>(key: &str, default: F) -> String {
    std::env::var(key).unwrap_or_else(|_| default())
}

fn random_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}{:x}", nanos, std::process::id())
}

#[tokio::test]
#[ignore = "talks to real AWS S3; set CELERIANT_REAL_S3_BUCKET to enable"]
async fn list_then_get_roundtrips_under_subfolder() {
    let Ok(bucket) = std::env::var("CELERIANT_REAL_S3_BUCKET") else {
        panic!("CELERIANT_REAL_S3_BUCKET not set; see file header for setup");
    };
    let region = env_or("CELERIANT_REAL_S3_REGION", || "us-east-1".to_string());
    let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
    let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

    let subfolder = format!("celeriant-roundtrip-{}", random_suffix());
    eprintln!("subfolder: {subfolder}");
    if session_token.is_some() {
        eprintln!("using AWS_SESSION_TOKEN (SSO/temp creds)");
    }

    let s3 = S3Config {
        region: region.clone(),
        bucket: bucket.clone(),
        access_key_id: access_key.clone(),
        secret_access_key: secret_key.clone(),
        session_token: session_token.clone(),
        subfolder: Some(subfolder.clone()),
        endpoint: None,
        skip_signature: false,
        allow_http: false,
    };

    let store = SidecarStore::new(StoreConfig { s3: Some(s3) })
        .expect("build SidecarStore");

    let logical_path = "cluster/fallback/shard_000/batch_000000001_000000005_test.bin";
    let payload = Bytes::from_static(b"roundtrip-canary");

    let put = store
        .process_request(Request::ObjectPut {
            path: logical_path.to_string(),
            data: payload.clone(),
            condition: PutCondition::None,
        })
        .await
        .expect("PUT must succeed");
    assert!(matches!(put, Response::ObjectPut { .. }));

    let listed = store
        .process_request(Request::ObjectList {
            prefix: "cluster/fallback/shard_000/".to_string(),
        })
        .await
        .expect("LIST must succeed");
    let Response::ObjectList { objects } = listed else {
        panic!("expected ObjectList response");
    };
    assert!(!objects.is_empty(), "LIST returned no objects");

    let returned = objects
        .iter()
        .find(|o| o.path.ends_with("test.bin"))
        .unwrap_or_else(|| panic!("PUT object not found in LIST; got: {:?}", objects));

    assert_eq!(
        returned.path, logical_path,
        "list_objects must return paths relative to subfolder (no '{}/' prefix). \
         Got '{}'; subfolder doubling will break get_object.",
        subfolder, returned.path
    );

    let got = store
        .process_request(Request::ObjectGet {
            path: returned.path.clone(),
        })
        .await
        .expect(
            "GET must succeed with the path LIST returned. If this fails with NotFound, \
             the subfolder is being prepended twice (the bug this test exists to catch).",
        );
    let Response::ObjectGet { data, .. } = got else {
        panic!("expected ObjectGet response");
    };
    assert_eq!(data.as_ref(), payload.as_ref(), "GET payload mismatch");

    let head = store
        .process_request(Request::ObjectHead {
            path: logical_path.to_string(),
        })
        .await
        .expect("HEAD must succeed");
    let Response::ObjectHead(meta) = head else {
        panic!("expected ObjectHead response");
    };
    assert_eq!(
        meta.path, logical_path,
        "head_object must also return a subfolder-relative path"
    );

    let _ = store
        .process_request(Request::ObjectDelete {
            path: logical_path.to_string(),
        })
        .await;
}
