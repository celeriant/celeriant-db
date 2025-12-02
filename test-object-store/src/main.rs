// use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
// use object_store::{ObjectStore, path::Path};
// use bytes::Bytes;

// #[tokio::main]
// async fn main() -> anyhow::Result<()> {

//     let bucket = "eventplanedb-testing";

//     let store = AmazonS3Builder::from_env()
//         .with_bucket_name(bucket)
//         .build()?;

//     println!("Created S3 client for bucket: {}", bucket);

//     //
//     // Write file
//     //
//     let data = Bytes::from("Hello from Rust object_store!\n");
//     let path = Path::from("hello.txt");

//     store.put(&path, data.into()).await?;

//     println!("Uploaded {}", path);
//     Ok(())
// }


use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use bytes::Bytes;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bucket = "eventplanedb-testing";

    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()?;

    println!("Created S3 client for bucket: {}", bucket);

    let path = Path::from("conditional-test.txt");

    // Clean up any existing file first
    let _ = store.delete(&path).await;

    //
    // Test 1: Competing "create" writes - only one should succeed
    //
    println!("\n=== Test 1: Competing CREATE writes ===");

    let mut tasks = JoinSet::new();
    for i in 0..5 {
        let store = store.clone();
        let path = path.clone();
        tasks.spawn(async move {
            let data = Bytes::from(format!("Writer {} was here!\n", i));
            let opts = PutOptions {
                mode: PutMode::Create, // Fail if object already exists
                ..Default::default()
            };
            (i, store.put_opts(&path, data.into(), opts).await)
        });
    }

    let mut winners = 0;
    let mut losers = 0;
    while let Some(result) = tasks.join_next().await {
        let (writer_id, put_result) = result?;
        match put_result {
            Ok(result) => {
                println!("Writer {} WON! ETag: {:?}", writer_id, result.e_tag);
                winners += 1;
            }
            Err(e) => {
                println!("Writer {} lost: {}", writer_id, e);
                losers += 1;
            }
        }
    }
    println!("Results: {} winner(s), {} loser(s)", winners, losers);
    assert_eq!(winners, 1, "Expected exactly one winner!");

    //
    // Test 2: Update with ETag matching (optimistic concurrency)
    //
    println!("\n=== Test 2: Competing UPDATE writes with ETag ===");

    // First, get the current version
    let meta = store.head(&path).await?;
    let current_version = UpdateVersion {
        e_tag: meta.e_tag.clone(),
        version: meta.version.clone(),
    };
    println!("Current ETag: {:?}", meta.e_tag);

    // Now try to update with multiple concurrent writers using the same ETag
    let mut tasks = JoinSet::new();
    for i in 0..5 {
        let store = store.clone();
        let path = path.clone();
        let version = current_version.clone();
        tasks.spawn(async move {
            let data = Bytes::from(format!("Updated by writer {}!\n", i));
            let opts = PutOptions {
                mode: PutMode::Update(version),
                ..Default::default()
            };
            (i, store.put_opts(&path, data.into(), opts).await)
        });
    }

    let mut winners = 0;
    let mut losers = 0;
    while let Some(result) = tasks.join_next().await {
        let (writer_id, put_result) = result?;
        match put_result {
            Ok(result) => {
                println!("Writer {} WON the update! New ETag: {:?}", writer_id, result.e_tag);
                winners += 1;
            }
            Err(e) => {
                println!("Writer {} lost (precondition failed): {}", writer_id, e);
                losers += 1;
            }
        }
    }
    println!("Results: {} winner(s), {} loser(s)", winners, losers);
    assert_eq!(winners, 1, "Expected exactly one winner!");

    // Show final content
    let result = store.get(&path).await?;
    let content = result.bytes().await?;
    println!("\nFinal file content: {}", String::from_utf8_lossy(&content));

    Ok(())
}