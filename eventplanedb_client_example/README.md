# Example client usage (rust)

Perform high concurrency writes, checking for errors, recording througput and latency:
```
cargo run --bin batch_main -p eventplanedb_client_example --release
```

Simple single connection write and then read back:
```
cargo run --bin single_main -p eventplanedb_client_example --release
```

Test for maximum write path performance (cached byte packets, thow away response bytes):
```
cargo run --bin performance_main -p eventplanedb_client_example --release -- 127.0.0.1:10000 11000 16 20 30
```

You should get results:
```
TCP Client (minimal work)
Server: 127.0.0.1:10000
Connections: 11000
Aggregates: 16
Sync delay (us): 20
Duration (s): 30
Completed: 10178035 requests in 30.23s -> 336733.7 RPS
```

To run without fsync:
```
cargo run --bin performance_main -p eventplanedb_client_example --release -- 127.0.0.1:10000 256 32 0 30
```

You should get results:
```
TCP Client (minimal work)
Server: 127.0.0.1:10000
Connections: 256
Aggregates: 32
Sync delay (us): 0
Duration (s): 30
Completed: 70978671 requests in 30.02s -> 2364208.9 RPS
```

# Running the server

Run the server as single node mode (no s3):
```
cargo run -p eventplanedb_server --release
```

Run the server with s3 as the control plane (specify bucket and optional subfolder):
```
./eventplanedb_client_example/s3_server.sh eventplanedb-testing test-ubuntu-pc
```

# AWS S3 Setup Prerequisites

To run the S3 control plane server (`./eventplanedb_client_example/s3_server.sh`), you must have the AWS CLI configured with credentials that have read/write access to a specified S3 bucket. The script uses the `default` AWS CLI profile.

1.  **Install and Configure AWS CLI**
    Ensure the AWS CLI is installed and configured. If you haven't done so, run `aws configure` to set up your keys and region, or manually ensure your configuration files exist:

    *   `~/.aws/credentials`: Stores `aws_access_key_id` and `aws_secret_access_key` under the `[default]` profile.
    *   `~/.aws/config`: Stores the `region` under the `[default]` profile.

2.  **Create S3 Bucket**
    Create a dedicated S3 bucket (e.g., `eventplanedb-testing`) in the region you configured.

3.  **Create IAM User and Policy (Recommended)**
    For security, create a dedicated IAM user and attach a policy that grants read and write access only to your specific bucket.

    Example minimal policy (replace `eventplanedb-testing` with your bucket name):

    ```json
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ,
                "Resource": [
                    "arn:aws:s3:::eventplanedb-testing",
                    "arn:aws:s3:::eventplanedb-testing/*"
                
            }
        ]
    }
    ```