#!/bin/bash

# Configuration
PROFILE_NAME="default"

# Check for AWS CLI dependency
if ! command -v aws &> /dev/null
then
    echo "Error: AWS CLI is not installed or not in PATH."
    exit 1
fi

echo "--- Loading AWS Credentials (Profile: $PROFILE_NAME) ---"

# 1. Extract Credentials
S3_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile $PROFILE_NAME 2>/dev/null)
S3_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile $PROFILE_NAME 2>/dev/null)

if [ -z "$S3_ACCESS_KEY_ID" ]  || [ -z "$S3_SECRET_ACCESS_KEY" ]; then
    echo "Error: Could not retrieve AWS access keys for profile '$PROFILE_NAME'."
    echo "Please ensure your credentials file (~/.aws/credentials) is configured correctly."
    exit 1
fi

# 2. Extract Region
S3_REGION=$(aws configure get region --profile $PROFILE_NAME 2>/dev/null)

if [ -z "$S3_REGION" ]; then
    echo "Error: Could not retrieve AWS region for profile '$PROFILE_NAME'."
    echo "Please ensure your config file (~/.aws/config) is configured correctly."
    exit 1
fi

# 3. Handle Bucket Name and Subfolder
S3_BUCKET="$1"
S3_SUBFOLDER="$2"

if [ -z "$S3_BUCKET" ]; then
    read -rp "Enter the S3 Bucket Name for the eventplane server (Argument 1): " S3_BUCKET
fi

if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3 Bucket Name cannot be empty."
    exit 1
fi

if [ -z "$S3_SUBFOLDER" ]; then
    S3_SUBFOLDER=""
fi

# For debug purposes only
# echo "S3_ACCESS_KEY_ID '$S3_ACCESS_KEY_ID'"
# echo "S3_SECRET_ACCESS_KEY '$S3_SECRET_ACCESS_KEY'"
# echo "S3_REGION '$S3_REGION'"
# echo "S3_BUCKET '$S3_BUCKET'"
# echo "S3_SUBFOLDER '$S3_SUBFOLDER'"

CMD_ARGS="--s3-enabled --s3-region $S3_REGION --s3-bucket $S3_BUCKET --s3-access-key-id $S3_ACCESS_KEY_ID --s3-secret-access-key $S3_SECRET_ACCESS_KEY"

if [ -n "$S3_SUBFOLDER" ]; then
    CMD_ARGS="$CMD_ARGS --s3-subfolder $S3_SUBFOLDER"
fi

cargo run -p eventplanedb_server --release -- $CMD_ARGS