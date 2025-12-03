#!/bin/bash
set -e

# Copy file from host to LocalStack container
docker cp /Users/spapatil/Desktop/Officework/Work/emails_spatil.zip localstack-flink:/tmp/spatil.zip
echo "Successfully copied to localstack-flink:/tmp/spatil.zip"

# Upload file from LocalStack container to S3 bucket
docker exec localstack-flink awslocal s3 cp /tmp/spatil.zip s3://my-bucket/spatil.zip
echo "Uploaded to s3://my-bucket/spatil.zip"