#!/bin/bash
set -e

echo "🪣 Creating S3 bucket: my-bucket"
awslocal s3 mb s3://my-bucket


echo "📬 Creating SQS queue: my-queue"
awslocal sqs create-queue --queue-name my-queue

echo "📬 Creating SQS queue: dead-letter-queue"
awslocal sqs create-queue --queue-name dead-letter-queue

echo "✅ Resources created successfully!"

echo "🔗 Getting SQS queue ARN..."
QUEUE_URL=$(awslocal sqs get-queue-url --queue-name my-queue --query 'QueueUrl' --output text)
QUEUE_ARN=$(awslocal sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-name QueueArn --query 'Attributes.QueueArn' --output text)

echo "🔒 Attaching SQS policy to allow S3 to send messages..."
awslocal sqs set-queue-attributes \
	--queue-url "$QUEUE_URL" \
	--attributes '{
		"Policy": "{\n  \"Version\": \"2012-10-17\",\n  \"Statement\": [\n    {\n      \"Effect\": \"Allow\",\n      \"Principal\": {\n        \"Service\": \"s3.amazonaws.com\"\n      },\n      \"Action\": \"sqs:SendMessage\",\n      \"Resource\": \"'$QUEUE_ARN'\",\n      \"Condition\": {\n        \"ArnLike\": {\n          \"aws:SourceArn\": \"arn:aws:s3:::my-bucket\"\n        }\n      }\n    }\n  ]\n}"
	}'

echo "🔔 Configuring S3 event notification to SQS..."
awslocal s3api put-bucket-notification-configuration \
  --bucket my-bucket \
  --notification-configuration '{
    "QueueConfigurations": [
      {
        "QueueArn": "'$QUEUE_ARN'",
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [
              {"Name": "suffix", "Value": ".zip"}
            ]
          }
        }
      }
    ]
  }'


echo "🚀 S3 event notification to SQS configured!"
