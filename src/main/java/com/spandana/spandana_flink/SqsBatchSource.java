

package com.spandana.spandana_flink;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import java.util.List;

/**
 * A Flink SourceFunction that polls AWS SQS for messages in batches.
 * Emits each SQS Message as a Flink record.
 */

public class SqsBatchSource implements ParallelSourceFunction<SimpleSqsMessage> {
    private final String queueUrl;
    private final String region;
    private final String sqsEndpoint;
    private final String awsUsername;
    private final String awsPassword;
    private final int batchSize;
    private final int waitTimeSeconds;
    private volatile boolean running = true;

    public SqsBatchSource(String queueUrl, String region, String sqsEndpoint, String awsUsername, String awsPassword) {
        this(queueUrl, region, sqsEndpoint, awsUsername, awsPassword, 10, 10);
    }

    public SqsBatchSource(String queueUrl, String region, String sqsEndpoint, String awsUsername, String awsPassword, int batchSize, int waitTimeSeconds) {
        this.queueUrl = queueUrl;
        this.region = region;
        this.sqsEndpoint = sqsEndpoint;
        this.awsUsername = awsUsername;
        this.awsPassword = awsPassword;
        this.batchSize = batchSize;
        this.waitTimeSeconds = waitTimeSeconds;
    }

    @Override
    public void run(SourceContext<SimpleSqsMessage> ctx) throws Exception {
        SqsClient sqsClient = SqsClient.builder()
                .endpointOverride(java.net.URI.create(sqsEndpoint))
                .region(software.amazon.awssdk.regions.Region.of(region))
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(awsUsername, awsPassword)))
                .build();
        while (running) {
            ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(batchSize)
                    .waitTimeSeconds(waitTimeSeconds)
                    .build();
            List<Message> messages = sqsClient.receiveMessage(request).messages();
            for (Message msg : messages) {
                SimpleSqsMessage simpleMsg = new SimpleSqsMessage();
                simpleMsg.setMessageId(msg.messageId());
                simpleMsg.setBody(msg.body());
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(simpleMsg);
                }
                // Immediately delete the message after collecting
                try {
                    sqsClient.deleteMessage(builder -> builder.queueUrl(queueUrl).receiptHandle(msg.receiptHandle()));
                } catch (Exception e) {
                    System.err.println("Failed to delete SQS message: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
