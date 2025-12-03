
package com.spandana.spandana_flink;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSink;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSinkBuilder;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;


public class EntryMainJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final AwsConfig awsConfig = AwsConfig.getInstance();
        final String queueUrl = awsConfig.getQueueUrl();
        final String region = awsConfig.getRegion();
        final String sqsEndpoint = awsConfig.getSqsEndpoint();
        final String awsUsername = awsConfig.getAwsUsername();
        final String awsPassword = awsConfig.getAwsPassword();
        final String jobName = awsConfig.getJobName();
        final String sourceBucket = awsConfig.getSourceBucket();

        // Step 0: SQS Source
        DataStream<SimpleSqsMessage> source =
            env.addSource(new SqsBatchSource(queueUrl, region, sqsEndpoint, awsUsername, awsPassword))
               .name("SQS Source")
               .setParallelism(4);

        // Step 1: Extract zip or email
        DataStream<Object> extracted = source.flatMap((SimpleSqsMessage message,
                                   Collector<Object> out) -> {
            try {
                String body = message.getBody();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(body);
                JsonNode records = root.path("Records");
                if (!records.isArray() || records.size() == 0) {
                    logger.error("SQS message missing 'Records' array or is empty: {}", body);
                    return;
                }
                JsonNode record = records.get(0);
                if (record == null) {
                    logger.error("First record in 'Records' is null: {}", body);
                    return;
                }

                String bucket = record.path("s3").path("bucket").path("name").asText("");
                String key = record.path("s3").path("object").path("key").asText("");
                if (bucket.isEmpty() || key.isEmpty()) {
                    logger.error("Missing bucket or key in SQS message: {}", body);
                    return;
                }

                logger.info("[SQS] Received event for bucket: '{}' key: '{}'", bucket, key);

                AwsConfig config = AwsConfig.getInstance();

                if (key.toLowerCase().endsWith(".zip")) {
                    // Emit each email file path from the zip
                    try {
                        ZipImportJob.extractAndEmitEmailsFromZip(config, bucket, key, out);
                    } catch (Exception e) {
                        logger.error("Error extracting zip file: {}/{}", bucket, key, e);
                    }
                } else {
                    out.collect(body);
                }
            } catch (Exception e) {
                logger.error("Error parsing SQS message", e);
            }
        })
        .returns(Object.class)
        .name("Extract zip or email")
        .setParallelism(4);

        // Step 2: Process to ES documents
        DataStream<Map<String, Object>> docs = extracted.flatMap((Object input,
                                      Collector<Map<String, Object>> out) -> {
            try {
                if (input instanceof File) {
                    File emailFile = (File) input;
                    AwsConfig config = AwsConfig.getInstance();
                    Map<String, Object> doc = EmailProcessor.processEmail(emailFile, config, sourceBucket);
                    if (doc != null) {
                        // logger.info("[EMAIL] Successfully processed: {}", emailFile.getName());
                        out.collect(doc);
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing email input", e);
            }
        })
        .returns(new TypeHint<Map<String, Object>>() {})
        .name("Process emails")
        .setParallelism(8);

        // Step 3: Elasticsearch 8 Async Sink with DLQ fallback
        String indexName = awsConfig.getTenantId().toLowerCase();
        String esHost = awsConfig.getEsHost();
        int esPort = awsConfig.getEsPort();
        String esScheme = awsConfig.getEsScheme();
        // Get DLQ queue URL from config
        final String dlqQueueUrl = awsConfig.getDeadLetterQueueUrl();



        // DLQ logic using AsyncFunction for record-level fault tolerance
        class ElasticsearchWithDLQAsyncFunction implements AsyncFunction<Map<String, Object>, Map<String, Object>> {
            private transient SqsClient dlqClient;

            public void open(Configuration parameters) {
                dlqClient = SqsClient.builder()
                    .endpointOverride(java.net.URI.create(sqsEndpoint))
                    .region(Region.of(region))
                    .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(awsUsername, awsPassword)))
                    .build();
            }

            public void asyncInvoke(Map<String, Object> doc, ResultFuture<Map<String, Object>> resultFuture) {
                CompletableFuture.runAsync(() -> {
                    try {
                        resultFuture.complete(java.util.Collections.singleton(doc));
                    } catch (Exception e) {
                        // If timeout or failure, send to DLQ
                        try {
                            dlqClient.sendMessage(builder -> builder.queueUrl(dlqQueueUrl).messageBody(doc.toString()));
                            logger.warn("Sent failed record to DLQ: {}", doc);
                        } catch (Exception dlqEx) {
                            logger.error("Failed to send to DLQ: {}", dlqEx.getMessage());
                        }
                        resultFuture.complete(java.util.Collections.emptyList());
                    }
                });
            }
        }

        Elasticsearch8AsyncSink<Map<String, Object>> esSink =
            new Elasticsearch8AsyncSinkBuilder<Map<String, Object>>() 
                .setHosts(new HttpHost(esHost, esPort, esScheme))
                .setHeaders(new Header[] {
                    new BasicHeader("Authorization", "ApiKey " + System.getenv("ELASTICSEARCH_API_KEY"))
                })
                .setElementConverter((element, context) ->
                    IndexOperation.<Map<String, Object>>of(b -> b
                        .index(indexName)
                        .document(element)
                    )
                )
                .setMaxBatchSize(500) 
                .setMaxBufferedRequests(5000) 
                .setMaxTimeInBufferMS(2000) 
                .setMaxInFlightRequests(16) 
                .build();

        // Custom async operator to handle DLQ fallback
        AsyncDataStream.unorderedWait(
            docs.filter(Objects::nonNull).rebalance(),
            new ElasticsearchWithDLQAsyncFunction(),
            5000,
            TimeUnit.MILLISECONDS,
            100
        )
        .sinkTo(esSink)
        .name("Elasticsearch 8 Async Sink")
        .setParallelism(8);
        logger.info("Starting Flink job: {}", jobName);
        env.execute(jobName);
    }

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EntryMainJob.class);

}