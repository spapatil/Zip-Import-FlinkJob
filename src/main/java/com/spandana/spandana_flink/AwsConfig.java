package com.spandana.spandana_flink;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.util.Map;

public class AwsConfig {
    private static AwsConfig instance;
    private final Map<String, Object> config;

    // AWS
    private final String s3Endpoint, sqsEndpoint, region, queueUrl, awsUsername, awsPassword;
    // Job
    private final String jobName, tenantId, sourceBucket, deadLetterQueueUrl;
    // Elasticsearch
    private final String esApiKey, esHost, esScheme;
    private final int esPort;

    private AwsConfig() {
        try {
            this.config = ConfigLoader.loadConfig("config.yaml");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config.yaml", e);
        }
        this.s3Endpoint = getStringConfig("s3Endpoint");
        this.sqsEndpoint = getStringConfig("sqsEndpoint");
        this.region = getStringConfig("region");
        this.queueUrl = getStringConfig("queueUrl");
        this.awsUsername = getStringConfig("awsUsername");
        this.awsPassword = getStringConfig("awsPassword");
        this.jobName = getStringConfig("jobName");
        this.tenantId = getStringConfig("tenantId");
        this.esApiKey = getStringConfig("esApiKey");
        this.esHost = getStringConfig("esHost");
        this.esPort = getIntConfig("esPort");
        this.esScheme = getStringConfig("esScheme");
        this.sourceBucket = getStringConfig("sourceBucket");
        this.deadLetterQueueUrl = getStringConfig("deadLetterQueueUrl");
    }

    private String getStringConfig(String key) {
        Object value = config.get(key);
        if (value == null) {
            System.err.println("[AwsConfig] Missing required config key: " + key);
            throw new IllegalArgumentException("Missing required config key: " + key);
        }
        return value.toString();
    }

    private int getIntConfig(String key) {
        Object value = config.get(key);
        if (value == null) {
            System.err.println("[AwsConfig] Missing required int config key: " + key);
            throw new IllegalArgumentException("Missing required int config key: " + key);
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            System.err.println("[AwsConfig] Invalid int for key: " + key + ", value: " + value);
            throw new IllegalArgumentException("Invalid int for key: " + key + ", value: " + value, e);
        }
    }

    public static synchronized AwsConfig getInstance() {
        if (instance == null) instance = new AwsConfig();
        return instance;
    }

    public Map<String, Object> getConfig() { return config; }
    // AWS
    public String getS3Endpoint() { return s3Endpoint; }
    public String getSqsEndpoint() { return sqsEndpoint; }
    public String getRegion() { return region; }
    public String getQueueUrl() { return queueUrl; }
    public String getAwsUsername() { return awsUsername; }
    public String getAwsPassword() { return awsPassword; }
    // Job
    public String getJobName() { return jobName; }
    public String getTenantId() { return tenantId; }
    public String getSourceBucket() { return sourceBucket; }
    public String getDeadLetterQueueUrl() { return deadLetterQueueUrl; }
    
    // Elasticsearch
    public String getEsApiKey() { return esApiKey; }
    public String getEsHost() { return esHost; }
    public int getEsPort() { return esPort; }
    public String getEsScheme() { return esScheme; }

    public SqsClient createSqsClient() {
        return SqsClient.builder()
                .endpointOverride(URI.create(sqsEndpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(awsUsername, awsPassword)))
                .build();
    }

    public S3Client createS3Client() {
        return S3Client.builder()
            .endpointOverride(URI.create(s3Endpoint))
            .region(Region.of(region))
            .forcePathStyle(true)
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(awsUsername, awsPassword)))
            .build();
    }
}
