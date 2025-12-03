package com.spandana.spandana_flink;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.file.*;
import java.util.zip.*;
import java.util.regex.Pattern;

public class ZipImportJob {
    private static final Logger logger = LoggerFactory.getLogger(ZipImportJob.class);

    private static final Pattern HASHED_PATTERN = Pattern.compile("^[a-f0-9]{29}\\.\\d_\\d+$", Pattern.CASE_INSENSITIVE);
    private static final Pattern EML_PATTERN = Pattern.compile(".*\\.eml$", Pattern.CASE_INSENSITIVE);

    /**
     * Extracts all email files from the given zip file (downloaded from S3) to a temp directory,
     * and emits each email file path to the provided collector. No manual threading or queueing.
     * This method is intended to be called from a Flink flatMap for native parallelism.
     */
    public static void extractAndEmitEmailsFromZip(AwsConfig awsConfig, String bucket, String key, org.apache.flink.util.Collector<Object> collector) throws Exception {
        Path tmpDirPath = Files.createTempDirectory("zipimport_");
        tmpDirPath.toFile().deleteOnExit();
        String tmpDir = tmpDirPath.toString();
        String localZip = tmpDir + "/" + new File(key).getName();

        // Download zip from S3
        S3Client s3 = awsConfig.createS3Client();
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build();
        s3.getObject(getObjectRequest, Paths.get(localZip));

        // Delete the zip file from S3 after download
        try {
            s3.deleteObject(b -> b.bucket(bucket).key(key));
            logger.info("Deleted zip file from S3: {}/{}", bucket, key);
        } catch (Exception e) {
            logger.warn("Failed to delete zip file from S3: {}/{}", bucket, key, e);
        }

        // Extract zip and emit each email file
        try (ZipInputStream zipFile = new ZipInputStream(new FileInputStream(localZip))) {
            ZipEntry entry;
            byte[] buffer = new byte[16 * 1024];
            Path destDirPath = Paths.get(tmpDir).toAbsolutePath().normalize();
            while ((entry = zipFile.getNextEntry()) != null) {
                File outFile = new File(tmpDir, entry.getName());
                Path outPath = outFile.toPath().toAbsolutePath().normalize();
                // Zip Slip protection
                if (!outPath.startsWith(destDirPath)) {
                    zipFile.closeEntry();
                    continue;
                }
                // Exclude macOS system files and folders
                String entryName = entry.getName();
                if (entryName.startsWith("__MACOSX") || entryName.startsWith("._") || entryName.startsWith("MAC_")) {
                    zipFile.closeEntry();
                    continue;
                }
                if (entry.isDirectory()) {
                    outFile.mkdirs();
                    zipFile.closeEntry();
                } else {
                    outFile.getParentFile().mkdirs();
                    try (FileOutputStream fos = new FileOutputStream(outFile)) {
                        int len;
                        while ((len = zipFile.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    } catch (IOException e) {
                        // Suppress error log
                    }
                    zipFile.closeEntry();
                    // Only emit email files
                    if (isEmailExtension(outFile.getName())) {
                        collector.collect(outFile);
                    }
                }
            }
        }

        // Clean up temp dir after use
        deleteDirectoryRecursivelyStatic(new File(tmpDir));
        logger.info("🎉 ZipImportJob completed for {}.", key);
    }

    private static boolean isEmailExtension(String filename) {
        return HASHED_PATTERN.matcher(filename).matches() || EML_PATTERN.matcher(filename).matches();
    }

    // Static recursive delete for temp dir cleanup
    private static void deleteDirectoryRecursivelyStatic(File dir) throws IOException {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectoryRecursivelyStatic(file);
                }
            }
        }
        dir.delete();
    }

}