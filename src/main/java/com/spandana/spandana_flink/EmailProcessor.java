package com.spandana.spandana_flink;

import java.nio.file.Files;

import java.io.File;
import java.io.FileInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;

public class EmailProcessor {
    // === Constants and Fields ===
    private static final Logger logger = LoggerFactory.getLogger(EmailProcessor.class);

    public static Map<String, Object> processEmail(File emailFile, AwsConfig awsConfig, String s3Bucket) throws Exception {
        String tenantId = awsConfig.getTenantId();
        try {
            // --- Streaming hash computation ---
            long fileSize = emailFile.length();
            byte[] fileBytes = Files.readAllBytes(emailFile.toPath());
            String sha1 = computeHash(fileBytes, "SHA-1");
            String md5 = computeHash(fileBytes, "MD5");

            // --- Streaming parse for MimeMessage ---
            Session session = Session.getDefaultInstance(new java.util.Properties());
            MimeMessage message;
            try (FileInputStream fis2 = new FileInputStream(emailFile)) {
                message = new MimeMessage(session, fis2);
            }

            String from = "";
            if (message.getFrom() != null && message.getFrom().length > 0 && message.getFrom()[0] != null) {
                from = formatAddress(message.getFrom()[0]);
            }
            String to = "";
            var recipients = message.getRecipients(jakarta.mail.Message.RecipientType.TO);
            if (recipients != null && recipients.length > 0 && recipients[0] != null) {
                to = formatAddress(recipients[0]);
            }
            String subject = message.getSubject();

            String sentDate = java.time.Instant.now().minus(java.time.Duration.ofDays(1)).toString();
            if (message.getSentDate() != null) {
                sentDate = message.getSentDate().toInstant().toString();
            }
            if (sentDate.contains("T")) {
                int dotIdx = sentDate.indexOf('.');
                int zIdx = sentDate.indexOf('Z');
                if (dotIdx > 0) {
                    sentDate = sentDate.substring(0, dotIdx) + "Z";
                } else if (zIdx < 0) {
                    sentDate = sentDate + "Z";
                }
            }
            String messageId = message.getMessageID();
            String body = getBody(message);

            List<String> contentTypes = new ArrayList<>();
            String ct = message.getContentType();
            if (ct != null) {
                for (String part : ct.split(";|,")) {
                    contentTypes.add(part.trim());
                }
            }

            String docId = sha1;
            String day = sentDate.substring(0, 10);
            String s3Key = String.format("tenant_id=%s/day=%s/%s.zstd", tenantId, day, docId);
            String s3Uri = String.format("s3://%s/%s", s3Bucket, s3Key);

            Map<String, Object> source = new LinkedHashMap<>();
            source.put("storage_location", s3Uri);
            boolean hasLinkedAttachment = false;
            String bodyText = body != null ? body : "";
            String htmlText = "";
            Object content = message.getContent();
            if (content instanceof String && message.isMimeType("text/html")) {
                htmlText = (String) content;
            } else if (message.isMimeType("multipart/*")) {
                jakarta.mail.Multipart mp = (jakarta.mail.Multipart) content;
                for (int i = 0; i < mp.getCount(); i++) {
                    jakarta.mail.BodyPart bp = mp.getBodyPart(i);
                    if (bp.isMimeType("text/html")) {
                        htmlText = (String) bp.getContent();
                        break;
                    }
                }
            }
            String textToSearch = !htmlText.isEmpty() ? htmlText : bodyText;
            if (textToSearch.toLowerCase().contains("sharepoint")) {
                java.util.regex.Pattern[] patterns = new java.util.regex.Pattern[] {
                    java.util.regex.Pattern.compile("href=([^\\s>]+)", java.util.regex.Pattern.CASE_INSENSITIVE),
                    java.util.regex.Pattern.compile("href=([\"'])(.+?)\\1", java.util.regex.Pattern.CASE_INSENSITIVE),
                    java.util.regex.Pattern.compile("https?://.+?(?=\\s|$)", java.util.regex.Pattern.CASE_INSENSITIVE),
                    java.util.regex.Pattern.compile("([\"'])(https?://.+?)\\1", java.util.regex.Pattern.CASE_INSENSITIVE)
                };
                for (java.util.regex.Pattern pattern : patterns) {
                    java.util.regex.Matcher matcher = pattern.matcher(textToSearch);
                    while (matcher.find()) {
                        String url = matcher.group(matcher.groupCount());
                        try {
                            java.net.URI uri = new java.net.URI(url.replaceAll("^[\"']+|[\"']+$", ""));
                            String host = uri.getHost();
                            if (host != null && host.toLowerCase().contains("sharepoint")) {
                                hasLinkedAttachment = true;
                                break;
                            }
                        } catch (Exception ignore) {}
                    }
                    if (hasLinkedAttachment) break;
                }
            }
            source.put("has_linked_attachment", hasLinkedAttachment);
            source.put("body", body);
            source.put("body_excerpt", getBodyExcerpt(message));
            source.put("object_type", "Email");
            source.put("is_encrypted", false);
            source.put("indexed_datetime", java.time.Instant.now().toString());
            source.put("subject", subject);
            boolean hasAttachment = hasAttachments(message);
            source.put("has_attachment", hasAttachment);
            source.put("delivery_date", sentDate);
            source.put("from", from);
            source.put("to", to);
            source.put("day", day);
            source.put("content_type", contentTypes);
            source.put("size", fileSize);
            source.put("tags", new ArrayList<>());
            source.put("folders", new ArrayList<>());
            source.put("object_status", "Processed");
            source.put("sha1", sha1);
            source.put("md5", md5);
            source.put("message_id", messageId);
            source.put("envelope_mail_addr", from);
            source.put("envelope_mail_domain", extractDomainList(from));
            source.put("envelope_mail_local", "0");
            source.put("envelope_rcpt_addr", to.isEmpty() ? new ArrayList<>() : new ArrayList<>(java.util.Collections.singletonList(to)));
            source.put("envelope_rcpt_domain", extractDomainList(to));
            source.put("envelope_rcpt_local", "0");
            source.put("envelope_rcpt_message_inbound", 1);
            source.put("envelope_rcpt_message_outbound", 0);
            source.put("envelope_rcpt_message_internal", 0);
            source.put("envelope_rcpt_message_mode", "inbound");
            List<String> ldapGroups = new ArrayList<>();
            if (!from.isEmpty()) ldapGroups.add(from);
            if (!to.isEmpty()) ldapGroups.add(to);
            source.put("expanded_ldap_groups", ldapGroups);
            source.put("_timestamp", System.currentTimeMillis());
            source.put("uri", sha1 + " " + docId + " " + fileSize);

            return source;
        } catch (Exception e) {
            logger.info("❌ Failed to process email file: {}", emailFile.getAbsolutePath(), e);
            throw new RuntimeException("❌ Failed to process email file: " + emailFile.getAbsolutePath(), e);
        }
    }

    // === Utility Methods ===
        private static List<String> extractDomainList(String address) {
            List<String> domains = new ArrayList<>();
            if (address != null && !address.isEmpty()) {
                String email = address;
                int lt = address.indexOf('<');
                int gt = address.indexOf('>');
                if (lt >= 0 && gt > lt) {
                    email = address.substring(lt + 1, gt);
                }
                int at = email.indexOf('@');
                if (at >= 0 && at < email.length() - 1) {
                    domains.add(email.substring(at + 1));
                }
            }
            return domains;
        }

    private static String formatAddress(Object address) {
        try {
            if (address instanceof jakarta.mail.internet.InternetAddress) {
                jakarta.mail.internet.InternetAddress ia = (jakarta.mail.internet.InternetAddress) address;
                String personal = ia.getPersonal();
                String email = ia.getAddress();
                if (personal != null && !personal.isEmpty()) {
                    return "\"" + personal + "\" <" + email + ">";
                } else {
                    return email;
                }
            }
            return address.toString();
        } catch (Exception e) {
            return address != null ? address.toString() : "";
        }
    }

    private static String getBody(jakarta.mail.Part part) throws Exception {
        if (part.isMimeType("text/plain")) {
            return (String) part.getContent();
        } else if (part.isMimeType("multipart/*")) {
            jakarta.mail.Multipart mp = (jakarta.mail.Multipart) part.getContent();
            for (int i = 0; i < mp.getCount(); i++) {
                String text = getBody(mp.getBodyPart(i));
                if (text != null && !text.isEmpty()) return text;
            }
        }
        return "";
    }

    private static boolean hasAttachments(jakarta.mail.Part part) throws Exception {
        if (part.isMimeType("multipart/*")) {
            jakarta.mail.Multipart mp = (jakarta.mail.Multipart) part.getContent();
            for (int i = 0; i < mp.getCount(); i++) {
                jakarta.mail.BodyPart bp = mp.getBodyPart(i);
                if (jakarta.mail.Part.ATTACHMENT.equalsIgnoreCase(bp.getDisposition())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String getBodyExcerpt(jakarta.mail.Part part) throws Exception {
        if (part.isMimeType("text/plain")) {
            return ((String) part.getContent()).substring(0, Math.min(100, ((String) part.getContent()).length()));
        } else if (part.isMimeType("multipart/*")) {
            jakarta.mail.Multipart mp = (jakarta.mail.Multipart) part.getContent();
            for (int i = 0; i < mp.getCount(); i++) {
                String text = getBodyExcerpt(mp.getBodyPart(i));
                if (text != null) return text;
            }
        }
        return "";
    }

    private static String computeHash(byte[] data, String algorithm) throws Exception {
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        byte[] hashBytes = digest.digest(data);
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

}


