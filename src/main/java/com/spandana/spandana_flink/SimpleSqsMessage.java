package com.spandana.spandana_flink;

import java.io.Serializable;

public class SimpleSqsMessage implements Serializable {
    private String messageId;
    private String body;

    public SimpleSqsMessage() {}

    public SimpleSqsMessage(String messageId, String body) {
        this.messageId = messageId;
        this.body = body;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "SimpleSqsMessage{" +
                "messageId='" + messageId + '\'' +
                ", body='" + body + '\'' +
                '}';
    }
}
