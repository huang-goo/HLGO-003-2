/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AnomalyEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventId;
    private AnomalyLevel level;
    private String algorithmType;
    private String trafficTag;
    private double score;
    private long timestamp;
    private String message;
    private Map<String, String> context;

    public AnomalyEvent() {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = System.currentTimeMillis();
        this.context = new HashMap<>();
    }

    public AnomalyEvent(AnomalyLevel level, String algorithmType, String trafficTag,
                        double score, String message) {
        this();
        this.level = level;
        this.algorithmType = algorithmType;
        this.trafficTag = trafficTag;
        this.score = score;
        this.message = message;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public AnomalyLevel getLevel() {
        return level;
    }

    public void setLevel(AnomalyLevel level) {
        this.level = level;
    }

    public String getAlgorithmType() {
        return algorithmType;
    }

    public void setAlgorithmType(String algorithmType) {
        this.algorithmType = algorithmType;
    }

    public String getTrafficTag() {
        return trafficTag;
    }

    public void setTrafficTag(String trafficTag) {
        this.trafficTag = trafficTag;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, String> getContext() {
        return context;
    }

    public void setContext(Map<String, String> context) {
        this.context = context;
    }

    public void addContext(String key, String value) {
        this.context.put(key, value);
    }

    @Override
    public String toString() {
        return "AnomalyEvent{" +
                "eventId='" + eventId + '\'' +
                ", level=" + level +
                ", algorithmType='" + algorithmType + '\'' +
                ", trafficTag='" + trafficTag + '\'' +
                ", score=" + score +
                ", timestamp=" + timestamp +
                ", message='" + message + '\'' +
                '}';
    }
}
