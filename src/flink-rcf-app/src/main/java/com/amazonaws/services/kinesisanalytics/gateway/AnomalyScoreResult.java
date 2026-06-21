/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AnomalyScoreResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private double score;
    private AnomalyLevel level;
    private String algorithmType;
    private String algorithmVersion;
    private long timestamp;
    private Map<String, String> explanation;
    private Map<String, Double> features;

    public AnomalyScoreResult() {
        this.explanation = new HashMap<>();
        this.features = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
    }

    public AnomalyScoreResult(double score, String algorithmType) {
        this();
        this.score = score;
        this.algorithmType = algorithmType;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
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

    public String getAlgorithmVersion() {
        return algorithmVersion;
    }

    public void setAlgorithmVersion(String algorithmVersion) {
        this.algorithmVersion = algorithmVersion;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getExplanation() {
        return explanation;
    }

    public void setExplanation(Map<String, String> explanation) {
        this.explanation = explanation;
    }

    public void addExplanation(String key, String value) {
        this.explanation.put(key, value);
    }

    public Map<String, Double> getFeatures() {
        return features;
    }

    public void setFeatures(Map<String, Double> features) {
        this.features = features;
    }

    public void addFeature(String key, double value) {
        this.features.put(key, value);
    }

    @Override
    public String toString() {
        return "AnomalyScoreResult{" +
                "score=" + score +
                ", level=" + level +
                ", algorithmType='" + algorithmType + '\'' +
                ", algorithmVersion='" + algorithmVersion + '\'' +
                ", timestamp=" + timestamp +
                ", explanation=" + explanation +
                '}';
    }
}
