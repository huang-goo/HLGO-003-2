/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AlgorithmState implements Serializable {
    private static final long serialVersionUID = 2L;

    private String algorithmType;
    private int stateVersion;
    private long lastUpdateTime;
    private long processedCount;
    private Map<String, Object> stateData;

    public AlgorithmState() {
        this.stateVersion = 1;
        this.lastUpdateTime = System.currentTimeMillis();
        this.processedCount = 0;
        this.stateData = new HashMap<>();
    }

    public AlgorithmState(String algorithmType) {
        this();
        this.algorithmType = algorithmType;
    }

    public String getAlgorithmType() {
        return algorithmType;
    }

    public void setAlgorithmType(String algorithmType) {
        this.algorithmType = algorithmType;
    }

    public int getStateVersion() {
        return stateVersion;
    }

    public void setStateVersion(int stateVersion) {
        this.stateVersion = stateVersion;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public long getProcessedCount() {
        return processedCount;
    }

    public void setProcessedCount(long processedCount) {
        this.processedCount = processedCount;
    }

    public void incrementProcessedCount() {
        this.processedCount++;
    }

    public Map<String, Object> getStateData() {
        return stateData;
    }

    public void setStateData(Map<String, Object> stateData) {
        this.stateData = stateData;
    }

    public Object get(String key) {
        return stateData.get(key);
    }

    public void put(String key, Object value) {
        stateData.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> type) {
        Object value = stateData.get(key);
        if (value == null) {
            return null;
        }
        return (T) value;
    }
}
