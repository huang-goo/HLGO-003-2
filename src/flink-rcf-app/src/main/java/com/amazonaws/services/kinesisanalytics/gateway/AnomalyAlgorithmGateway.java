/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AnomalyAlgorithmGateway implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(AnomalyAlgorithmGateway.class);

    private ParameterTool parameter;
    private String defaultAlgorithmType;
    private final Map<String, AnomalyAlgorithm> algorithmCache;
    private final Map<String, AlgorithmState> stateStore;
    private final Map<String, String> tagAlgorithmMapping;
    private int dimensions;

    public AnomalyAlgorithmGateway(ParameterTool parameter) {
        this.parameter = parameter;
        this.defaultAlgorithmType = parameter.get("DefaultAlgorithmType", "RCF");
        this.algorithmCache = new HashMap<>();
        this.stateStore = new HashMap<>();
        this.tagAlgorithmMapping = new HashMap<>();
        this.dimensions = -1;
    }

    public void init(int dimensions) {
        this.dimensions = dimensions;
        loadTagMappings();
    }

    private void loadTagMappings() {
        String mappingStr = parameter.get("TagAlgorithmMapping", "");
        if (mappingStr != null && !mappingStr.isEmpty()) {
            String[] mappings = mappingStr.split(",");
            for (String mapping : mappings) {
                String[] parts = mapping.split("=");
                if (parts.length == 2) {
                    tagAlgorithmMapping.put(parts[0].trim(), parts[1].trim());
                }
            }
        }
    }

    public void setTagAlgorithmMapping(String tag, String algorithmType) {
        tagAlgorithmMapping.put(tag, algorithmType);
    }

    public String getAlgorithmTypeForTag(String tag) {
        return tagAlgorithmMapping.getOrDefault(tag, defaultAlgorithmType);
    }

    public AnomalyScoreResult detect(String trafficTag, double[] values) {
        String algorithmType = getAlgorithmTypeForTag(trafficTag);
        AnomalyAlgorithm algorithm = getOrCreateAlgorithm(trafficTag, algorithmType);

        AnomalyScoreResult result = algorithm.detect(values);
        result.addExplanation("traffic_tag", trafficTag);
        result.addExplanation("selected_algorithm", algorithmType);

        return result;
    }

    public AnomalyEvent detectAndCreateEvent(String trafficTag, double[] values) {
        AnomalyScoreResult scoreResult = detect(trafficTag, values);

        if (scoreResult.getLevel() == null || scoreResult.getLevel() == AnomalyLevel.INFO) {
            return null;
        }

        AnomalyEvent event = new AnomalyEvent(
                scoreResult.getLevel(),
                scoreResult.getAlgorithmType(),
                trafficTag,
                scoreResult.getScore(),
                "Anomaly detected with score: " + scoreResult.getScore()
        );

        for (Map.Entry<String, String> entry : scoreResult.getExplanation().entrySet()) {
            event.addContext(entry.getKey(), entry.getValue());
        }

        return event;
    }

    private AnomalyAlgorithm getOrCreateAlgorithm(String tag, String algorithmType) {
        AnomalyAlgorithm algorithm = algorithmCache.get(tag);

        if (algorithm == null || !algorithm.getAlgorithmType().equals(algorithmType)) {
            if (algorithm != null) {
                AlgorithmState state = algorithm.saveState();
                stateStore.put(tag, state);
                algorithm.close();
                logger.info("Switched algorithm for tag '{}' from {} to {}",
                        tag, algorithm.getAlgorithmType(), algorithmType);
            }

            algorithm = AlgorithmFactory.createAndInitAlgorithm(algorithmType, parameter, dimensions);

            AlgorithmState previousState = stateStore.get(tag);
            if (previousState != null) {
                if (algorithm.canMigrateFrom(previousState)) {
                    algorithm.migrateFrom(previousState);
                    logger.info("Migrated state for tag '{}' to algorithm {}", tag, algorithmType);
                } else {
                    logger.info("Cannot migrate state for tag '{}' from {} to {}, starting fresh",
                            tag, previousState.getAlgorithmType(), algorithmType);
                }
            }

            algorithmCache.put(tag, algorithm);
        }

        return algorithm;
    }

    public void switchAlgorithm(String tag, String newAlgorithmType) {
        if (!AlgorithmFactory.isSupported(newAlgorithmType)) {
            throw new IllegalArgumentException("Unsupported algorithm type: " + newAlgorithmType);
        }

        tagAlgorithmMapping.put(tag, newAlgorithmType);

        AnomalyAlgorithm currentAlgorithm = algorithmCache.get(tag);
        if (currentAlgorithm != null && !currentAlgorithm.getAlgorithmType().equals(newAlgorithmType)) {
            AlgorithmState state = currentAlgorithm.saveState();
            stateStore.put(tag, state);
            currentAlgorithm.close();
            algorithmCache.remove(tag);
            logger.info("Scheduled algorithm switch for tag '{}' to {}, state saved", tag, newAlgorithmType);
        }
    }

    public Map<String, AlgorithmState> saveAllStates() {
        Map<String, AlgorithmState> states = new HashMap<>();

        for (Map.Entry<String, AnomalyAlgorithm> entry : algorithmCache.entrySet()) {
            states.put(entry.getKey(), entry.getValue().saveState());
        }

        states.putAll(stateStore);

        return states;
    }

    public void restoreAllStates(Map<String, AlgorithmState> states) {
        if (states == null) {
            return;
        }

        for (Map.Entry<String, AlgorithmState> entry : states.entrySet()) {
            stateStore.put(entry.getKey(), entry.getValue());
        }

        for (String tag : tagAlgorithmMapping.keySet()) {
            String algorithmType = getAlgorithmTypeForTag(tag);
            AlgorithmState state = stateStore.get(tag);

            if (state != null && algorithmType.equals(state.getAlgorithmType())) {
                try {
                    AnomalyAlgorithm algorithm = AlgorithmFactory.createAndInitAlgorithm(
                            algorithmType, parameter, dimensions);
                    algorithm.restoreState(state);
                    algorithmCache.put(tag, algorithm);
                    logger.info("Restored algorithm state for tag '{}' ({})", tag, algorithmType);
                } catch (Exception e) {
                    logger.error("Failed to restore state for tag '{}'", tag, e);
                }
            }
        }
    }

    public AnomalyAlgorithm getAlgorithm(String tag) {
        return algorithmCache.get(tag);
    }

    public String getDefaultAlgorithmType() {
        return defaultAlgorithmType;
    }

    public void setDefaultAlgorithmType(String defaultAlgorithmType) {
        this.defaultAlgorithmType = defaultAlgorithmType;
    }

    public void close() {
        for (AnomalyAlgorithm algorithm : algorithmCache.values()) {
            try {
                algorithm.close();
            } catch (Exception e) {
                logger.error("Error closing algorithm", e);
            }
        }
        algorithmCache.clear();
        stateStore.clear();
    }

    public Map<String, String> getTagAlgorithmMapping() {
        return new HashMap<>(tagAlgorithmMapping);
    }
}
