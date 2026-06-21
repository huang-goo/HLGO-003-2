/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class AnomalyDetectOperator extends KeyedProcessFunction<String, AnomalyInput, AnomalyScoreResult> {

    private static final Logger logger = LoggerFactory.getLogger(AnomalyDetectOperator.class);

    public static final OutputTag<AnomalyEvent> ANOMALY_EVENT_OUTPUT_TAG =
            new OutputTag<AnomalyEvent>("anomaly-events") {};

    private final ParameterTool parameter;
    private final int dimensions;

    private transient ValueState<AlgorithmState> algorithmState;
    private transient AnomalyAlgorithm currentAlgorithm;
    private transient String currentAlgorithmType;

    public AnomalyDetectOperator(ParameterTool parameter, int dimensions) {
        this.parameter = parameter;
        this.dimensions = dimensions;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<AlgorithmState> stateDescriptor = new ValueStateDescriptor<>(
                "algorithm-state",
                TypeInformation.of(new TypeHint<AlgorithmState>() {})
        );
        algorithmState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(AnomalyInput input, Context ctx,
                               Collector<AnomalyScoreResult> out) throws Exception {
        String trafficTag = ctx.getCurrentKey();

        String targetAlgorithmType = getAlgorithmTypeForTag(trafficTag);

        ensureAlgorithmInitialized(targetAlgorithmType, trafficTag);

        AnomalyScoreResult result = currentAlgorithm.detect(input.getValues());
        result.addExplanation("traffic_tag", trafficTag);
        result.addExplanation("operator_state_version", "v2");

        out.collect(result);

        if (result.getLevel() != null && result.getLevel() != AnomalyLevel.INFO) {
            AnomalyEvent event = createAnomalyEvent(result, trafficTag);
            ctx.output(ANOMALY_EVENT_OUTPUT_TAG, event);
        }

        algorithmState.update(currentAlgorithm.saveState());
    }

    private void ensureAlgorithmInitialized(String algorithmType, String trafficTag) throws Exception {
        if (currentAlgorithm == null || !algorithmType.equals(currentAlgorithmType)) {
            AlgorithmState savedState = algorithmState.value();

            if (currentAlgorithm != null) {
                currentAlgorithm.close();
                logger.info("Switching algorithm for tag '{}' from {} to {}",
                        trafficTag, currentAlgorithmType, algorithmType);
            }

            currentAlgorithm = AlgorithmFactory.createAndInitAlgorithm(algorithmType, parameter, dimensions);
            currentAlgorithmType = algorithmType;

            if (savedState != null) {
                if (currentAlgorithm.canMigrateFrom(savedState)) {
                    currentAlgorithm.migrateFrom(savedState);
                    logger.info("Migrated state for tag '{}' to {} (state version: {})",
                            trafficTag, algorithmType, savedState.getStateVersion());
                } else {
                    logger.info("Cannot migrate state for tag '{}' from {} to {}, starting fresh",
                            trafficTag,
                            savedState.getAlgorithmType() != null ? savedState.getAlgorithmType() : "unknown",
                            algorithmType);
                }
            } else {
                logger.info("No saved state for tag '{}', initializing new {} algorithm",
                        trafficTag, algorithmType);
            }
        }
    }

    private String getAlgorithmTypeForTag(String trafficTag) {
        String mappingKey = "TagAlgorithm." + trafficTag;
        String mappedType = parameter.get(mappingKey, null);
        if (mappedType != null && AlgorithmFactory.isSupported(mappedType)) {
            return mappedType;
        }
        return parameter.get("DefaultAlgorithmType", "RCF");
    }

    private AnomalyEvent createAnomalyEvent(AnomalyScoreResult result, String trafficTag) {
        AnomalyEvent event = new AnomalyEvent(
                result.getLevel(),
                result.getAlgorithmType(),
                trafficTag,
                result.getScore(),
                "Anomaly detected with score " + result.getScore()
        );

        event.setTimestamp(result.getTimestamp());
        event.addContext("algorithm_version", result.getAlgorithmVersion());

        for (java.util.Map.Entry<String, String> entry : result.getExplanation().entrySet()) {
            event.addContext(entry.getKey(), entry.getValue());
        }

        return event;
    }

    @Override
    public void close() throws Exception {
        if (currentAlgorithm != null) {
            currentAlgorithm.close();
        }
        super.close();
    }
}
