/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway.algorithms;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.util.ShingleBuilder;
import com.amazonaws.services.kinesisanalytics.gateway.AlgorithmState;
import com.amazonaws.services.kinesisanalytics.gateway.AnomalyAlgorithm;
import com.amazonaws.services.kinesisanalytics.gateway.AnomalyLevel;
import com.amazonaws.services.kinesisanalytics.gateway.AnomalyScoreResult;

import org.apache.flink.api.java.utils.ParameterTool;

public class RcfAlgorithm implements AnomalyAlgorithm {

    public static final String ALGORITHM_TYPE = "RCF";
    public static final String ALGORITHM_VERSION = "1.0.0";

    private ParameterTool parameter;
    private RandomCutForest forest;
    private ShingleBuilder shingleBuilder;
    private double[] pointBuffer;
    private double[] shingleBuffer;
    private int dimensions;
    private long processedCount;

    private double warningThreshold;
    private double errorThreshold;
    private double criticalThreshold;

    @Override
    public String getAlgorithmType() {
        return ALGORITHM_TYPE;
    }

    @Override
    public String getAlgorithmVersion() {
        return ALGORITHM_VERSION;
    }

    @Override
    public void init(ParameterTool parameter, int dimensions) {
        this.parameter = parameter;
        this.dimensions = dimensions;
        this.processedCount = 0;

        this.warningThreshold = Double.parseDouble(parameter.get("RcfWarningThreshold", "1.0"));
        this.errorThreshold = Double.parseDouble(parameter.get("RcfErrorThreshold", "2.0"));
        this.criticalThreshold = Double.parseDouble(parameter.get("RcfCriticalThreshold", "3.0"));

        pointBuffer = new double[dimensions];
        shingleBuilder = new ShingleBuilder(dimensions,
                Integer.parseInt(parameter.get("RcfShingleSize", "1")),
                Boolean.parseBoolean(parameter.get("RcfShingleCyclic", "false")));
        shingleBuffer = new double[shingleBuilder.getShingledPointSize()];

        forest = RandomCutForest.builder()
                .numberOfTrees(Integer.parseInt(parameter.get("RcfNumberOfTrees", "50")))
                .sampleSize(Integer.parseInt(parameter.get("RcfSampleSize", "8192")))
                .dimensions(shingleBuilder.getShingledPointSize())
                .lambda(Double.parseDouble(parameter.get("RcfLambda", "0.00001220703125")))
                .randomSeed(Integer.parseInt(parameter.get("RcfRandomSeed", "42")))
                .build();
    }

    @Override
    public AnomalyScoreResult detect(double[] values) {
        AnomalyScoreResult result = new AnomalyScoreResult();
        result.setAlgorithmType(ALGORITHM_TYPE);
        result.setAlgorithmVersion(ALGORITHM_VERSION);

        if (values.length != dimensions) {
            throw new IllegalArgumentException(
                    String.format("Wrong number of values. Expected %d but found %d.",
                            dimensions, values.length));
        }

        System.arraycopy(values, 0, pointBuffer, 0, values.length);
        shingleBuilder.addPoint(pointBuffer);
        processedCount++;

        double score;
        if (shingleBuilder.isFull()) {
            shingleBuilder.getShingle(shingleBuffer);
            score = forest.getAnomalyScore(shingleBuffer);
            forest.update(shingleBuffer);
        } else {
            score = 0.0;
        }

        result.setScore(score);
        result.setLevel(AnomalyLevel.fromScore(score, warningThreshold, errorThreshold, criticalThreshold));

        result.addExplanation("shingle_size", String.valueOf(shingleBuilder.getShingledPointSize()));
        result.addExplanation("shingle_full", String.valueOf(shingleBuilder.isFull()));
        result.addExplanation("trees", String.valueOf(forest.getNumberOfTrees()));
        result.addExplanation("sample_size", String.valueOf(forest.getSampleSize()));
        result.addExplanation("warning_threshold", String.valueOf(warningThreshold));
        result.addExplanation("error_threshold", String.valueOf(errorThreshold));
        result.addExplanation("critical_threshold", String.valueOf(criticalThreshold));

        for (int i = 0; i < values.length; i++) {
            result.addFeature("dim_" + i, values[i]);
        }

        return result;
    }

    @Override
    public AlgorithmState saveState() {
        AlgorithmState state = new AlgorithmState(ALGORITHM_TYPE);
        state.setStateVersion(1);
        state.setProcessedCount(processedCount);
        state.setLastUpdateTime(System.currentTimeMillis());

        state.put("dimensions", dimensions);
        state.put("pointBuffer", pointBuffer);
        state.put("shingleBuffer", shingleBuffer);
        state.put("forest", forest);
        state.put("shingleBuilder", shingleBuilder);
        state.put("warningThreshold", warningThreshold);
        state.put("errorThreshold", errorThreshold);
        state.put("criticalThreshold", criticalThreshold);

        return state;
    }

    @Override
    public void restoreState(AlgorithmState state) {
        if (state == null || !ALGORITHM_TYPE.equals(state.getAlgorithmType())) {
            return;
        }

        this.processedCount = state.getProcessedCount();
        this.dimensions = state.get("dimensions", Integer.class);
        this.pointBuffer = state.get("pointBuffer", double[].class);
        this.shingleBuffer = state.get("shingleBuffer", double[].class);
        this.forest = state.get("forest", RandomCutForest.class);
        this.shingleBuilder = state.get("shingleBuilder", ShingleBuilder.class);

        Double warnThresh = state.get("warningThreshold", Double.class);
        Double errThresh = state.get("errorThreshold", Double.class);
        Double critThresh = state.get("criticalThreshold", Double.class);

        if (warnThresh != null) this.warningThreshold = warnThresh;
        if (errThresh != null) this.errorThreshold = errThresh;
        if (critThresh != null) this.criticalThreshold = critThresh;
    }

    @Override
    public void close() {
    }
}
