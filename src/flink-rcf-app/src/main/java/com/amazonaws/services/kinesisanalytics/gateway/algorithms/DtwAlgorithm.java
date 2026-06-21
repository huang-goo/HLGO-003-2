/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway.algorithms;

import com.amazonaws.services.kinesisanalytics.gateway.AlgorithmState;
import com.amazonaws.services.kinesisanalytics.gateway.AnomalyAlgorithm;
import com.amazonaws.services.kinesisanalytics.gateway.AnomalyLevel;
import com.amazonaws.services.kinesisanalytics.gateway.AnomalyScoreResult;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import smile.math.distance.DynamicTimeWarping;

public class DtwAlgorithm implements AnomalyAlgorithm {

    public static final String ALGORITHM_TYPE = "DTW";
    public static final String ALGORITHM_VERSION = "1.0.0";

    private ParameterTool parameter;
    private int dimensions;
    private int windowSize;
    private int warpingWindow;
    private Deque<double[]> dataWindow;
    private double[] referencePattern;
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

        this.windowSize = Integer.parseInt(parameter.get("DtwWindowSize", "100"));
        this.warpingWindow = Integer.parseInt(parameter.get("DtwWarpingWindow", "50"));
        this.warningThreshold = Double.parseDouble(parameter.get("DtwWarningThreshold", "10.0"));
        this.errorThreshold = Double.parseDouble(parameter.get("DtwErrorThreshold", "20.0"));
        this.criticalThreshold = Double.parseDouble(parameter.get("DtwCriticalThreshold", "30.0"));

        this.dataWindow = new ArrayDeque<>(windowSize);
        this.referencePattern = null;
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

        processedCount++;

        double[] point = values.clone();
        dataWindow.addLast(point);

        if (dataWindow.size() > windowSize) {
            dataWindow.pollFirst();
        }

        double score;
        boolean hasReference = referencePattern != null && referencePattern.length > 0;

        if (hasReference && dataWindow.size() >= referencePattern.length) {
            double[] currentSeries = flattenWindow();
            score = DynamicTimeWarping.d(referencePattern, currentSeries, warpingWindow);
        } else {
            score = 0.0;
        }

        if (referencePattern == null && dataWindow.size() >= windowSize / 2) {
            referencePattern = flattenWindow();
        }

        result.setScore(score);
        result.setLevel(AnomalyLevel.fromScore(score, warningThreshold, errorThreshold, criticalThreshold));

        result.addExplanation("window_size", String.valueOf(windowSize));
        result.addExplanation("current_window_size", String.valueOf(dataWindow.size()));
        result.addExplanation("warping_window", String.valueOf(warpingWindow));
        result.addExplanation("has_reference_pattern", String.valueOf(hasReference));
        result.addExplanation("reference_pattern_length",
                referencePattern != null ? String.valueOf(referencePattern.length) : "0");
        result.addExplanation("warning_threshold", String.valueOf(warningThreshold));
        result.addExplanation("error_threshold", String.valueOf(errorThreshold));
        result.addExplanation("critical_threshold", String.valueOf(criticalThreshold));

        for (int i = 0; i < values.length; i++) {
            result.addFeature("dim_" + i, values[i]);
        }

        return result;
    }

    private double[] flattenWindow() {
        List<Double> flatList = new ArrayList<>();
        for (double[] point : dataWindow) {
            for (double v : point) {
                flatList.add(v);
            }
        }
        double[] result = new double[flatList.size()];
        for (int i = 0; i < flatList.size(); i++) {
            result[i] = flatList.get(i);
        }
        return result;
    }

    public void setReferencePattern(double[] pattern) {
        this.referencePattern = pattern.clone();
    }

    @Override
    public AlgorithmState saveState() {
        AlgorithmState state = new AlgorithmState(ALGORITHM_TYPE);
        state.setStateVersion(1);
        state.setProcessedCount(processedCount);
        state.setLastUpdateTime(System.currentTimeMillis());

        state.put("dimensions", dimensions);
        state.put("windowSize", windowSize);
        state.put("warpingWindow", warpingWindow);
        state.put("referencePattern", referencePattern);
        state.put("warningThreshold", warningThreshold);
        state.put("errorThreshold", errorThreshold);
        state.put("criticalThreshold", criticalThreshold);

        List<double[]> windowList = new ArrayList<>(dataWindow);
        state.put("dataWindow", windowList);

        return state;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void restoreState(AlgorithmState state) {
        if (state == null || !ALGORITHM_TYPE.equals(state.getAlgorithmType())) {
            return;
        }

        this.processedCount = state.getProcessedCount();
        this.dimensions = state.get("dimensions", Integer.class);
        this.windowSize = state.get("windowSize", Integer.class);
        this.warpingWindow = state.get("warpingWindow", Integer.class);
        this.referencePattern = state.get("referencePattern", double[].class);

        Double warnThresh = state.get("warningThreshold", Double.class);
        Double errThresh = state.get("errorThreshold", Double.class);
        Double critThresh = state.get("criticalThreshold", Double.class);

        if (warnThresh != null) this.warningThreshold = warnThresh;
        if (errThresh != null) this.errorThreshold = errThresh;
        if (critThresh != null) this.criticalThreshold = critThresh;

        List<double[]> windowList = state.get("dataWindow", List.class);
        if (windowList != null) {
            this.dataWindow = new ArrayDeque<>(windowList);
        }
    }

    @Override
    public void close() {
        if (dataWindow != null) {
            dataWindow.clear();
        }
    }
}
