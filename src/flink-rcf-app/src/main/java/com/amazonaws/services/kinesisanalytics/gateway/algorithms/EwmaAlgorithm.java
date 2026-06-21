/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway.algorithms;

import com.amazonaws.services.kinesisanalytics.gateway.AlgorithmState;
import com.amazonaws.services.kinesisanalytics.gateway.AnomalyAlgorithm;
import com.amazonaws.services.kinesisanalytics.gateway.AnomalyLevel;
import com.amazonaws.services.kinesisanalytics.gateway.AnomalyScoreResult;

import org.apache.flink.api.java.utils.ParameterTool;

public class EwmaAlgorithm implements AnomalyAlgorithm {

    public static final String ALGORITHM_TYPE = "EWMA";
    public static final String ALGORITHM_VERSION = "1.0.0";

    private ParameterTool parameter;
    private int dimensions;
    private double alpha;
    private double[] ewma;
    private double[] variance;
    private double[] stdDev;
    private long processedCount;
    private boolean initialized;

    private double warningSigma;
    private double errorSigma;
    private double criticalSigma;

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
        this.initialized = false;

        this.alpha = Double.parseDouble(parameter.get("EwmaAlpha", "0.2"));
        this.warningSigma = Double.parseDouble(parameter.get("EwmaWarningSigma", "2.0"));
        this.errorSigma = Double.parseDouble(parameter.get("EwmaErrorSigma", "3.0"));
        this.criticalSigma = Double.parseDouble(parameter.get("EwmaCriticalSigma", "4.0"));

        this.ewma = new double[dimensions];
        this.variance = new double[dimensions];
        this.stdDev = new double[dimensions];
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

        double maxZScore = 0.0;
        int maxIndex = 0;

        if (!initialized) {
            for (int i = 0; i < dimensions; i++) {
                ewma[i] = values[i];
                variance[i] = 0.0;
                stdDev[i] = 1.0;
            }
            initialized = true;
        } else {
            for (int i = 0; i < dimensions; i++) {
                double diff = values[i] - ewma[i];
                ewma[i] = ewma[i] + alpha * diff;
                variance[i] = (1 - alpha) * (variance[i] + alpha * diff * diff);
                stdDev[i] = Math.sqrt(Math.max(variance[i], 0.0));

                double zScore = stdDev[i] > 0 ? Math.abs(diff) / stdDev[i] : 0.0;
                if (zScore > maxZScore) {
                    maxZScore = zScore;
                    maxIndex = i;
                }
            }
        }

        double warningThreshold = warningSigma;
        double errorThreshold = errorSigma;
        double criticalThreshold = criticalSigma;

        result.setScore(maxZScore);
        result.setLevel(AnomalyLevel.fromScore(maxZScore, warningThreshold, errorThreshold, criticalThreshold));

        result.addExplanation("alpha", String.valueOf(alpha));
        result.addExplanation("dimensions", String.valueOf(dimensions));
        result.addExplanation("max_deviation_dim", String.valueOf(maxIndex));
        result.addExplanation("warning_sigma", String.valueOf(warningSigma));
        result.addExplanation("error_sigma", String.valueOf(errorSigma));
        result.addExplanation("critical_sigma", String.valueOf(criticalSigma));
        result.addExplanation("initialized", String.valueOf(initialized));

        for (int i = 0; i < dimensions; i++) {
            result.addFeature("dim_" + i + "_value", values[i]);
            result.addFeature("dim_" + i + "_ewma", ewma[i]);
            result.addFeature("dim_" + i + "_stddev", stdDev[i]);
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
        state.put("alpha", alpha);
        state.put("ewma", ewma);
        state.put("variance", variance);
        state.put("stdDev", stdDev);
        state.put("initialized", initialized);
        state.put("warningSigma", warningSigma);
        state.put("errorSigma", errorSigma);
        state.put("criticalSigma", criticalSigma);

        return state;
    }

    @Override
    public void restoreState(AlgorithmState state) {
        if (state == null || !ALGORITHM_TYPE.equals(state.getAlgorithmType())) {
            return;
        }

        this.processedCount = state.getProcessedCount();
        this.dimensions = state.get("dimensions", Integer.class);
        this.alpha = state.get("alpha", Double.class);
        this.ewma = state.get("ewma", double[].class);
        this.variance = state.get("variance", double[].class);
        this.stdDev = state.get("stdDev", double[].class);

        Boolean init = state.get("initialized", Boolean.class);
        if (init != null) this.initialized = init;

        Double warnSigma = state.get("warningSigma", Double.class);
        Double errSigma = state.get("errorSigma", Double.class);
        Double critSigma = state.get("criticalSigma", Double.class);

        if (warnSigma != null) this.warningSigma = warnSigma;
        if (errSigma != null) this.errorSigma = errSigma;
        if (critSigma != null) this.criticalSigma = critSigma;
    }

    @Override
    public void close() {
    }
}
