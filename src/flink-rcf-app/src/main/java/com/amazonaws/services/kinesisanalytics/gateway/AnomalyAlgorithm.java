/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import org.apache.flink.api.java.utils.ParameterTool;

public interface AnomalyAlgorithm {

    String getAlgorithmType();

    String getAlgorithmVersion();

    void init(ParameterTool parameter, int dimensions);

    AnomalyScoreResult detect(double[] values);

    AlgorithmState saveState();

    void restoreState(AlgorithmState state);

    default boolean canMigrateFrom(AlgorithmState state) {
        return state != null && getAlgorithmType().equals(state.getAlgorithmType());
    }

    default void migrateFrom(AlgorithmState oldState) {
        if (canMigrateFrom(oldState)) {
            restoreState(oldState);
        }
    }

    void close();
}
