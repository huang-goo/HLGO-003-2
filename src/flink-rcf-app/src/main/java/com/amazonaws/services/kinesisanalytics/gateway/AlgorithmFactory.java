/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import com.amazonaws.services.kinesisanalytics.gateway.algorithms.DtwAlgorithm;
import com.amazonaws.services.kinesisanalytics.gateway.algorithms.EwmaAlgorithm;
import com.amazonaws.services.kinesisanalytics.gateway.algorithms.RcfAlgorithm;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class AlgorithmFactory {

    private static final Map<String, Class<? extends AnomalyAlgorithm>> REGISTRY = new HashMap<>();

    static {
        REGISTRY.put(RcfAlgorithm.ALGORITHM_TYPE, RcfAlgorithm.class);
        REGISTRY.put(DtwAlgorithm.ALGORITHM_TYPE, DtwAlgorithm.class);
        REGISTRY.put(EwmaAlgorithm.ALGORITHM_TYPE, EwmaAlgorithm.class);
    }

    public static AnomalyAlgorithm createAlgorithm(String algorithmType) {
        Class<? extends AnomalyAlgorithm> clazz = REGISTRY.get(algorithmType);
        if (clazz == null) {
            throw new IllegalArgumentException("Unknown algorithm type: " + algorithmType);
        }
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create algorithm: " + algorithmType, e);
        }
    }

    public static AnomalyAlgorithm createAndInitAlgorithm(String algorithmType,
                                                           ParameterTool parameter, int dimensions) {
        AnomalyAlgorithm algorithm = createAlgorithm(algorithmType);
        algorithm.init(parameter, dimensions);
        return algorithm;
    }

    public static boolean isSupported(String algorithmType) {
        return REGISTRY.containsKey(algorithmType);
    }

    public static void registerAlgorithm(String type, Class<? extends AnomalyAlgorithm> clazz) {
        REGISTRY.put(type, clazz);
    }

    public static Map<String, Class<? extends AnomalyAlgorithm>> getRegisteredAlgorithms() {
        return new HashMap<>(REGISTRY);
    }

    public static void loadFromServiceLoader() {
        ServiceLoader<AnomalyAlgorithm> loader = ServiceLoader.load(AnomalyAlgorithm.class);
        for (AnomalyAlgorithm algorithm : loader) {
            REGISTRY.put(algorithm.getAlgorithmType(), algorithm.getClass());
        }
    }
}
