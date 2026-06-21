/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

public enum AnomalyLevel {
    INFO(0, "INFO"),
    WARNING(1, "WARNING"),
    ERROR(2, "ERROR"),
    CRITICAL(3, "CRITICAL");

    private final int severity;
    private final String name;

    AnomalyLevel(int severity, String name) {
        this.severity = severity;
        this.name = name;
    }

    public int getSeverity() {
        return severity;
    }

    public String getName() {
        return name;
    }

    public static AnomalyLevel fromScore(double score, double warningThreshold,
                                         double errorThreshold, double criticalThreshold) {
        if (score >= criticalThreshold) {
            return CRITICAL;
        } else if (score >= errorThreshold) {
            return ERROR;
        } else if (score >= warningThreshold) {
            return WARNING;
        } else {
            return INFO;
        }
    }
}
