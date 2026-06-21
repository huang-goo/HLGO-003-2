/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import java.io.Serializable;
import java.util.Arrays;

public class AnomalyInput implements Serializable {
    private static final long serialVersionUID = 1L;

    private String trafficTag;
    private double[] values;
    private long timestamp;

    public AnomalyInput() {
        this.timestamp = System.currentTimeMillis();
    }

    public AnomalyInput(String trafficTag, double[] values) {
        this();
        this.trafficTag = trafficTag;
        this.values = values;
    }

    public String getTrafficTag() {
        return trafficTag;
    }

    public void setTrafficTag(String trafficTag) {
        this.trafficTag = trafficTag;
    }

    public double[] getValues() {
        return values;
    }

    public void setValues(double[] values) {
        this.values = values;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AnomalyInput{" +
                "trafficTag='" + trafficTag + '\'' +
                ", values=" + Arrays.toString(values) +
                ", timestamp=" + timestamp +
                '}';
    }
}
