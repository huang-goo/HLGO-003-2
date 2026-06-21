/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GatewayIntegrationExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameter = ParameterTool.fromArgs(args);

        int dimensions = 5;

        DataStream<AnomalyInput> inputStream = env.fromElements(
                createInput("stream-6", new double[]{1.0, 2.0, 3.0, 4.0, 5.0}),
                createInput("stream-6", new double[]{1.1, 2.1, 3.1, 4.1, 5.1}),
                createInput("stream-9", new double[]{2.0, 3.0, 4.0, 5.0, 6.0}),
                createInput("stream-11", new double[]{0.5, 1.5, 2.5, 3.5, 4.5})
        );

        SingleOutputStreamOperator<AnomalyScoreResult> resultStream = inputStream
                .keyBy(AnomalyInput::getTrafficTag)
                .process(new AnomalyDetectOperator(parameter, dimensions))
                .name("AnomalyDetectGateway");

        DataStream<AnomalyEvent> eventStream = resultStream
                .getSideOutput(AnomalyDetectOperator.ANOMALY_EVENT_OUTPUT_TAG);

        resultStream.print("score");
        eventStream.print("event");

        env.execute("Anomaly Gateway Example");
    }

    private static AnomalyInput createInput(String tag, double[] values) {
        return new AnomalyInput(tag, values);
    }
}
