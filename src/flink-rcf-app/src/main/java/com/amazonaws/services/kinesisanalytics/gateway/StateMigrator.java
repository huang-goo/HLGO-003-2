/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.kinesisanalytics.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StateMigrator {

    private static final Logger logger = LoggerFactory.getLogger(StateMigrator.class);

    public static final int CURRENT_STATE_VERSION = 2;

    public boolean needsMigration(AlgorithmState state) {
        return state == null || state.getStateVersion() < CURRENT_STATE_VERSION;
    }

    public AlgorithmState migrate(AlgorithmState oldState) {
        if (oldState == null) {
            return null;
        }

        int fromVersion = oldState.getStateVersion();

        if (fromVersion >= CURRENT_STATE_VERSION) {
            return oldState;
        }

        AlgorithmState migrated = oldState;
        int currentVersion = fromVersion;

        while (currentVersion < CURRENT_STATE_VERSION) {
            migrated = migrateVersion(migrated, currentVersion, currentVersion + 1);
            currentVersion = migrated.getStateVersion();
        }

        logger.info("State migrated from version {} to {} (algorithm: {})",
                fromVersion, currentVersion, migrated.getAlgorithmType());

        return migrated;
    }

    private AlgorithmState migrateVersion(AlgorithmState state, int from, int to) {
        switch (from) {
            case 1:
                return migrateV1ToV2(state);
            default:
                logger.warn("No migration path from version {} to {}", from, to);
                return state;
        }
    }

    private AlgorithmState migrateV1ToV2(AlgorithmState v1State) {
        AlgorithmState v2State = new AlgorithmState(v1State.getAlgorithmType());
        v2State.setStateVersion(2);
        v2State.setProcessedCount(v1State.getProcessedCount());
        v2State.setLastUpdateTime(System.currentTimeMillis());

        Map<String, Object> v1Data = v1State.getStateData();
        if (v1Data != null) {
            for (Map.Entry<String, Object> entry : v1Data.entrySet()) {
                v2State.put(entry.getKey(), entry.getValue());
            }
        }

        v2State.put("migration_history", "v1->v2");
        v2State.put("state_format_version", "2.0");

        return v2State;
    }

    public static AlgorithmState createEmptyState(String algorithmType) {
        AlgorithmState state = new AlgorithmState(algorithmType);
        state.setStateVersion(CURRENT_STATE_VERSION);
        return state;
    }
}
