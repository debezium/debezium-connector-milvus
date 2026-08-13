/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The full offset state, the MQ positions, per-vchannel timetick watermarks, and checkpoint
 * timestamp, is compared against the value captured when the monitor was last consulted, and
 * when none have moved, a stale result is reported. Milvus publishes timetick messages on the
 * physical channel continuously even when no data is written, so a stationary offset means
 * the connector is no longer consuming anything from the message queue rather than that the
 * captured collections are quiet.
 * <p>
 * No check is performed until the first MQ position has been recorded, so an idle channel
 * that has not yet delivered its first message after a seek to latest is not reported as
 * stale.
 *
 * @author Chris Cranford
 */
public class MilvusOffsetActivityMonitor implements OffsetActivityMonitor<MilvusPartition, MilvusOffsetContext> {

    private final Duration checkInterval;

    private Map<String, ?> previousOffset;

    public MilvusOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(MilvusPartition partition, MilvusOffsetContext offsetContext) {
        final Map<String, ?> offset = offsetContext.getOffset();
        final Long mqOffset = offsetContext.getMqOffset(partition.getPchannel());

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (mqOffset != null && Objects.equals(previousOffset, offset)) {
            result = StaleOffsetsResult.stale(
                    ("Offsets at MQ position %d for pchannel '%s' have not changed in %d milliseconds. " +
                            "Milvus publishes timetick messages on the physical channel continuously even when idle, " +
                            "so this may indicate the connector is no longer receiving messages from the message queue.")
                            .formatted(mqOffset, partition.getPchannel(), checkInterval.toMillis()));
        }

        // Update tracked stats
        previousOffset = offset;

        return result;
    }

}