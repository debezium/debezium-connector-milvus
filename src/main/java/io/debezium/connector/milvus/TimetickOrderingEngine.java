/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.Clock;

/**
 * TSO-based ordering engine for Milvus change events.
 *
 * <p>
 * Multiple proxy nodes write to the same vchannels concurrently.
 * Messages may arrive out of TSO order across vchannels.
 * This engine buffers DML/DDL events and emits them in strict TSO order using
 * a per-vchannel timetick watermark mechanism.
 * </p>
 *
 * <ul>
 * <li>{@code TreeMap<Long, List<MilvusChangeEvent>> pendingByTso} — TSO-keyed
 * buffer. Multiple events at the same TSO are allowed (batch insert from
 * two proxies at the same physical millisecond).</li>
 * <li>{@code Map<String, Long> latestTimetickByVchannel} — per-vchannel
 * timetick watermarks.</li>
 * <li>{@code Set<String> trackedVchannels} — all vchannels this engine is
 * tracking.</li>
 * </ul>
 *
 * <p>
 * <b>Watermark computation</b>:
 * {@code globalWatermark = min(latestTimetickByVchannel.values())}. Only events
 * with {@code tso <= globalWatermark} are safe to emit.
 * </p>
 *
 * <p>
 * <b>Stall detection</b>: If the global watermark does not advance for
 * {@code timetickStallTimeoutMs}, the engine is considered stalled. A
 * force-flush
 * can then be triggered with an emergency watermark of
 * {@code max(pendingByTso.keys())}.
 * </p>
 *
 */
public class TimetickOrderingEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimetickOrderingEngine.class);

    /**
     * Approximate overhead per buffered event entry in bytes. Used for the
     * byte-limit heuristic alongside actual payload estimates.
     */
    private static final int EVENT_OVERHEAD_BYTES = 256;

    private final int maxBufferedEvents;
    private final long maxBufferedBytes;
    private final long timetickStallTimeoutMs;
    private final Clock clock;

    private final TreeMap<Long, List<MilvusChangeEvent>> pendingByTso = new TreeMap<>();
    private final Map<String, Long> latestTimetickByVchannel = new HashMap<>();
    private final Set<String> trackedVchannels = new HashSet<>();

    private int bufferedEventCount;
    private long bufferedBytes;
    private long globalWatermark;
    private long lastWatermarkAdvanceTimeMs;
    private long forceFlushCount;
    private long lateMessagesDropped;
    private long channelTimetick;

    public TimetickOrderingEngine(MilvusConnectorConfig config) {
        this(config, Clock.system());
    }

    TimetickOrderingEngine(MilvusConnectorConfig config, Clock clock) {
        this.maxBufferedEvents = config.getMaxBufferedEvents();
        this.maxBufferedBytes = config.getMaxBufferedBytes();
        this.timetickStallTimeoutMs = config.getTimetickStallTimeoutMs();
        this.clock = clock;
        this.lastWatermarkAdvanceTimeMs = clock.currentTimeInMillis();
    }

    /**
     * Buffer a DML or DDL event for TSO-ordered emission.
     *
     * <p>
     * Late events (events whose TSO is at or below the already-emitted
     * global watermark) are dropped with a WARN log. This is the hardcoded
     * {@code drop_and_warn} policy.
     * </p>
     *
     * @param event the change event to buffer; must not be null
     * @throws MilvusBufferFullException if adding this event would exceed either
     *                                   {@code maxBufferedEvents} or
     *                                   {@code maxBufferedBytes}
     */
    public void buffer(MilvusChangeEvent event) throws MilvusBufferFullException {
        long tso = event.getTso();

        // Late message detection: event arrived after we already flushed past its TSO
        if (globalWatermark > 0 && tso <= globalWatermark) {
            lateMessagesDropped++;
            LOGGER.warn("Dropping late event: vchannel={}, tso={}, watermark={}, collection={}",
                    event.getVchannel(), tso, globalWatermark, event.getCollectionName());
            return;
        }

        long eventBytes = estimateEventBytes(event);

        // Check buffer limits before adding
        if (bufferedEventCount + 1 > maxBufferedEvents) {
            throw new MilvusBufferFullException(bufferedEventCount, maxBufferedEvents);
        }
        if (bufferedBytes + eventBytes > maxBufferedBytes) {
            throw new MilvusBufferFullException(bufferedBytes + eventBytes, maxBufferedBytes);
        }

        pendingByTso.computeIfAbsent(tso, k -> new ArrayList<>(4)).add(event);
        bufferedEventCount++;
        bufferedBytes += eventBytes;
        String vchannel = event.getVchannel();
        if (vchannel != null && trackedVchannels.add(vchannel) && channelTimetick > 0) {
            latestTimetickByVchannel.putIfAbsent(vchannel, channelTimetick);
        }
    }

    /**
     * Process a TimeTick watermark event for a vchannel.
     *
     * <p>
     * Updates the per-vchannel timetick and recomputes the global watermark
     * as {@code min(latestTimetickByVchannel.values())}.
     * </p>
     *
     * @param vchannel the vchannel name; must not be null or blank
     * @param tso      the timetick TSO value
     */
    public void updateWatermark(String vchannel, long tso) {
        trackedVchannels.add(vchannel);

        Long previous = latestTimetickByVchannel.get(vchannel);
        if (previous == null || tso > previous) {
            latestTimetickByVchannel.put(vchannel, tso);
        }

        advanceWatermark();
    }

    /**
     * Process a channel-level TimeTick, i.e. one whose {@code TimeTickMsg}
     * carries no shard name. Milvus emits these per pchannel as the minimum
     * guarantee across every producer on the channel: no message with a lower
     * TSO can still arrive on any vchannel multiplexed onto this pchannel.
     * The tick therefore advances every tracked vchannel at once, and is
     * remembered so vchannels discovered later start from it rather than from
     * zero (which would drag the global watermark back to zero).
     *
     * @param tso the timetick TSO value
     */
    public void updateChannelWatermark(long tso) {
        if (tso > channelTimetick) {
            channelTimetick = tso;
        }
        for (String vchannel : trackedVchannels) {
            latestTimetickByVchannel.merge(vchannel, tso, Math::max);
        }
        if (trackedVchannels.isEmpty() && tso > globalWatermark) {
            globalWatermark = tso;
            lastWatermarkAdvanceTimeMs = clock.currentTimeInMillis();
            return;
        }
        advanceWatermark();
    }

    private void advanceWatermark() {
        long newWatermark = computeWatermark();
        if (newWatermark > globalWatermark) {
            globalWatermark = newWatermark;
            lastWatermarkAdvanceTimeMs = clock.currentTimeInMillis();
        }
    }

    /**
     * Compute the global watermark as the minimum of all tracked vchannel
     * timeticks.
     *
     * @return the global watermark TSO, or {@code 0L} if no vchannels are
     *         tracked or no timeticks have been received
     */
    public long computeWatermark() {
        if (latestTimetickByVchannel.isEmpty()) {
            return 0L;
        }
        long min = Long.MAX_VALUE;
        for (String vc : trackedVchannels) {
            Long tt = latestTimetickByVchannel.get(vc);
            long value = (tt != null) ? tt : 0L;
            if (value < min) {
                min = value;
            }
        }
        return min == Long.MAX_VALUE ? 0L : min;
    }

    /**
     * Flush all buffered events whose TSO is at or below the current global
     * watermark. Events are returned in strict ascending TSO order.
     *
     * @return the flushed events in TSO order; empty if nothing is flushable
     */
    public List<MilvusChangeEvent> flush() {
        if (pendingByTso.isEmpty() || globalWatermark <= 0) {
            return Collections.emptyList();
        }

        List<MilvusChangeEvent> flushed = new ArrayList<>();
        var flushable = new TreeMap<>(pendingByTso.headMap(globalWatermark, true));

        for (Map.Entry<Long, List<MilvusChangeEvent>> entry : flushable.entrySet()) {
            flushed.addAll(orderWithinTso(entry.getValue()));
            long entryBytes = entry.getValue().stream()
                    .mapToLong(this::estimateEventBytes)
                    .sum();
            bufferedEventCount -= entry.getValue().size();
            bufferedBytes -= entryBytes;
            pendingByTso.remove(entry.getKey());
        }

        return flushed;
    }

    /**
     * Order events sharing one TSO so that deletes are emitted before inserts.
     * A Milvus upsert expands to a delete and an insert stamped with the same
     * TSO; downstream consumers must observe the delete first or the applied
     * end state is a deleted row instead of the upserted one. The sort is
     * stable, so arrival order is preserved within each operation type.
     */
    private static List<MilvusChangeEvent> orderWithinTso(List<MilvusChangeEvent> events) {
        if (events.size() < 2) {
            return events;
        }
        List<MilvusChangeEvent> ordered = new ArrayList<>(events);
        ordered.sort(Comparator.comparingInt(e -> e instanceof MilvusChangeEvent.Delete ? 0 : 1));
        return ordered;
    }

    /**
     * Force-flush all buffered events using an emergency watermark equal to the
     * maximum buffered TSO. This is an explicit relaxation of strict TSO ordering
     * used only when the watermark is stalled.
     *
     * <p>
     * After a force-flush, events arriving with TSO below the emergency
     * watermark will be dropped by the {@code drop_and_warn} late-message
     * policy.
     * </p>
     *
     * @return the force-flushed events in TSO order; empty if buffer is empty
     */
    public List<MilvusChangeEvent> forceFlush() {
        if (pendingByTso.isEmpty()) {
            return Collections.emptyList();
        }

        long emergencyWatermark = pendingByTso.lastKey();
        LOGGER.warn("Force-flushing with emergency watermark={} (previous watermark={}). "
                + "Strict TSO ordering is relaxed for this flush. bufferedEvents={}, bufferedBytes={}",
                emergencyWatermark, globalWatermark, bufferedEventCount, bufferedBytes);

        globalWatermark = emergencyWatermark;
        lastWatermarkAdvanceTimeMs = clock.currentTimeInMillis();
        forceFlushCount++;

        List<MilvusChangeEvent> flushed = new ArrayList<>();
        for (List<MilvusChangeEvent> events : pendingByTso.values()) {
            flushed.addAll(orderWithinTso(events));
        }

        pendingByTso.clear();
        bufferedEventCount = 0;
        bufferedBytes = 0;

        return flushed;
    }

    /**
     * Check whether the ordering engine is stalled.
     *
     * <p>
     * The engine is stalled when the buffer is non-empty and no watermark
     * progress has been made for {@code timetickStallTimeoutMs}.
     * </p>
     *
     * @return {@code true} if stalled
     */
    public boolean isStalled() {
        if (pendingByTso.isEmpty()) {
            return false;
        }
        long elapsed = clock.currentTimeInMillis() - lastWatermarkAdvanceTimeMs;
        return elapsed >= timetickStallTimeoutMs;
    }

    /**
     * Pre-warm the engine with stored vchannel timeticks from a previous offset.
     * This avoids a zero-watermark stall on restart.
     *
     * @param vchannelTimeticks the stored timeticks; may be null or empty
     */
    public void preWarm(Map<String, Long> vchannelTimeticks) {
        if (vchannelTimeticks == null || vchannelTimeticks.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Long> entry : vchannelTimeticks.entrySet()) {
            String vc = entry.getKey();
            Long tso = entry.getValue();
            if (vc != null && tso != null) {
                trackedVchannels.add(vc);
                latestTimetickByVchannel.put(vc, tso);
            }
        }

        long newWatermark = computeWatermark();
        if (newWatermark > globalWatermark) {
            globalWatermark = newWatermark;
            lastWatermarkAdvanceTimeMs = clock.currentTimeInMillis();
        }

        LOGGER.info("Pre-warmed ordering engine with {} vchannels, watermark={}",
                vchannelTimeticks.size(), globalWatermark);
    }

    /**
     * @return the number of events currently buffered
     */
    public int getBufferedEventCount() {
        return bufferedEventCount;
    }

    /**
     * @return approximate number of bytes currently buffered
     */
    public long getBufferedBytes() {
        return bufferedBytes;
    }

    /**
     * @return the current global watermark TSO
     */
    public long getGlobalWatermark() {
        return globalWatermark;
    }

    /**
     * @return an unmodifiable copy of the current per-vchannel timeticks
     */
    public Map<String, Long> getVchannelTimeticks() {
        return Collections.unmodifiableMap(new HashMap<>(latestTimetickByVchannel));
    }

    /**
     * @return the number of force-flush operations performed
     */
    public long getForceFlushCount() {
        return forceFlushCount;
    }

    /**
     * @return the number of late messages dropped
     */
    public long getLateMessagesDropped() {
        return lateMessagesDropped;
    }

    /**
     * @return the set of vchannels that appear to be stalled (no timetick
     *         received, or timetick below the majority)
     */
    public Set<String> getStalledVchannels() {
        if (latestTimetickByVchannel.isEmpty()) {
            return Collections.emptySet();
        }
        long max = latestTimetickByVchannel.values().stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L);

        Set<String> stalled = new HashSet<>();
        for (String vc : trackedVchannels) {
            Long tt = latestTimetickByVchannel.get(vc);
            if (tt == null || tt < max) {
                stalled.add(vc);
            }
        }
        return stalled;
    }

    /**
     * @return an unmodifiable view of currently tracked vchannels
     */
    public Set<String> getTrackedVchannels() {
        return Collections.unmodifiableSet(trackedVchannels);
    }

    /**
     * Estimate the memory footprint of a single event for byte-budget tracking.
     * This is a rough heuristic, not an exact measurement.
     */
    private long estimateEventBytes(MilvusChangeEvent event) {
        long bytes = EVENT_OVERHEAD_BYTES;
        if (event instanceof MilvusChangeEvent.Insert insert) {
            MilvusRow row = insert.getRow();
            if (row != null) {
                for (Object value : row.getFieldValues()) {
                    bytes += estimateValueBytes(value);
                }
            }
        }
        return bytes;
    }

    /**
     * Estimate the byte size of a single field value.
     */
    private static long estimateValueBytes(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof byte[] bytes) {
            return bytes.length;
        }
        if (value instanceof float[] floats) {
            return (long) floats.length * Float.BYTES;
        }
        if (value instanceof String s) {
            return (long) s.length() * 2;
        }
        if (value instanceof List<?> list) {
            long total = 0;
            for (Object elem : list) {
                total += estimateValueBytes(elem);
            }
            return total;
        }
        return 16;
    }
}
