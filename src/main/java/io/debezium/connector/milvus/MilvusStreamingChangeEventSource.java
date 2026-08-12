/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.milvus.checkpoint.ChannelCheckpoint;
import io.debezium.connector.milvus.checkpoint.EtcdCheckpointReader;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.OffsetActivityMonitorService;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.Strings;
import io.milvus.grpc.DataType;

/**
 * Streaming change event source for Milvus.
 *
 * <ol>
 * <li>{@code poll()} raw messages from Kafka via
 * {@link MilvusMessageConsumer}</li>
 * <li>Deserialize via {@link MilvusProtoDeserializer}</li>
 * <li>Buffer DML/DDL events and update watermark via
 * {@link TimetickOrderingEngine}</li>
 * <li>Flush ready events in strict TSO order</li>
 * <li>Dispatch flushed events through the Debezium
 * {@link EventDispatcher} pipeline</li>
 * <li>Update offset context with MQ position and vchannel timeticks</li>
 * </ol>
 *
 */
public class MilvusStreamingChangeEventSource
        implements StreamingChangeEventSource<MilvusPartition, MilvusOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusStreamingChangeEventSource.class);

    /** Maximum number of attempts when resolving etcd checkpoint offsets for snapshot handoff. */
    private static final int CHECKPOINT_RESOLVE_MAX_ATTEMPTS = 3;

    private final MilvusConnectorConfig connectorConfig;
    private final MilvusMessageConsumer messageConsumer;
    private final MilvusProtoDeserializer deserializer;
    private final TimetickOrderingEngine orderingEngine;
    private final EventDispatcher<MilvusPartition, TableId> dispatcher;
    private final MilvusDatabaseSchema databaseSchema;
    private final Duration pollTimeout;
    private final EtcdCheckpointReader checkpointReader;
    private final MilvusStreamingChangeEventSourceMetrics streamingMetrics;
    private final OffsetActivityMonitorService offsetActivityMonitorService;
    private OffsetActivityMonitor<MilvusPartition, MilvusOffsetContext> offsetActivityMonitor;

    public MilvusStreamingChangeEventSource(MilvusConnectorConfig connectorConfig,
                                            MilvusMessageConsumer messageConsumer,
                                            MilvusProtoDeserializer deserializer,
                                            TimetickOrderingEngine orderingEngine,
                                            EventDispatcher<MilvusPartition, TableId> dispatcher,
                                            MilvusDatabaseSchema databaseSchema,
                                            EtcdCheckpointReader checkpointReader,
                                            MilvusStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.messageConsumer = messageConsumer;
        this.deserializer = deserializer;
        this.orderingEngine = orderingEngine;
        this.dispatcher = dispatcher;
        this.databaseSchema = databaseSchema;
        this.pollTimeout = Duration.ofMillis(connectorConfig.getPollIntervalMs());
        this.checkpointReader = checkpointReader;
        this.streamingMetrics = streamingMetrics;
        this.offsetActivityMonitorService = OffsetActivityMonitorService.lookup(connectorConfig.getServiceRegistry());
    }

    @Override
    public void execute(ChangeEventSource.ChangeEventSourceContext context,
                        MilvusPartition partition,
                        MilvusOffsetContext offsetContext)
            throws InterruptedException {

        LOGGER.info("Starting Milvus streaming change event source for partition {}", partition);

        if (offsetContext == null) {
            LOGGER.warn("No offset context provided to streaming source; initializing a fresh offset context");
            offsetContext = new MilvusOffsetContext(new MilvusSourceInfo(connectorConfig));
        }

        String pchannel = partition.getPchannel();
        int partitionIndex = connectorConfig.getKafkaPartitionIndex();
        TopicPartition tp = new TopicPartition(pchannel, partitionIndex);

        preWarmEngine(offsetContext);

        seekConsumer(pchannel, tp, offsetContext);
        streamingMetrics.positionResolved(true);

        boolean bufferFull = false;

        while (context.isRunning()) {
            if (context.isPaused()) {
                context.waitStreamingPaused();
                continue;
            }

            try {
                offsetActivityMonitorService.pulse(partition, offsetContext);

                if (!bufferFull) {
                    List<RawMilvusMessage> messages = messageConsumer.poll(pollTimeout);
                    if (!messages.isEmpty()) {
                        processMessages(messages, pchannel, offsetContext);
                    }
                }

                List<MilvusChangeEvent> flushed = orderingEngine.flush();
                if (!flushed.isEmpty()) {
                    dispatchAndUpdateWatermark(flushed, partition, offsetContext);
                }

                if (orderingEngine.isStalled()) {
                    LOGGER.warn("Timetick stall detected. stalledVchannels={}, bufferedEvents={}, "
                            + "bufferedBytes={}, watermark={}",
                            orderingEngine.getStalledVchannels(),
                            orderingEngine.getBufferedEventCount(),
                            orderingEngine.getBufferedBytes(),
                            orderingEngine.getGlobalWatermark());

                    List<MilvusChangeEvent> forceFlushed = orderingEngine.forceFlush();
                    if (!forceFlushed.isEmpty()) {
                        dispatchAndUpdateWatermark(forceFlushed, partition, offsetContext);
                    }
                }

                bufferFull = false;

                offsetContext.setVchannelTimeticks(orderingEngine.getVchannelTimeticks());

                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            catch (MilvusBufferFullException e) {
                LOGGER.warn("Buffer full: {}. Pausing poll, waiting for watermark to advance.", e.getMessage());
                bufferFull = true;

                List<MilvusChangeEvent> flushed = orderingEngine.flush();
                if (!flushed.isEmpty()) {
                    dispatchAndUpdateWatermark(flushed, partition, offsetContext);
                    bufferFull = false;
                }
                else if (orderingEngine.isStalled()) {
                    List<MilvusChangeEvent> forceFlushed = orderingEngine.forceFlush();
                    if (!forceFlushed.isEmpty()) {
                        dispatchAndUpdateWatermark(forceFlushed, partition, offsetContext);
                    }
                    bufferFull = false;
                }
                else {
                    Thread.sleep(Math.min(pollTimeout.toMillis(), 1000));
                }
            }
            catch (MilvusWireFormatMismatchException e) {
                LOGGER.error("Fatal wire format error during streaming: {}", e.getMessage(), e);
                throw new DebeziumException("Wire format mismatch during streaming", e);
            }
            catch (Exception e) {
                if (e instanceof InterruptedException) {
                    LOGGER.info("Streaming interrupted");
                }
                else {
                    LOGGER.error("Error during streaming execution", e);
                }
                throw e;
            }
        }

        LOGGER.info("Milvus streaming change event source stopped. "
                + "forceFlushCount={}, lateMessagesDropped={}, finalWatermark={}",
                orderingEngine.getForceFlushCount(),
                orderingEngine.getLateMessagesDropped(),
                orderingEngine.getGlobalWatermark());
        streamingMetrics.positionResolved(false);
    }

    @Override
    public void close() {
        streamingMetrics.positionResolved(false);
        if (messageConsumer != null) {
            messageConsumer.close();
        }
    }

    @Override
    public Optional<OffsetActivityMonitor<MilvusPartition, MilvusOffsetContext>> getOffsetActivityMonitor() {
        if (offsetActivityMonitor == null) {
            offsetActivityMonitor = new MilvusOffsetActivityMonitor(connectorConfig.getOffsetActivityMonitorInterval());
        }
        return Optional.of(offsetActivityMonitor);
    }

    /**
     * Process raw messages: deserialize and route to the ordering engine.
     */
    private void processMessages(List<RawMilvusMessage> messages, String pchannel,
                                 MilvusOffsetContext offsetContext)
            throws MilvusWireFormatMismatchException, MilvusBufferFullException {

        for (RawMilvusMessage message : messages) {
            offsetContext.setMqPosition(message.getTopic(), message.getPartition(), message.getOffset());

            List<MilvusChangeEvent> events = deserializer.deserialize(message);

            for (MilvusChangeEvent event : events) {
                if (event instanceof MilvusChangeEvent.TimeTick timeTick) {
                    String vchannel = Strings.defaultIfEmpty(timeTick.getVchannel(), pchannel);
                    orderingEngine.updateWatermark(vchannel, timeTick.getTso());
                }
                else {
                    orderingEngine.buffer(event);
                }
            }
        }
    }

    /**
     * Dispatch a batch of flushed events and update the global watermark metric
     * in one step. These two operations must always happen together, so every
     * flush/force-flush call site in {@link #execute} routes through here rather
     * than duplicating the pairing.
     */
    private void dispatchAndUpdateWatermark(List<MilvusChangeEvent> events,
                                            MilvusPartition partition,
                                            MilvusOffsetContext offsetContext)
            throws InterruptedException {
        dispatchFlushedEvents(events, partition, offsetContext);
        streamingMetrics.updateGlobalWatermark(orderingEngine.getGlobalWatermark());
    }

    /**
     * Dispatch flushed events through the Debezium {@link EventDispatcher}.
     *
     * <p>
     * For each event: determines the operation, registers the collection
     * schema if needed, creates a {@link MilvusChangeRecordEmitter}, and
     * dispatches via {@link EventDispatcher#dispatchDataChangeEvent}.
     * </p>
     */
    private void dispatchFlushedEvents(List<MilvusChangeEvent> events,
                                       MilvusPartition partition,
                                       MilvusOffsetContext offsetContext)
            throws InterruptedException {
        String dbName = connectorConfig.getMilvusDatabase();
        int dispatched = 0;

        for (MilvusChangeEvent event : events) {
            String collectionName = event.getCollectionName();

            if (collectionName == null || collectionName.isEmpty()) {
                LOGGER.debug("Skipping event with no collection name: type={}, tso={}",
                        event.getClass().getSimpleName(), event.getTso());
                continue;
            }

            TableId tableId = new TableId(null, dbName, collectionName);

            if (!databaseSchema.isCollectionRegistered(tableId)) {
                if (event instanceof MilvusChangeEvent.Insert insert
                        && insert.getRow().size() > 0) {
                    MilvusRow row = insert.getRow();
                    List<FieldDefinition> fields = new ArrayList<>(row.size());
                    String[] fieldNames = row.getFieldNames();
                    Object[] fieldValues = row.getFieldValues();
                    DataType[] fieldTypes = row.getFieldTypes();
                    for (int i = 0; i < row.size(); i++) {
                        fields.add(new FieldDefinition(fieldNames[i], fieldValues[i], fieldTypes[i]));
                    }
                    databaseSchema.registerCollection(dbName, collectionName, fields);
                }
                else {
                    boolean registered = databaseSchema.registerCollectionFromMetadata(dbName, collectionName);
                    if (!registered) {
                        LOGGER.debug("Skipping event for unregistered collection {}: type={}",
                                collectionName, event.getClass().getSimpleName());
                        continue;
                    }
                }
            }

            Envelope.Operation operation = determineOperation(event);
            if (operation == null) {
                continue;
            }

            String[] columnNames = databaseSchema.getColumnNames(tableId);
            String pkFieldName = databaseSchema.getPkFieldName(tableId);

            offsetContext.updateForEvent(dbName, collectionName,
                    event.getPchannel(), event.getVchannel(), event.getTso());

            MilvusChangeRecordEmitter emitter = new MilvusChangeRecordEmitter(
                    partition, offsetContext, Clock.SYSTEM, connectorConfig,
                    event, operation, columnNames, pkFieldName);

            dispatcher.dispatchDataChangeEvent(partition, tableId, emitter);
            dispatched++;
        }

        if (dispatched > 0) {
            LOGGER.info("Dispatched {} events (watermark={})",
                    dispatched, orderingEngine.getGlobalWatermark());
            streamingMetrics.updateSourceEventPosition(partition.getPchannel(), orderingEngine.getGlobalWatermark());
        }
    }

    /**
     * Map a change event to the corresponding Debezium envelope operation.
     */
    private static Envelope.Operation determineOperation(MilvusChangeEvent event) {
        if (event instanceof MilvusChangeEvent.Insert) {
            return Envelope.Operation.CREATE;
        }
        if (event instanceof MilvusChangeEvent.Delete) {
            return Envelope.Operation.DELETE;
        }
        return null;
    }

    /**
     * Pre-warm the ordering engine from stored vchannel timeticks in the
     * offset context. This avoids a zero-watermark stall on restart.
     */
    private void preWarmEngine(MilvusOffsetContext offsetContext) {
        Map<String, Long> storedTimeticks = extractVchannelTimeticks(offsetContext);
        orderingEngine.preWarm(storedTimeticks);
    }

    /**
     * Extract stored per-vchannel timeticks from the offset context.
     */
    private Map<String, Long> extractVchannelTimeticks(MilvusOffsetContext offsetContext) {
        Map<String, Long> timeticks = new HashMap<>();
        Map<String, ?> offsetMap = offsetContext.getOffset();
        for (Map.Entry<String, ?> entry : offsetMap.entrySet()) {
            String key = entry.getKey();
            if (key != null && key.startsWith("vchannel_timetick_")) {
                String vchannel = key.substring("vchannel_timetick_".length());
                if (!Strings.isNullOrBlank(vchannel) && entry.getValue() != null) {
                    timeticks.put(vchannel, Strings.asLong(entry.getValue().toString(), 0L));
                }
            }
        }
        return timeticks;
    }

    /**
     * Seek the consumer to the appropriate starting position.
     *
     * <p>
     * Mirrors the behaviour of Postgres {@code NO_DATA} and Oracle
     * {@code NO_DATA} snapshot modes: when there is no stored offset and no
     * snapshot checkpoint to resume from, the consumer seeks to the
     * <em>end</em> of the pchannel (i.e. "now") rather than the beginning.
     * Replaying the entire history is rarely useful and would re-deliver
     * stale events from prior connector runs or test setup.
     * </p>
     */
    private void seekConsumer(String pchannel, TopicPartition tp,
                              MilvusOffsetContext offsetContext)
            throws InterruptedException {
        Long storedOffset = offsetContext.getMqOffset(pchannel);

        if (storedOffset != null) {
            messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.STORED_OFFSET_PLUS_ONE,
                    Map.of(tp, storedOffset));
            LOGGER.info("Resumed from stored offset + 1 = {}", storedOffset + 1);
        }
        else if (!offsetContext.isSnapshotCompleted()) {
            Map<TopicPartition, Long> checkpointOffsets = resolveCheckpointOffsets(pchannel);
            if (checkpointOffsets != null && !checkpointOffsets.isEmpty()) {
                messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.DEFAULT, checkpointOffsets);
                LOGGER.info("Resumed from snapshot checkpoint offset for pchannel {}", pchannel);
            }
            else {
                LOGGER.warn(
                        "No checkpoint offset available for snapshot handoff on pchannel {}, falling back to LATEST",
                        pchannel);
                messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.LATEST, null);
            }
        }
        else {
            messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.LATEST, null);
            LOGGER.info("No stored offset, seeking to latest");
        }
    }

    /**
     * Resolve checkpoint offsets for snapshot-to-streaming handoff.
     *
     * <p>
     * Reads the etcd channel checkpoint for the given pchannel. If a checkpoint
     * exists, returns a map with the Kafka {@link TopicPartition} → offset so the
     * consumer can seek to the exact position corresponding to the snapshot's
     * {@code guarantee_ts}. Returns {@code null} when no checkpoint is found,
     * causing the caller to fall back to {@link SeekPosition#LATEST}.
     * </p>
     *
     * <p>
     * Transient etcd errors (e.g. a network blip) are retried with exponential
     * backoff. If all retries are exhausted the exception is rethrown so the
     * connector fails hard rather than silently skipping to
     * {@link SeekPosition#LATEST}, which could drop all events between the
     * checkpoint and the current head.
     * </p>
     */
    private Map<TopicPartition, Long> resolveCheckpointOffsets(String pchannel)
            throws InterruptedException {
        if (checkpointReader == null) {
            return null;
        }

        DelayStrategy delay = DelayStrategy.exponential(
                Duration.ofMillis(500), Duration.ofSeconds(5));
        Exception lastError = null;

        for (int attempt = 1; attempt <= CHECKPOINT_RESOLVE_MAX_ATTEMPTS; attempt++) {
            try {
                Optional<ChannelCheckpoint> checkpointOpt = checkpointReader.read(pchannel);
                if (checkpointOpt.isEmpty()) {
                    return null;
                }
                ChannelCheckpoint checkpoint = checkpointOpt.get();
                long kafkaOffset = checkpoint.getKafkaOffset();
                TopicPartition tp = new TopicPartition(pchannel, connectorConfig.getKafkaPartitionIndex());
                LOGGER.info("Resolved checkpoint offset for pchannel={}: kafkaOffset={}, guaranteeTs={}",
                        pchannel, kafkaOffset, checkpoint.getTimestamp());
                return Map.of(tp, kafkaOffset);
            }
            catch (Exception e) {
                lastError = e;
                if (attempt < CHECKPOINT_RESOLVE_MAX_ATTEMPTS) {
                    LOGGER.warn("Failed to resolve checkpoint offsets for pchannel={} (attempt {}/{}): {}. Retrying.",
                            pchannel, attempt, CHECKPOINT_RESOLVE_MAX_ATTEMPTS, e.getMessage());
                    delay.sleepWhen(true);
                }
            }
        }

        throw new DebeziumException(String.format(
                "Failed to resolve etcd checkpoint offsets for pchannel=%s after %d attempts; "
                        + "cannot safely resume streaming without risking data loss.",
                pchannel, CHECKPOINT_RESOLVE_MAX_ATTEMPTS), lastError);
    }
}
