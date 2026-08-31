/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.milvus.checkpoint.EtcdCheckpointReader;
import io.debezium.connector.milvus.metadata.MilvusMetadataClient;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;

/**
 * Factory for creating Milvus change event sources.
 *
 * <p>Builds the streaming source with its full dependency chain:
 * {@link KafkaMilvusMessageConsumer} → {@link MilvusProtoDeserializer} →
 * {@link TimetickOrderingEngine} → {@link MilvusStreamingChangeEventSource},
 * wired with the {@link EventDispatcher} and {@link MilvusDatabaseSchema}.</p>
 *
 * <p>Passes the shared {@link EtcdCheckpointReader} to both the snapshot
 * source (for reading {@code guarantee_ts}) and the streaming source (for
 * resolving the Kafka offset for snapshot-to-streaming handoff).</p>
 *
 * <p>The MQ wire format handed to the deserializer is resolved once, when the
 * streaming source is first built. An explicit {@code milvus.wire.format} is
 * used as-is; {@code auto} (the default) runs {@link MilvusWireFormatDetector}
 * against the configured pchannel. On a warm restart the probe starts from the
 * stored MQ offset so it inspects the messages that will actually be processed
 * next rather than potentially pre-upgrade payloads at the head of the topic.</p>
 */
public class MilvusChangeEventSourceFactory implements ChangeEventSourceFactory<MilvusPartition, MilvusOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusChangeEventSourceFactory.class);

    static final String WIRE_FORMAT_AUTO = "auto";

    private final MilvusConnectorConfig connectorConfig;
    private final EventDispatcher<MilvusPartition, TableId> dispatcher;
    private final MilvusDatabaseSchema schema;
    private final EtcdCheckpointReader checkpointReader;
    private final MilvusSnapshotQueryClient snapshotQueryClient;
    private final MilvusMetadataClient metadataClient;
    private final MilvusStreamingChangeEventSourceMetrics streamingMetrics;
    private final MilvusOffsetContext previousOffset;
    private final Supplier<MilvusWireFormatDetector> detectorSupplier;

    private String resolvedWireFormat;

    public MilvusChangeEventSourceFactory(MilvusConnectorConfig connectorConfig,
                                          EventDispatcher<MilvusPartition, TableId> dispatcher,
                                          MilvusDatabaseSchema schema,
                                          EtcdCheckpointReader checkpointReader,
                                          MilvusSnapshotQueryClient snapshotQueryClient,
                                          MilvusMetadataClient metadataClient,
                                          MilvusStreamingChangeEventSourceMetrics streamingMetrics,
                                          MilvusOffsetContext previousOffset) {
        this(connectorConfig, dispatcher, schema, checkpointReader, snapshotQueryClient, metadataClient,
                streamingMetrics, previousOffset, () -> new MilvusWireFormatDetector(connectorConfig));
    }

    MilvusChangeEventSourceFactory(MilvusConnectorConfig connectorConfig,
                                   EventDispatcher<MilvusPartition, TableId> dispatcher,
                                   MilvusDatabaseSchema schema,
                                   EtcdCheckpointReader checkpointReader,
                                   MilvusSnapshotQueryClient snapshotQueryClient,
                                   MilvusMetadataClient metadataClient,
                                   MilvusStreamingChangeEventSourceMetrics streamingMetrics,
                                   MilvusOffsetContext previousOffset,
                                   Supplier<MilvusWireFormatDetector> detectorSupplier) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.schema = schema;
        this.checkpointReader = checkpointReader;
        this.snapshotQueryClient = snapshotQueryClient;
        this.metadataClient = metadataClient;
        this.streamingMetrics = streamingMetrics;
        this.previousOffset = previousOffset;
        this.detectorSupplier = detectorSupplier;
    }

    @Override
    public MilvusSnapshotChangeEventSource getSnapshotChangeEventSource(
                                                                        SnapshotProgressListener<MilvusPartition> snapshotProgressListener,
                                                                        NotificationService<MilvusPartition, MilvusOffsetContext> notificationService) {
        return new MilvusSnapshotChangeEventSource(
                connectorConfig,
                snapshotProgressListener,
                notificationService,
                checkpointReader,
                snapshotQueryClient,
                metadataClient,
                dispatcher,
                schema);
    }

    @Override
    public StreamingChangeEventSource<MilvusPartition, MilvusOffsetContext> getStreamingChangeEventSource() {
        MilvusMessageConsumer messageConsumer = new KafkaMilvusMessageConsumer(connectorConfig);
        MilvusValueConverter valueConverter = new MilvusValueConverter(connectorConfig);
        MilvusColumnarPivot pivot = new MilvusColumnarPivot(valueConverter);
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(resolveWireFormat(), pivot);
        TimetickOrderingEngine orderingEngine = new TimetickOrderingEngine(connectorConfig);
        return new MilvusStreamingChangeEventSource(
                connectorConfig, messageConsumer, deserializer, orderingEngine,
                dispatcher, schema, checkpointReader, streamingMetrics);
    }

    /**
     * Returns the wire format the deserializer must use.
     *
     * <p>An explicit {@code milvus.wire.format} value wins without any probing.
     * With {@code auto}, the configured pchannel is probed by
     * {@link MilvusWireFormatDetector}; the probe starts from the stored MQ
     * offset when one exists (warm restart) and from the earliest available
     * message otherwise. The result is cached for the lifetime of the factory.</p>
     */
    synchronized String resolveWireFormat() {
        if (resolvedWireFormat != null) {
            return resolvedWireFormat;
        }

        String configured = connectorConfig.getWireFormat();
        String normalized = configured == null ? WIRE_FORMAT_AUTO : configured.trim().toLowerCase();
        if (!WIRE_FORMAT_AUTO.equals(normalized)) {
            LOGGER.info("Using explicitly configured Milvus wire format '{}'", normalized);
            resolvedWireFormat = normalized;
            return resolvedWireFormat;
        }

        String pchannel = connectorConfig.getPchannelName();
        Map<TopicPartition, Long> storedOffsets = storedMqOffsets(pchannel);
        LOGGER.info("Probing pchannel '{}' to detect the Milvus wire format, starting from {}",
                pchannel, storedOffsets.isEmpty() ? "the earliest available message" : "stored offsets " + storedOffsets);
        resolvedWireFormat = detectorSupplier.get().detect(Set.of(pchannel), storedOffsets);
        LOGGER.info("Detected Milvus wire format '{}' on pchannel '{}'", resolvedWireFormat, pchannel);
        return resolvedWireFormat;
    }

    private Map<TopicPartition, Long> storedMqOffsets(String pchannel) {
        if (previousOffset == null) {
            return Map.of();
        }
        Long offset = previousOffset.getMqOffset(pchannel);
        if (offset == null) {
            return Map.of();
        }
        return Map.of(new TopicPartition(pchannel, connectorConfig.getKafkaPartitionIndex()), offset);
    }
}
