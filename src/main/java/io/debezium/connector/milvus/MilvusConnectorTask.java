/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.milvus.checkpoint.EtcdCheckpointReader;
import io.debezium.connector.milvus.checkpoint.JetcdEtcdCheckpointReader;
import io.debezium.connector.milvus.metadata.MilvusMetadataClient;
import io.debezium.connector.milvus.metadata.MilvusServiceMetadataClient;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.monitor.OffsetActivityMonitorServiceProvider;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.LoggingContext;

/**
 * Source task for the Milvus connector.
 *
 * <p>
 * Wires the full Debezium pipeline: {@link ChangeEventQueue},
 * {@link EventDispatcher}, {@link ChangeEventSourceCoordinator}.
 * Raw messages are consumed from Kafka, deserialized, ordered by TSO,
 * and dispatched as Debezium {@link SourceRecord}s.
 * </p>
 */
public class MilvusConnectorTask extends BaseSourceTask<MilvusPartition, MilvusOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusConnectorTask.class);

    private volatile MilvusConnectorConfig connectorConfig;
    private volatile CdcSourceTaskContext<MilvusConnectorConfig> taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile MilvusErrorHandler errorHandler;
    private volatile MilvusMetadataClient metadataClient;
    private volatile EtcdCheckpointReader checkpointReader;
    private volatile MilvusSnapshotQueryClient snapshotQueryClient;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    public CdcSourceTaskContext<MilvusConnectorConfig> preStart(Configuration config) {
        this.connectorConfig = new MilvusConnectorConfig(config);
        this.taskContext = new CdcSourceTaskContext<>(config, connectorConfig, Collections.emptyMap());
        return this.taskContext;
    }

    @Override
    protected ChangeEventSourceCoordinator<MilvusPartition, MilvusOffsetContext> start(Configuration config) {
        LOGGER.info("Starting Milvus connector task — wiring EventDispatcher pipeline");

        this.metadataClient = new MilvusServiceMetadataClient(connectorConfig);
        this.checkpointReader = new JetcdEtcdCheckpointReader(connectorConfig);
        this.snapshotQueryClient = new MilvusSnapshotQueryClient(connectorConfig);
        MilvusDatabaseSchema schema = MilvusDatabaseSchema.create(connectorConfig, taskContext, metadataClient);

        MilvusSourceInfo sourceInfo = new MilvusSourceInfo(connectorConfig);
        MilvusOffsetContext.Loader offsetLoader = new MilvusOffsetContext.Loader(sourceInfo);

        String pchannel = connectorConfig.getPchannelName();
        Partition.Provider<MilvusPartition> partitionProvider = new MilvusPartition.Provider(connectorConfig,
                List.of(pchannel));

        Offsets<MilvusPartition, MilvusOffsetContext> previousOffsets = getPreviousOffsets(partitionProvider,
                offsetLoader);

        registerServiceProviders(connectorConfig.getServiceRegistry());

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.ofMillis(connectorConfig.getPollIntervalMs()))
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> LoggingContext.forConnector(
                        Module.name(), connectorConfig.getLogicalName(), "streaming"))
                .build();

        this.errorHandler = new MilvusErrorHandler(connectorConfig, queue, null);

        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig
                .getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);

        MilvusEventMetadataProvider metadataProvider = new MilvusEventMetadataProvider();

        // Milvus has no JDBC connection, so no HeartbeatConnectionProvider/HeartbeatErrorHandler
        // is supplied here; heartbeat.action.query is rejected outright by MilvusConnectorConfig
        // validation, so the action-query path in HeartbeatFactory is never exercised.
        ScheduledHeartbeat heartbeat = new HeartbeatFactory<TableId>()
                .getScheduledHeartbeat(connectorConfig, null, null, queue);

        DebeziumHeaderProducer headerProducer = new DebeziumHeaderProducer(taskContext);
        MilvusEventDispatcher dispatcher = new MilvusEventDispatcher(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                heartbeat,
                connectorConfig.schemaNameAdjuster(),
                headerProducer);

        MilvusStreamingChangeEventSourceMetrics streamingMetrics = new MilvusStreamingChangeEventSourceMetrics(
                taskContext, queue, metadataProvider, schema::dataCollectionIds);

        MilvusChangeEventSourceFactory factory = new MilvusChangeEventSourceFactory(
                connectorConfig, dispatcher, schema, checkpointReader, snapshotQueryClient, metadataClient,
                streamingMetrics);

        SnapshotterService snapshotterService = MilvusSnapshotter.createService();

        NotificationService<MilvusPartition, MilvusOffsetContext> notificationService = new NotificationService<>(Collections.emptyList(), connectorConfig,
                SchemaFactory.get(), dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<MilvusPartition, MilvusOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                MilvusConnector.class,
                connectorConfig,
                factory,
                new MilvusChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema,
                null,
                notificationService,
                snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        LOGGER.info("Milvus connector task started successfully — pipeline active");
        return coordinator;
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> events = queue.poll();
        return events.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected void doStop() {
        LOGGER.info("Stopping Milvus connector task");
        if (snapshotQueryClient != null) {
            try {
                snapshotQueryClient.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing Milvus snapshot query client", e);
            }
            snapshotQueryClient = null;
        }
        if (checkpointReader != null) {
            try {
                checkpointReader.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing etcd checkpoint reader", e);
            }
            checkpointReader = null;
        }
        if (metadataClient != null) {
            try {
                metadataClient.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing Milvus metadata client", e);
            }
            metadataClient = null;
        }
        LOGGER.info("Milvus connector task stopped");
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MilvusConnectorConfig.ALL_FIELDS;
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void registerServiceProviders(ServiceRegistry serviceRegistry) {
        // todo: remove and use super method once Milvus supports all service providers
        serviceRegistry.registerServiceProvider(new OffsetActivityMonitorServiceProvider());
    }
}
