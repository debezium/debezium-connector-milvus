/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

public class MilvusEventDispatcher extends EventDispatcher<MilvusPartition, TableId> {

    public MilvusEventDispatcher(MilvusConnectorConfig connectorConfig,
                                 TopicNamingStrategy<TableId> topicNamingStrategy,
                                 MilvusDatabaseSchema schema,
                                 ChangeEventQueue<DataChangeEvent> queue,
                                 DataCollectionFilters.DataCollectionFilter<TableId> filter,
                                 ChangeEventCreator changeEventCreator,
                                 EventMetadataProvider metadataProvider,
                                 ScheduledHeartbeat heartbeat,
                                 SchemaNameAdjuster schemaNameAdjuster,
                                 DebeziumHeaderProducer headerProducer) {
        super(connectorConfig, topicNamingStrategy, schema, queue, filter,
                changeEventCreator, metadataProvider, heartbeat, schemaNameAdjuster,
                headerProducer);
    }
}
