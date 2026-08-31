/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.debezium.config.Configuration;
import io.debezium.connector.milvus.util.MilvusTestContainer;
import io.debezium.connector.milvus.util.TestHelper;
import io.debezium.data.Json;
import io.debezium.data.vector.FloatVector;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.embedded.util.MetricsHelper;
import io.debezium.junit.logging.LogInterceptor;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;

/**
 * End-to-end integration tests for the Milvus connector using the Debezium
 * Embedded Engine framework.
 *
 * <p>
 * Each test:
 * <ol>
 *   <li>Starts the connector via {@link #start(Class, Configuration)}</li>
 *   <li>Waits for streaming to be running via
 *       {@link #waitForStreamingRunning(String, String)}</li>
 *   <li>Performs Milvus DML (insert / delete)</li>
 *   <li>Consumes records via {@link #consumeRecordsByTopic(int)}</li>
 *   <li>Asserts record contents</li>
 * </ol>
 * {@code stopConnector()} is called automatically by the base class
 * {@code @AfterEach}.
 * </p>
 */
@Tag("integration")
class MilvusStreamingPipelineIT extends AbstractAsyncEngineConnectorTest {

    private static final String PCHANNEL = "by-dev-rootcoord-dml_0";
    private static final int DIM = 4;
    private static final Gson GSON = new Gson();

    private static MilvusClientV2 milvusClient;
    private static String bootstrap;

    private String collectionName;

    @BeforeAll
    static void startInfrastructure() {
        MilvusTestContainer.startAll();
        bootstrap = MilvusTestContainer.kafkaBootstrapServers();

        Awaitility.await("Milvus MQ ready")
                .atMost(Duration.ofMinutes(5))
                .pollInterval(Duration.ofSeconds(5))
                .until(() -> {
                    try {
                        if (milvusClient == null) {
                            milvusClient = TestHelper.createMilvusClient();
                        }
                        String sentinel = "_dbz_ready_probe_";
                        CreateCollectionReq.FieldSchema idField = CreateCollectionReq.FieldSchema.builder()
                                .name("id").dataType(DataType.Int64)
                                .isPrimaryKey(true).autoID(false).build();
                        CreateCollectionReq.FieldSchema vecField = CreateCollectionReq.FieldSchema.builder()
                                .name("vec").dataType(DataType.FloatVector)
                                .dimension(2).build();
                        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                                .fieldSchemaList(Arrays.asList(idField, vecField)).build();
                        milvusClient.createCollection(CreateCollectionReq.builder()
                                .collectionName(sentinel).collectionSchema(schema).build());
                        milvusClient.dropCollection(
                                DropCollectionReq.builder()
                                        .collectionName(sentinel).build());
                        return true;
                    }
                    catch (Exception e) {
                        if (milvusClient != null) {
                            try {
                                milvusClient.close(1);
                            }
                            catch (Exception ignored) {
                            }
                            milvusClient = null;
                        }
                        return false;
                    }
                });

        TestHelper.ensureTopic(bootstrap, PCHANNEL);
    }

    @AfterAll
    static void stopInfrastructure() {
        try {
            milvusClient.close(10);
        }
        catch (Exception ignored) {
        }
        MilvusTestContainer.stopAll();
    }

    @BeforeEach
    void createCollection() {
        collectionName = "test_" + UUID.randomUUID().toString().substring(0, 8);
        setConsumeTimeout(20, TimeUnit.SECONDS);
    }

    @AfterEach
    void dropCollection() {
        try {
            milvusClient.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName).build());
        }
        catch (Exception ignored) {
        }
    }

    /**
     * Wait until the streaming source's Kafka consumer has resolved and
     * assigned its starting position (e.g. seeked to LATEST).
     *
     * <p>Guards on the {@code PositionResolved} JMX attribute exposed by
     * {@link MilvusStreamingChangeEventSourceMetrics} rather than a fixed
     * sleep, so the test isn't brittle under slow CI environments: a DML
     * issued before the consumer's position is resolved could be missed
     * entirely (if seeking to LATEST lands after the write) rather than
     * merely delayed.</p>
     */
    private void waitForConsumerPositionResolved() {
        Awaitility.await("consumer position resolved")
                .pollInterval(Duration.ofMillis(100))
                .atMost(Duration.ofSeconds(30))
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> Boolean.TRUE.equals(
                        MetricsHelper.<Boolean> getStreamingMetric(
                                "milvus", TestHelper.TOPIC_PREFIX, "streaming", "PositionResolved")));
    }

    private Configuration connectorConfig() {
        return TestHelper.defaultConfig(bootstrap).edit()
                .with(MilvusConnectorConfig.PCHANNEL_NAME, PCHANNEL)
                .with(MilvusConnectorConfig.WIRE_FORMAT, MilvusProtoDeserializer.FORMAT_PROTO_SINGLE)
                .with(MilvusConnectorConfig.TIMETICK_STALL_TIMEOUT_MS, 5_000L)
                .build();
    }

    /**
     * Same as {@link #connectorConfig()} but with {@code milvus.wire.format}
     * at its {@code auto} default, so the connector must probe the pchannel
     * to discover the format instead of trusting the test's knowledge of it.
     */
    private Configuration autoDetectConfig() {
        return connectorConfig().edit()
                .with(MilvusConnectorConfig.WIRE_FORMAT, MilvusChangeEventSourceFactory.WIRE_FORMAT_AUTO)
                .build();
    }

    private void createSimpleCollection(String name) {
        CreateCollectionReq.FieldSchema idField = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build();
        CreateCollectionReq.FieldSchema titleField = CreateCollectionReq.FieldSchema.builder()
                .name("title")
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build();
        CreateCollectionReq.FieldSchema vecField = CreateCollectionReq.FieldSchema.builder()
                .name("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build();
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(idField, titleField, vecField))
                .build();
        milvusClient.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .build());
        TestHelper.loadCollection(milvusClient, name, "vector");
    }

    private void createAllFieldTypesCollection(String name) {
        CreateCollectionReq.FieldSchema idField = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build();
        CreateCollectionReq.FieldSchema countField = CreateCollectionReq.FieldSchema.builder()
                .name("count")
                .dataType(DataType.Int32)
                .build();
        CreateCollectionReq.FieldSchema titleField = CreateCollectionReq.FieldSchema.builder()
                .name("title")
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build();
        CreateCollectionReq.FieldSchema priceField = CreateCollectionReq.FieldSchema.builder()
                .name("price")
                .dataType(DataType.Float)
                .build();
        CreateCollectionReq.FieldSchema scoreField = CreateCollectionReq.FieldSchema.builder()
                .name("score")
                .dataType(DataType.Double)
                .build();
        CreateCollectionReq.FieldSchema activeField = CreateCollectionReq.FieldSchema.builder()
                .name("active")
                .dataType(DataType.Bool)
                .build();
        CreateCollectionReq.FieldSchema metaField = CreateCollectionReq.FieldSchema.builder()
                .name("meta")
                .dataType(DataType.JSON)
                .build();
        CreateCollectionReq.FieldSchema embeddingField = CreateCollectionReq.FieldSchema.builder()
                .name("embedding")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build();
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(idField, countField, titleField, priceField,
                        scoreField, activeField, metaField, embeddingField))
                .build();
        milvusClient.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .build());
        TestHelper.loadCollection(milvusClient, name, "embedding");
    }

    private JsonObject rowWithIdAndTitle(long id, String title) {
        JsonObject row = new JsonObject();
        row.addProperty("id", id);
        row.addProperty("title", title);
        List<Float> vec = Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f);
        row.add("vector", GSON.toJsonTree(vec));
        return row;
    }

    private JsonObject allFieldTypesRow() {
        JsonObject row = new JsonObject();
        row.addProperty("id", 42L);
        row.addProperty("count", 7);
        row.addProperty("title", "hello");
        row.addProperty("price", 1.5f);
        row.addProperty("score", 99.9d);
        row.addProperty("active", true);
        JsonObject meta = new JsonObject();
        meta.addProperty("k", 1);
        row.add("meta", meta);
        List<Float> vec = Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f);
        row.add("embedding", GSON.toJsonTree(vec));
        return row;
    }

    @Test
    void shouldCaptureInsert() throws Exception {
        createSimpleCollection(collectionName);
        start(MilvusConnector.class, connectorConfig());
        waitForStreamingRunning("milvus", TestHelper.TOPIC_PREFIX);

        milvusClient.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(rowWithIdAndTitle(42L, "hello")))
                .build());

        // The connector emits one Debezium record for the inserted row.
        String expectedTopic = TestHelper.TOPIC_PREFIX + "." + MilvusConnectorConfig.MILVUS_DATABASE.defaultValueAsString()
                + "." + collectionName;
        var records = consumeRecordsByTopic(1);
        List<SourceRecord> topicRecords = records.recordsForTopic(expectedTopic);
        assertThat(topicRecords).hasSize(1);

        SourceRecord record = topicRecords.get(0);
        assertThat(record.topic()).startsWith(TestHelper.TOPIC_PREFIX);

        // Key
        Schema keySchema = record.keySchema();
        assertThat(keySchema).isNotNull();
        assertThat(keySchema.type()).isEqualTo(Schema.Type.STRUCT);
        Struct key = (Struct) record.key();
        assertThat(key).isNotNull();
        assertThat(key.get("id")).isEqualTo(42L);

        // Value / envelope
        Struct value = (Struct) record.value();
        assertThat(value).isNotNull();
        assertThat(value.getString("op")).isEqualTo("c");
        assertThat(value.get("before")).isNull();

        Struct after = value.getStruct("after");
        assertThat(after).isNotNull();
        assertThat(after.get("id")).isEqualTo(42L);
        assertThat(after.get("title")).isEqualTo("hello");

        // Source block
        Struct source = value.getStruct("source");
        assertThat(source).isNotNull();
        assertThat(source.getString("connector")).isEqualTo("milvus");
        assertThat(source.getString("collection")).isEqualTo(collectionName);
        assertThat(source.getString("db")).isEqualTo("default");
        assertThat(source.getInt64("tso")).isPositive();
    }

    @Test
    void shouldCaptureInsertAndDelete() throws Exception {
        createSimpleCollection(collectionName);
        start(MilvusConnector.class, connectorConfig());
        waitForStreamingRunning("milvus", TestHelper.TOPIC_PREFIX);
        waitForConsumerPositionResolved();

        long entityId = System.nanoTime();

        milvusClient.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(rowWithIdAndTitle(entityId, "hello")))
                .build());

        milvusClient.delete(DeleteReq.builder()
                .collectionName(collectionName)
                .ids(Collections.singletonList(entityId))
                .build());

        String expectedTopic = TestHelper.TOPIC_PREFIX + "." + MilvusConnectorConfig.MILVUS_DATABASE.defaultValueAsString()
                + "." + collectionName;

        var records = consumeRecordsByTopic(2);
        List<SourceRecord> topicRecords = records.recordsForTopic(expectedTopic);
        assertThat(topicRecords).as("Expected insert + delete on topic " + expectedTopic)
                .hasSize(2);

        SourceRecord insertRecord = topicRecords.get(0);
        Struct insertValue = (Struct) insertRecord.value();
        assertThat(insertValue.getString("op")).isEqualTo("c");
        Struct insertAfter = insertValue.getStruct("after");
        assertThat(insertAfter.get("id")).isEqualTo(entityId);
        assertThat(insertAfter.get("title")).isEqualTo("hello");
        Struct insertSource = insertValue.getStruct("source");
        assertThat(insertSource.getString("collection")).isEqualTo(collectionName);
        assertThat(insertSource.getString("db")).isEqualTo("default");
        assertThat(insertSource.getInt64("tso")).isPositive();

        SourceRecord deleteRecord = topicRecords.get(1);
        Struct deleteValue = (Struct) deleteRecord.value();
        assertThat(deleteValue.getString("op")).isEqualTo("d");
        assertThat(deleteValue.get("after")).isNull();

        Struct deleteBefore = deleteValue.getStruct("before");
        assertThat(deleteBefore).isNotNull();
        assertThat(deleteBefore.get("id")).isEqualTo(entityId);
    }

    @Test
    void shouldAutoDetectWireFormat() throws Exception {
        LogInterceptor factoryLogs = new LogInterceptor(MilvusChangeEventSourceFactory.class);
        LogInterceptor detectorLogs = new LogInterceptor(MilvusWireFormatDetector.class);

        createSimpleCollection(collectionName);
        start(MilvusConnector.class, autoDetectConfig());
        waitForStreamingRunning("milvus", TestHelper.TOPIC_PREFIX);
        waitForConsumerPositionResolved();

        long entityId = System.nanoTime();
        milvusClient.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(rowWithIdAndTitle(entityId, "auto")))
                .build());

        String expectedTopic = TestHelper.TOPIC_PREFIX + "." + MilvusConnectorConfig.MILVUS_DATABASE.defaultValueAsString()
                + "." + collectionName;
        var records = consumeRecordsByTopic(1);
        List<SourceRecord> topicRecords = records.recordsForTopic(expectedTopic);
        assertThat(topicRecords).as("Expected insert on topic " + expectedTopic).hasSize(1);

        Struct value = (Struct) topicRecords.get(0).value();
        assertThat(value.getString("op")).isEqualTo("c");
        assertThat(value.getStruct("after").get("id")).isEqualTo(entityId);
        assertThat(value.getStruct("source").getString("collection")).isEqualTo(collectionName);

        assertThat(factoryLogs.containsMessage("starting from the earliest available message")).isTrue();
        assertThat(factoryLogs.containsMessage("Detected Milvus wire format 'proto_single'")).isTrue();
        assertThat(factoryLogs.containsMessage("Using explicitly configured Milvus wire format")).isFalse();
        assertThat(detectorLogs.containsMessage("defaulting wire format to")).isFalse();
    }

    @Test
    void shouldAutoDetectWireFormatOnRestart() throws Exception {
        LogInterceptor factoryLogs = new LogInterceptor(MilvusChangeEventSourceFactory.class);
        LogInterceptor detectorLogs = new LogInterceptor(MilvusWireFormatDetector.class);

        createSimpleCollection(collectionName);
        start(MilvusConnector.class, autoDetectConfig());
        waitForStreamingRunning("milvus", TestHelper.TOPIC_PREFIX);
        waitForConsumerPositionResolved();

        String expectedTopic = TestHelper.TOPIC_PREFIX + "." + MilvusConnectorConfig.MILVUS_DATABASE.defaultValueAsString()
                + "." + collectionName;

        long firstId = System.nanoTime();
        milvusClient.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(rowWithIdAndTitle(firstId, "before-restart")))
                .build());
        List<SourceRecord> firstRecords = consumeRecordsByTopic(1).recordsForTopic(expectedTopic);
        assertThat(firstRecords).hasSize(1);
        assertThat(((Struct) firstRecords.get(0).value()).getStruct("after").get("id")).isEqualTo(firstId);

        stopConnector();
        factoryLogs.clear();
        detectorLogs.clear();

        start(MilvusConnector.class, autoDetectConfig());
        waitForStreamingRunning("milvus", TestHelper.TOPIC_PREFIX);
        waitForConsumerPositionResolved();

        long secondId = firstId + 1;
        milvusClient.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(rowWithIdAndTitle(secondId, "after-restart")))
                .build());
        List<SourceRecord> secondRecords = consumeRecordsByTopic(1).recordsForTopic(expectedTopic);
        assertThat(secondRecords).as("Expected the post-restart insert on topic " + expectedTopic).hasSize(1);
        assertThat(((Struct) secondRecords.get(0).value()).getStruct("after").get("id")).isEqualTo(secondId);

        assertThat(factoryLogs.containsMessage("starting from stored offsets")).isTrue();
        assertThat(factoryLogs.containsMessage("Detected Milvus wire format 'proto_single'")).isTrue();
        assertThat(detectorLogs.containsMessage("defaulting wire format to")).isFalse();
    }

    @Test
    void shouldCaptureAllFieldTypes() throws Exception {
        createAllFieldTypesCollection(collectionName);
        start(MilvusConnector.class, connectorConfig());
        waitForStreamingRunning("milvus", TestHelper.TOPIC_PREFIX);

        milvusClient.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(allFieldTypesRow()))
                .build());

        String expectedTopic = TestHelper.TOPIC_PREFIX + "." + MilvusConnectorConfig.MILVUS_DATABASE.defaultValueAsString()
                + "." + collectionName;
        var records = consumeRecordsByTopic(1);
        List<SourceRecord> topicRecords = records.recordsForTopic(expectedTopic);
        assertThat(topicRecords).hasSize(1);

        SourceRecord record = topicRecords.get(0);
        Struct value = (Struct) record.value();
        assertThat(value.getString("op")).isEqualTo("c");

        Struct after = value.getStruct("after");
        assertThat(after).isNotNull();

        assertThat(after.get("id")).isEqualTo(42L);
        assertThat(after.get("count")).isEqualTo(7);
        assertThat(after.get("title")).isEqualTo("hello");
        assertThat(after.get("price")).isEqualTo(1.5f);
        assertThat(after.get("score")).isEqualTo(99.9d);
        assertThat(after.get("active")).isEqualTo(true);
        assertThat(after.get("meta")).isEqualTo("{\"k\":1}");

        Schema afterSchema = after.schema();
        assertThat(afterSchema.field("id").schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(afterSchema.field("count").schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat(afterSchema.field("title").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(afterSchema.field("price").schema().type()).isEqualTo(Schema.Type.FLOAT32);
        assertThat(afterSchema.field("score").schema().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(afterSchema.field("active").schema().type()).isEqualTo(Schema.Type.BOOLEAN);
        assertThat(afterSchema.field("meta").schema().name()).isEqualTo(Json.LOGICAL_NAME);
        assertThat(afterSchema.field("embedding").schema().name()).isEqualTo(FloatVector.LOGICAL_NAME);
        assertThat(after.get("embedding")).isInstanceOf(List.class);

        @SuppressWarnings("unchecked")
        List<Float> embedding = (List<Float>) after.get("embedding");
        assertThat(embedding).containsExactly(1.5f, 2.5f, 3.5f, 4.5f);
    }

}
