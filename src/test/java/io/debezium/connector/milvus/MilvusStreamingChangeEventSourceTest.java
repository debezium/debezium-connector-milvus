/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.milvus.checkpoint.ChannelCheckpoint;
import io.debezium.connector.milvus.checkpoint.EtcdCheckpointReader;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;

public class MilvusStreamingChangeEventSourceTest {

    private static final String TOPIC = "by-dev-rootcoord-dml_0";

    private MilvusConnectorConfig config;
    private MilvusMessageConsumer messageConsumer;
    private MilvusProtoDeserializer deserializer;
    private TimetickOrderingEngine orderingEngine;
    private ChangeEventSource.ChangeEventSourceContext context;
    private MilvusPartition partition;
    private MilvusOffsetContext offsetContext;
    private MilvusStreamingChangeEventSource source;
    private EtcdCheckpointReader checkpointReader;

    private EventDispatcher<MilvusPartition, TableId> dispatcher;
    private MilvusDatabaseSchema schema;
    private MilvusStreamingChangeEventSourceMetrics streamingMetrics;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        Configuration configuration = Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092",
                "poll.interval.ms", "100"));
        config = new MilvusConnectorConfig(configuration);
        messageConsumer = mock(MilvusMessageConsumer.class);
        deserializer = mock(MilvusProtoDeserializer.class);
        orderingEngine = new TimetickOrderingEngine(config);
        context = mock(ChangeEventSource.ChangeEventSourceContext.class);
        partition = MilvusPartition.create("milvus-test", TOPIC);
        offsetContext = new MilvusOffsetContext(new MilvusSourceInfo(config));
        checkpointReader = mock(EtcdCheckpointReader.class);

        dispatcher = mock(EventDispatcher.class);
        schema = mock(MilvusDatabaseSchema.class);

        when(schema.isCollectionRegistered(any(TableId.class))).thenReturn(false);
        when(schema.registerCollection(anyString(), anyString(), any(List.class))).thenReturn(true);
        when(schema.getColumnNames(any(TableId.class))).thenReturn(new String[]{ "id" });
        when(schema.getPkFieldName(any(TableId.class))).thenReturn("id");

        streamingMetrics = mock(MilvusStreamingChangeEventSourceMetrics.class);

        source = new MilvusStreamingChangeEventSource(
                config, messageConsumer, deserializer, orderingEngine,
                dispatcher, schema, checkpointReader, streamingMetrics);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldStopWhenContextNotRunning() throws InterruptedException {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        when(context.isRunning()).thenReturn(false);

        source.execute(context, partition, offsetContext);

        verify(messageConsumer, never()).poll(any(Duration.class));
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldMarkPositionResolvedAfterSeekAndClearOnStop() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        InOrder inOrder = inOrder(messageConsumer, streamingMetrics);
        inOrder.verify(messageConsumer).assignAndSeek(any(), any(), any());
        inOrder.verify(streamingMetrics).positionResolved(true);
        inOrder.verify(streamingMetrics).positionResolved(false);
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldClearPositionResolvedOnClose() {
        source.close();

        verify(streamingMetrics).positionResolved(false);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldPollAndTrackOffset() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        RawMilvusMessage msg = new RawMilvusMessage(TOPIC, 0, 1L, null, "payload".getBytes(), 0L);

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of(msg));
        when(deserializer.deserialize(any(RawMilvusMessage.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).poll(any(Duration.class));
        assertThat(offsetContext.getMqOffset(TOPIC)).isEqualTo(1L);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldHandleEmptyPoll() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).poll(any(Duration.class));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldHandlePauseAndResume() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        when(context.isRunning()).thenReturn(true, true, false);
        when(context.isPaused()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(context).waitStreamingPaused();
        verify(messageConsumer, times(1)).poll(any(Duration.class));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldBreakLoopOnInterrupt() throws Exception {
        when(context.isRunning()).thenReturn(true);
        when(messageConsumer.poll(any(Duration.class))).then(invocation -> {
            Thread.sleep(10);
            return List.of();
        });

        Thread testThread = new Thread(() -> {
            try {
                source.execute(context, partition, offsetContext);
            }
            catch (InterruptedException e) {
                // expecte
            }
        });
        testThread.start();
        Thread.sleep(30);
        testThread.interrupt();
        testThread.join(1000);

        assertThat(testThread.isAlive()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldResumeFromStoredOffset() throws Exception {
        offsetContext.setMqPosition(TOPIC, 0, 99L);
        offsetContext.postSnapshotCompletion();

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).assignAndSeek(
                Set.of(TOPIC),
                SeekPosition.STORED_OFFSET_PLUS_ONE,
                Map.of(new TopicPartition(TOPIC, 0), 99L));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldSeekToLatestWhenSnapshotCompletedAndNoStoredOffset() throws Exception {
        offsetContext.postSnapshotCompletion();

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).assignAndSeek(
                Set.of(TOPIC),
                SeekPosition.LATEST,
                null);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldFallbackToLatestForSnapshotHandoffWhenNoCheckpoint() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).assignAndSeek(
                Set.of(TOPIC),
                SeekPosition.LATEST,
                null);
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldUseCheckpointOffsetForSnapshotHandoff() throws Exception {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(8)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN);
        buf.putLong(99L);
        ChannelCheckpoint checkpoint = new ChannelCheckpoint(TOPIC, buf.array(), 440000000000L);
        when(checkpointReader.read(TOPIC)).thenReturn(Optional.of(checkpoint));

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).assignAndSeek(
                Set.of(TOPIC),
                SeekPosition.DEFAULT,
                Map.of(new TopicPartition(TOPIC, 0), 99L));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldDeserializeAndBufferEvents() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        RawMilvusMessage msg = new RawMilvusMessage(TOPIC, 0, 1L, null, "payload".getBytes(), 0L);

        MilvusRow insertRow = new MilvusRow(
                new String[]{ "id" },
                new Object[]{ 1L },
                new io.milvus.grpc.DataType[]{ io.milvus.grpc.DataType.Int64 });
        MilvusChangeEvent.Insert insert = new MilvusChangeEvent.Insert(
                "coll", TOPIC, TOPIC, 100, insertRow);
        MilvusChangeEvent.TimeTick tick = new MilvusChangeEvent.TimeTick(
                null, TOPIC, TOPIC, 200);

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of(msg));
        when(deserializer.deserialize(any(RawMilvusMessage.class)))
                .thenReturn(List.of(insert, tick));
        when(dispatcher.dispatchDataChangeEvent(any(), any(), any())).thenReturn(true);

        source.execute(context, partition, offsetContext);

        // The insert should have been buffered and the timetick should have updated
        // the watermark. Since watermark=200 >= TSO=100, the insert should have been
        // flushed.
        assertThat(orderingEngine.getBufferedEventCount()).isZero();
        assertThat(orderingEngine.getGlobalWatermark()).isEqualTo(200L);

        verify(dispatcher).dispatchDataChangeEvent(any(), any(), any());
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldDispatchInsertEventThroughPipeline() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        RawMilvusMessage msg = new RawMilvusMessage(TOPIC, 0, 1L, null, "payload".getBytes(), 0L);

        MilvusRow insertRow = new MilvusRow(
                new String[]{ "id", "name" },
                new Object[]{ 42L, "test-vector" },
                new io.milvus.grpc.DataType[]{ io.milvus.grpc.DataType.Int64, io.milvus.grpc.DataType.VarChar });
        MilvusChangeEvent.Insert insert = new MilvusChangeEvent.Insert(
                "test_collection", TOPIC, TOPIC, 100, insertRow);
        MilvusChangeEvent.TimeTick tick = new MilvusChangeEvent.TimeTick(
                null, TOPIC, TOPIC, 200);

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of(msg));
        when(deserializer.deserialize(any(RawMilvusMessage.class)))
                .thenReturn(List.of(insert, tick));
        when(dispatcher.dispatchDataChangeEvent(any(), any(), any())).thenReturn(true);

        when(schema.getColumnNames(any(TableId.class))).thenReturn(new String[]{ "id", "name" });
        when(schema.getPkFieldName(any(TableId.class))).thenReturn("id");

        source.execute(context, partition, offsetContext);

        List<FieldDefinition> expectedFields = List.of(
                new FieldDefinition("id", 42L, io.milvus.grpc.DataType.Int64),
                new FieldDefinition("name", "test-vector", io.milvus.grpc.DataType.VarChar));
        verify(schema).registerCollection("default", "test_collection", expectedFields);

        verify(dispatcher).dispatchDataChangeEvent(
                any(MilvusPartition.class),
                any(TableId.class),
                any(MilvusChangeRecordEmitter.class));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldSkipEventsWithNoCollectionName() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        RawMilvusMessage msg = new RawMilvusMessage(TOPIC, 0, 1L, null, "payload".getBytes(), 0L);

        MilvusRow insertRow = new MilvusRow(
                new String[]{ "id" },
                new Object[]{ 1L },
                new io.milvus.grpc.DataType[]{ io.milvus.grpc.DataType.Int64 });
        MilvusChangeEvent.Insert insert = new MilvusChangeEvent.Insert(
                null, TOPIC, TOPIC, 100, insertRow);
        MilvusChangeEvent.TimeTick tick = new MilvusChangeEvent.TimeTick(
                null, TOPIC, TOPIC, 200);

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of(msg));
        when(deserializer.deserialize(any(RawMilvusMessage.class)))
                .thenReturn(List.of(insert, tick));

        source.execute(context, partition, offsetContext);

        verify(dispatcher, never()).dispatchDataChangeEvent(any(), any(), any());
    }

    @Test
    @FixFor("debezium/dbz#2328")
    void shouldDispatchHeartbeatEventOnEachLoopIteration() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        when(context.isRunning()).thenReturn(true, true, true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(dispatcher, times(3)).dispatchHeartbeatEvent(partition, offsetContext);
    }

    @Test
    @FixFor("debezium/dbz#2328")
    void shouldDispatchHeartbeatEventEvenWhenMessagesArePolled() throws Exception {
        when(checkpointReader.read(anyString())).thenReturn(Optional.empty());
        RawMilvusMessage msg = new RawMilvusMessage(TOPIC, 0, 1L, null, "payload".getBytes(), 0L);

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of(msg));
        when(deserializer.deserialize(any(RawMilvusMessage.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(dispatcher, times(1)).dispatchHeartbeatEvent(partition, offsetContext);
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldFailHardOnPersistentEtcdErrorRatherThanSkipToLatest() throws Exception {
        when(checkpointReader.read(anyString())).thenThrow(new RuntimeException("etcd unavailable"));

        assertThatThrownBy(() -> source.execute(context, partition, offsetContext))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("cannot safely resume streaming without risking data loss");

        verify(messageConsumer, never()).assignAndSeek(any(), any(), any());
        verify(checkpointReader, times(3)).read(anyString());
    }
}
