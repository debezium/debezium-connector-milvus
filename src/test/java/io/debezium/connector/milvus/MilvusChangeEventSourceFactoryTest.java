/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.milvus.grpc.MsgBase;
import io.milvus.grpc.MsgType;

import milvus.proto.msg.Msg.CreateCollectionRequest;

/**
 * Verifies that {@link MilvusChangeEventSourceFactory} resolves the MQ wire
 * format through {@link MilvusWireFormatDetector} when configured as
 * {@code auto}, and bypasses the probe for an explicit format.
 */
public class MilvusChangeEventSourceFactoryTest {

    private static final String TOPIC = "by-dev-rootcoord-dml_0";

    @Test
    void shouldUseExplicitWireFormatWithoutProbing() {
        AtomicInteger detectorCreations = new AtomicInteger();
        MilvusChangeEventSourceFactory factory = factory("proto_single", null, () -> {
            detectorCreations.incrementAndGet();
            throw new AssertionError("detector must not be created for an explicit wire format");
        });

        assertThat(factory.resolveWireFormat()).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
        assertThat(detectorCreations).hasValue(0);
    }

    @Test
    void shouldProbeFromEarliestWhenAutoAndNoStoredOffset() {
        RecordingConsumer consumer = new RecordingConsumer(List.of(message(protoCreateCollection(), 5L)));
        MilvusConnectorConfig config = config("auto");
        MilvusChangeEventSourceFactory factory = factory(config, null, detector(config, consumer));

        assertThat(factory.resolveWireFormat()).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
        assertThat(consumer.seekPosition).isEqualTo(SeekPosition.EARLIEST);
        assertThat(consumer.seekOffsets).isNull();
        assertThat(consumer.closed).isTrue();
    }

    @Test
    void shouldProbeFromStoredOffsetOnWarmRestart() {
        MilvusConnectorConfig config = config("auto");
        MilvusOffsetContext previousOffset = new MilvusOffsetContext(new MilvusSourceInfo(config));
        previousOffset.setMqPosition(TOPIC, 0, 42L);

        RecordingConsumer consumer = new RecordingConsumer(List.of(message(protoCreateCollection(), 43L)));
        MilvusChangeEventSourceFactory factory = factory(config, previousOffset, detector(config, consumer));

        assertThat(factory.resolveWireFormat()).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
        assertThat(consumer.seekPosition).isNull();
        assertThat(consumer.seekOffsets).containsExactly(Map.entry(new TopicPartition(TOPIC, 0), 42L));
    }

    @Test
    void shouldCacheResolvedWireFormat() {
        MilvusConnectorConfig config = config("auto");
        AtomicInteger detectorCreations = new AtomicInteger();
        MilvusChangeEventSourceFactory factory = factory(config, null, () -> {
            detectorCreations.incrementAndGet();
            return new MilvusWireFormatDetector(config,
                    () -> new RecordingConsumer(List.of(message(protoCreateCollection(), 1L))));
        });

        assertThat(factory.resolveWireFormat()).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
        assertThat(factory.resolveWireFormat()).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
        assertThat(detectorCreations).hasValue(1);
    }

    private static MilvusChangeEventSourceFactory factory(String wireFormat, MilvusOffsetContext previousOffset,
                                                          Supplier<MilvusWireFormatDetector> detectorSupplier) {
        return factory(config(wireFormat), previousOffset, detectorSupplier);
    }

    private static MilvusChangeEventSourceFactory factory(MilvusConnectorConfig config, MilvusOffsetContext previousOffset,
                                                          Supplier<MilvusWireFormatDetector> detectorSupplier) {
        return new MilvusChangeEventSourceFactory(config, null, null, null, null, null, null, previousOffset,
                detectorSupplier);
    }

    private static Supplier<MilvusWireFormatDetector> detector(MilvusConnectorConfig config, RecordingConsumer consumer) {
        return () -> new MilvusWireFormatDetector(config, () -> consumer);
    }

    private static MilvusConnectorConfig config(String wireFormat) {
        return new MilvusConnectorConfig(Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092",
                "milvus.pchannel.name", TOPIC,
                "milvus.wire.format", wireFormat)));
    }

    private static RawMilvusMessage message(byte[] payload, long offset) {
        return new RawMilvusMessage(TOPIC, 0, offset, null, payload, 0L);
    }

    private static byte[] protoCreateCollection() {
        return CreateCollectionRequest.newBuilder()
                .setBase(MsgBase.newBuilder().setMsgType(MsgType.CreateCollection).setTimestamp(1L).build())
                .setCollectionName("books")
                .build()
                .toByteArray();
    }

    /**
     * Records how the detector positioned the consumer and serves the given
     * messages exactly once.
     */
    private static final class RecordingConsumer implements MilvusMessageConsumer {
        private final List<RawMilvusMessage> messages;
        private SeekPosition seekPosition;
        private Map<TopicPartition, Long> seekOffsets;
        private boolean delivered;
        private boolean closed;

        private RecordingConsumer(List<RawMilvusMessage> messages) {
            this.messages = messages;
        }

        @Override
        public void assignAndSeek(Map<TopicPartition, Long> offsets) {
            this.seekOffsets = offsets;
        }

        @Override
        public void assignAndSeek(Set<String> pchannels, SeekPosition position, Map<TopicPartition, Long> storedOffsets) {
            this.seekPosition = position;
            this.seekOffsets = storedOffsets;
        }

        @Override
        public List<RawMilvusMessage> poll(Duration timeout) {
            if (delivered) {
                return List.of();
            }
            delivered = true;
            return messages;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
