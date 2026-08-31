/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.milvus.grpc.FloatArray;
import io.milvus.grpc.MsgBase;
import io.milvus.grpc.MsgType;
import io.milvus.grpc.VectorField;

import milvus.proto.msg.Msg.CreateCollectionRequest;
import milvus.proto.msg.Msg.DeleteRequest;
import milvus.proto.msg.Msg.DropCollectionRequest;
import milvus.proto.msg.Msg.InsertRequest;
import milvus.proto.msg.Msg.TimeTickMsg;

public class MilvusWireFormatDetectorTest {

    private static final String TOPIC = "by-dev-rootcoord-dml_0";

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldDetectMsgPackBatchFromValidInsertRequest() throws Exception {
        MilvusWireFormatDetector detector = detector("auto", List.of(message(msgpackInsertBatch(), 1L)));

        assertThat(detector.detect(Set.of(TOPIC))).isEqualTo(MilvusProtoDeserializer.FORMAT_MSGPACK_BATCH);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldDetectProtoSingleFromValidInsertRequest() throws Exception {
        InsertRequest insert = InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, 11L))
                .setCollectionName("books")
                .addFieldsData(MilvusProtoDeserializerTestSupport.floatVectorField("embedding", 2, 1.0f, 2.0f))
                .setNumRows(1)
                .build();

        MilvusWireFormatDetector detector = detector("auto", List.of(message(insert.toByteArray(), 1L)));

        assertThat(detector.detect(Set.of(TOPIC))).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldDetectMsgPackBatchFromDeleteRequest() throws Exception {
        MilvusWireFormatDetector detector = detector("auto", List.of(message(msgpackDeleteBatch(), 2L)));

        assertThat(detector.detect(Set.of(TOPIC))).isEqualTo(MilvusProtoDeserializer.FORMAT_MSGPACK_BATCH);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldDetectProtoSingleFromDeleteRequest() throws Exception {
        DeleteRequest delete = DeleteRequest.newBuilder()
                .setBase(base(MsgType.Delete, 12L))
                .setCollectionName("books")
                .addInt64PrimaryKeys(1L)
                .build();

        MilvusWireFormatDetector detector = detector("auto", List.of(message(delete.toByteArray(), 2L)));

        assertThat(detector.detect(Set.of(TOPIC))).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldSkipTimeTicksAndDetectSubsequentDataMessage() throws Exception {
        MilvusWireFormatDetector detector = detector("auto", List.of(
                message(msgpackTimeTickBatch(), 1L),
                message(protoTimeTick().toByteArray(), 2L),
                message(protoCreate().toByteArray(), 3L)));

        assertThat(detector.detect(Set.of(TOPIC))).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldFallbackToConfiguredFormatOnEmptyTopic() {
        MilvusWireFormatDetector detector = detector("proto_single", List.of());

        assertThat(detector.detect(Set.of(TOPIC))).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldDefaultToMsgPackBatchWhenAutoAndEmptyTopic() {
        MilvusWireFormatDetector detector = detector("auto", List.of());

        assertThat(detector.detect(Set.of(TOPIC))).isEqualTo(MilvusProtoDeserializer.FORMAT_MSGPACK_BATCH);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldThrowOnMismatchWhenExplicitFormatDoesNotMatch() throws Exception {
        MilvusWireFormatDetector detector = detector("proto_single", List.of(message(msgpackInsertBatch(), 1L)));

        assertThatThrownBy(() -> detector.detect(Set.of(TOPIC)))
                .isInstanceOf(MilvusWireFormatMismatchException.class)
                .hasMessageContaining("expected=proto_single")
                .hasMessageContaining("detected=msgpack_batch");
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldThrowOnUnrecognizablePayload() {
        MilvusWireFormatDetector detector = detector("auto", List.of(message(new byte[]{ 0x55, 0x66 }, 1L)));

        assertThatThrownBy(() -> detector.detect(Set.of(TOPIC)))
                .isInstanceOf(MilvusWireFormatMismatchException.class)
                .hasMessageContaining("Unrecognizable payload");
    }

    @Test
    void shouldSkipUnrecognizablePayloadWhenRecognizableMessageFollows() {
        MilvusWireFormatDetector detector = detector("auto", List.of(
                message(new byte[0], 0L),
                message(new byte[]{ 0x55, 0x66 }, 1L),
                message(protoCreate().toByteArray(), 2L)));

        assertThat(detector.detect(Set.of(TOPIC))).isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldRejectMixedFormatsAcrossChannels() throws Exception {
        FakeMilvusMessageConsumer consumer = new FakeMilvusMessageConsumer(Map.of(
                TOPIC, List.of(message(msgpackInsertBatch(), 1L)),
                "by-dev-rootcoord-dml_1", List.of(message(protoDrop().toByteArray(), 1L))));
        MilvusWireFormatDetector detector = detector("auto", consumer);

        assertThatThrownBy(() -> detector.detect(Set.of(TOPIC, "by-dev-rootcoord-dml_1")))
                .isInstanceOf(MilvusWireFormatMismatchException.class)
                .hasMessageContaining("Mixed wire formats detected");
    }

    @Test
    void shouldReprobeFromEarliestWhenStoredOffsetYieldsOnlyTimeTicks() {
        FakeMilvusMessageConsumer consumer = new FakeMilvusMessageConsumer(
                Map.of(TOPIC, List.of(message(protoCreate().toByteArray(), 1L))),
                Map.of(TOPIC, List.of(message(protoTimeTick().toByteArray(), 42L))));
        MilvusWireFormatDetector detector = detector("auto", consumer);

        assertThat(detector.detect(Set.of(TOPIC), Map.of(new TopicPartition(TOPIC, 0), 42L)))
                .isEqualTo(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE);
        assertThat(consumer.storedOffsetProbes).isEqualTo(1);
        assertThat(consumer.earliestProbes).isEqualTo(1);
    }

    private static MilvusWireFormatDetector detector(String configuredWireFormat,
                                                     List<RawMilvusMessage> polledMessages) {
        return detector(configuredWireFormat, new FakeMilvusMessageConsumer(Map.of(TOPIC, polledMessages)));
    }

    private static MilvusWireFormatDetector detector(String configuredWireFormat, FakeMilvusMessageConsumer consumer) {
        MilvusConnectorConfig config = new MilvusConnectorConfig(Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092",
                "milvus.wire.format", configuredWireFormat)));
        return new MilvusWireFormatDetector(config, () -> consumer);
    }

    private static RawMilvusMessage message(byte[] payload, long offset) {
        return new RawMilvusMessage(TOPIC, 0, offset, null, payload, 0L);
    }

    private static MsgBase base(MsgType type, long ts) {
        return MsgBase.newBuilder()
                .setMsgType(type)
                .setTimestamp(ts)
                .build();
    }

    private static CreateCollectionRequest protoCreate() {
        return CreateCollectionRequest.newBuilder()
                .setBase(base(MsgType.CreateCollection, 1L))
                .setCollectionName("books")
                .build();
    }

    private static DropCollectionRequest protoDrop() {
        return DropCollectionRequest.newBuilder()
                .setBase(base(MsgType.DropCollection, 2L))
                .setCollectionName("books")
                .build();
    }

    private static TimeTickMsg protoTimeTick() {
        return TimeTickMsg.newBuilder()
                .setBase(base(MsgType.TimeTick, 3L))
                .build();
    }

    private static byte[] msgpackInsertBatch() throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packArrayHeader(3);
        packer.packInt(MsgType.Insert.getNumber());
        packer.packString(TOPIC);
        packer.packArrayHeader(1);
        packer.packMapHeader(6);
        packer.packString("msgType");
        packer.packInt(MsgType.Insert.getNumber());
        packer.packString("collectionName");
        packer.packString("books");
        packer.packString("vchannel");
        packer.packString("books_v0");
        packer.packString("ts");
        packer.packLong(1L);
        packer.packString("numRows");
        packer.packInt(1);
        packer.packString("fieldsData");
        packer.packArrayHeader(0);
        packer.close();
        return packer.toByteArray();
    }

    private static byte[] msgpackDeleteBatch() throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packArrayHeader(3);
        packer.packInt(MsgType.Delete.getNumber());
        packer.packString(TOPIC);
        packer.packArrayHeader(1);
        packer.packMapHeader(5);
        packer.packString("msgType");
        packer.packInt(MsgType.Delete.getNumber());
        packer.packString("collectionName");
        packer.packString("books");
        packer.packString("vchannel");
        packer.packString("books_v0");
        packer.packString("ts");
        packer.packLong(2L);
        packer.packString("primaryKeys");
        packer.packArrayHeader(1);
        packer.packLong(1L);
        packer.close();
        return packer.toByteArray();
    }

    private static byte[] msgpackTimeTickBatch() throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packArrayHeader(3);
        packer.packInt(MsgType.TimeTick.getNumber());
        packer.packString(TOPIC);
        packer.packArrayHeader(1);
        packer.packMapHeader(2);
        packer.packString("msgType");
        packer.packInt(MsgType.TimeTick.getNumber());
        packer.packString("ts");
        packer.packLong(10L);
        packer.close();
        return packer.toByteArray();
    }

    private static final class FakeMilvusMessageConsumer implements MilvusMessageConsumer {
        private final Map<String, List<RawMilvusMessage>> messagesByTopic;
        private final Map<String, List<RawMilvusMessage>> messagesAfterStoredOffset;
        private String currentPchannel;
        private boolean delivered;
        private boolean seekedToStoredOffset;
        private int storedOffsetProbes;
        private int earliestProbes;

        private FakeMilvusMessageConsumer(Map<String, List<RawMilvusMessage>> messagesByTopic) {
            this(messagesByTopic, Map.of());
        }

        /**
         * @param messagesByTopic          messages served after a strategy-based seek (EARLIEST)
         * @param messagesAfterStoredOffset messages served after a stored-offset seek
         */
        private FakeMilvusMessageConsumer(Map<String, List<RawMilvusMessage>> messagesByTopic,
                                          Map<String, List<RawMilvusMessage>> messagesAfterStoredOffset) {
            this.messagesByTopic = messagesByTopic;
            this.messagesAfterStoredOffset = messagesAfterStoredOffset;
        }

        @Override
        public void assignAndSeek(Map<TopicPartition, Long> offsets) {
            this.currentPchannel = offsets.keySet().iterator().next().topic();
            this.seekedToStoredOffset = true;
            this.delivered = false;
            this.storedOffsetProbes++;
        }

        @Override
        public void assignAndSeek(Set<String> pchannels, SeekPosition position,
                                  Map<TopicPartition, Long> storedOffsets) {
            this.currentPchannel = pchannels.iterator().next();
            this.seekedToStoredOffset = false;
            this.delivered = false;
            this.earliestProbes++;
        }

        @Override
        public List<RawMilvusMessage> poll(java.time.Duration timeout) {
            if (delivered) {
                return List.of();
            }
            delivered = true;
            Map<String, List<RawMilvusMessage>> source = seekedToStoredOffset ? messagesAfterStoredOffset : messagesByTopic;
            return source.getOrDefault(currentPchannel, List.of());
        }

        @Override
        public void close() {
        }
    }

    private static final class MilvusProtoDeserializerTestSupport {
        private static io.milvus.grpc.FieldData floatVectorField(String name, int dim, float... values) {
            FloatArray.Builder floats = FloatArray.newBuilder();
            for (float value : values) {
                floats.addData(value);
            }
            return io.milvus.grpc.FieldData.newBuilder()
                    .setFieldName(name)
                    .setType(io.milvus.grpc.DataType.FloatVector)
                    .setVectors(VectorField.newBuilder().setDim(dim).setFloatVector(floats.build()).build())
                    .build();
        }
    }
}
