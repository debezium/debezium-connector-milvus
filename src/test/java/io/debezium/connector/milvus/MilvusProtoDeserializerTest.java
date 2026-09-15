/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import io.debezium.connector.milvus.MilvusConnectorConfig.WireFormat;
import io.debezium.doc.FixFor;
import io.milvus.grpc.BoolArray;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.FloatArray;
import io.milvus.grpc.IDs;
import io.milvus.grpc.JSONArray;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.MsgBase;
import io.milvus.grpc.MsgType;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.SparseFloatArray;
import io.milvus.grpc.StringArray;
import io.milvus.grpc.VectorField;

import milvus.proto.msg.Msg.CreateCollectionRequest;
import milvus.proto.msg.Msg.DeleteRequest;
import milvus.proto.msg.Msg.DropCollectionRequest;
import milvus.proto.msg.Msg.InsertRequest;
import milvus.proto.msg.Msg.TimeTickMsg;

public class MilvusProtoDeserializerTest {

    private static final String TOPIC = "by-dev-rootcoord-dml_0";

    private final MilvusColumnarPivot pivot = new MilvusColumnarPivot(new MilvusValueConverter(null));

    private static Object rowGet(MilvusChangeEvent.Insert insert, String fieldName) {
        MilvusRow row = insert.getRow();
        String[] names = row.getFieldNames();
        Object[] values = row.getFieldValues();
        for (int i = 0; i < names.length; i++) {
            if (names[i].equals(fieldName)) {
                return values[i];
            }
        }
        return null;
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldDeserializeProtoSingleInsertIntoRowEvents() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        InsertRequest insert = InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, 123L))
                .setCollectionName("books")
                .setShardName("books_v0")
                .addFieldsData(longField("id", 1L, 2L))
                .addFieldsData(stringField("title", "Dune", "Hyperion"))
                .addFieldsData(floatVectorField("embedding", 2, 1.0f, 2.0f, 3.0f, 4.0f))
                .setNumRows(2)
                .build();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(insert.toByteArray()));

        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(MilvusChangeEvent.Insert.class);
        MilvusChangeEvent.Insert first = (MilvusChangeEvent.Insert) events.get(0);
        assertThat(first.getCollectionName()).isEqualTo("books");
        assertThat(first.getPchannel()).isEqualTo(TOPIC);
        assertThat(first.getVchannel()).isEqualTo("books_v0");
        assertThat(first.getTso()).isEqualTo(123L);
        assertThat(first.getRow().getFieldNames()).containsExactly("id", "title", "embedding");
        assertThat(rowGet(first, "id")).isEqualTo(1L);
        assertThat(rowGet(first, "title")).isEqualTo("Dune");
        assertThat(rowGet(first, "embedding")).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Float> firstEmbedding = (List<Float>) rowGet(first, "embedding");
        assertThat(firstEmbedding).containsExactly(1.0f, 2.0f);

        MilvusChangeEvent.Insert second = (MilvusChangeEvent.Insert) events.get(1);
        assertThat(rowGet(second, "id")).isEqualTo(2L);
        assertThat(rowGet(second, "title")).isEqualTo("Hyperion");
        assertThat(rowGet(second, "embedding")).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Float> secondEmbedding = (List<Float>) rowGet(second, "embedding");
        assertThat(secondEmbedding).containsExactly(3.0f, 4.0f);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldInferProtoInsertRowCountFromFieldData() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        InsertRequest insert = InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, 77L))
                .setCollectionName("books")
                .addFieldsData(longField("id", 10L, 11L))
                .build();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(insert.toByteArray()));

        assertThat(events).hasSize(2);
        assertThat(rowGet((MilvusChangeEvent.Insert) events.get(0), "id")).isEqualTo(10L);
        assertThat(rowGet((MilvusChangeEvent.Insert) events.get(1), "id")).isEqualTo(11L);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldDeserializeInsertWithJsonField() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        JSONArray jsonArr = JSONArray.newBuilder()
                .addData(com.google.protobuf.ByteString.copyFromUtf8("{\"k\":1}"))
                .addData(com.google.protobuf.ByteString.copyFromUtf8("{\"k\":2}"))
                .build();
        FieldData jsonField = FieldData.newBuilder()
                .setFieldName("meta")
                .setType(DataType.JSON)
                .setScalars(ScalarField.newBuilder().setJsonData(jsonArr).build())
                .build();

        InsertRequest insert = InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, 200L))
                .setCollectionName("docs")
                .addFieldsData(longField("id", 1L, 2L))
                .addFieldsData(jsonField)
                .setNumRows(2)
                .build();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(insert.toByteArray()));

        assertThat(events).hasSize(2);
        assertThat(rowGet((MilvusChangeEvent.Insert) events.get(0), "meta")).isEqualTo("{\"k\":1}");
        assertThat(rowGet((MilvusChangeEvent.Insert) events.get(1), "meta")).isEqualTo("{\"k\":2}");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldDeserializeInsertWithBoolField() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        BoolArray boolArr = BoolArray.newBuilder().addData(true).addData(false).build();
        FieldData boolField = FieldData.newBuilder()
                .setFieldName("active")
                .setType(DataType.Bool)
                .setScalars(ScalarField.newBuilder().setBoolData(boolArr).build())
                .build();

        InsertRequest insert = InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, 201L))
                .setCollectionName("flags")
                .addFieldsData(longField("id", 10L, 11L))
                .addFieldsData(boolField)
                .setNumRows(2)
                .build();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(insert.toByteArray()));

        assertThat(events).hasSize(2);
        assertThat(rowGet((MilvusChangeEvent.Insert) events.get(0), "active")).isEqualTo(true);
        assertThat(rowGet((MilvusChangeEvent.Insert) events.get(1), "active")).isEqualTo(false);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldDeserializeProtoSingleDeleteWithInt64PKsViaIDsOneof() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        IDs ids = IDs.newBuilder()
                .setIntId(LongArray.newBuilder().addData(9L).addData(11L).build())
                .build();
        DeleteRequest delete = DeleteRequest.newBuilder()
                .setBase(base(MsgType.Delete, 555L))
                .setCollectionName("books")
                .setShardName("books_v0")
                .setPrimaryKeys(ids)
                .build();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(delete.toByteArray()));

        assertThat(events).singleElement().isInstanceOf(MilvusChangeEvent.Delete.class);
        MilvusChangeEvent.Delete event = (MilvusChangeEvent.Delete) events.get(0);
        assertThat(event.getCollectionName()).isEqualTo("books");
        assertThat(event.getVchannel()).isEqualTo("books_v0");
        assertThat(event.getTso()).isEqualTo(555L);
        assertThat(event.getPrimaryKeys()).isEqualTo(List.of(9L, 11L));
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldDeserializeProtoSingleDeleteWithStringPKsViaIDsOneof() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        IDs ids = IDs.newBuilder()
                .setStrId(StringArray.newBuilder().addData("pk-a").addData("pk-b").build())
                .build();
        DeleteRequest delete = DeleteRequest.newBuilder()
                .setBase(base(MsgType.Delete, 600L))
                .setCollectionName("articles")
                .setShardName("articles_v0")
                .setPrimaryKeys(ids)
                .build();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(delete.toByteArray()));

        assertThat(events).singleElement().isInstanceOf(MilvusChangeEvent.Delete.class);
        MilvusChangeEvent.Delete event = (MilvusChangeEvent.Delete) events.get(0);
        assertThat(event.getPrimaryKeys()).isEqualTo(List.of("pk-a", "pk-b"));
    }

    @Test
    @FixFor("debezium/dbz#2437")
    void shouldFallBackToRowTimestampsWhenInsertBaseTimestampIsZero() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        InsertRequest insert = InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, 0L))
                .setCollectionName("books")
                .setShardName("books_v0")
                .addAllTimestamps(List.of(777L, 777L))
                .addFieldsData(longField("id", 1L, 2L))
                .setNumRows(2)
                .build();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(insert.toByteArray()));

        assertThat(events).hasSize(2);
        assertThat(events).allMatch(e -> e.getTso() == 777L);
    }

    @Test
    @FixFor("debezium/dbz#2437")
    void shouldFallBackToRowTimestampsWhenDeleteBaseTimestampIsZero() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        IDs ids = IDs.newBuilder()
                .setIntId(LongArray.newBuilder().addData(9L).build())
                .build();
        DeleteRequest delete = DeleteRequest.newBuilder()
                .setBase(base(MsgType.Delete, 0L))
                .setCollectionName("books")
                .setShardName("books_v0")
                .addTimestamps(888L)
                .setPrimaryKeys(ids)
                .build();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(delete.toByteArray()));

        assertThat(events).singleElement().isInstanceOf(MilvusChangeEvent.Delete.class);
        assertThat(events.get(0).getTso()).isEqualTo(888L);
    }

    @Test
    @FixFor("debezium/dbz#2437")
    void shouldPreferBaseTimestampOverRowTimestamps() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        InsertRequest insert = InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, 123L))
                .setCollectionName("books")
                .addTimestamps(999L)
                .addFieldsData(longField("id", 1L))
                .setNumRows(1)
                .build();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(insert.toByteArray()));

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getTso()).isEqualTo(123L);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldDeserializeProtoSingleDdlAndTimetick() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        CreateCollectionRequest create = CreateCollectionRequest.newBuilder()
                .setBase(base(MsgType.CreateCollection, 42L))
                .setCollectionName("books")
                .build();
        DropCollectionRequest drop = DropCollectionRequest.newBuilder()
                .setBase(base(MsgType.DropCollection, 43L))
                .setCollectionName("books")
                .build();
        TimeTickMsg timetick = TimeTickMsg.newBuilder()
                .setBase(base(MsgType.TimeTick, 44L))
                .build();

        List<MilvusChangeEvent> createEvents = deserializer.deserialize(message(create.toByteArray()));
        List<MilvusChangeEvent> dropEvents = deserializer.deserialize(message(drop.toByteArray()));
        List<MilvusChangeEvent> timetickEvents = deserializer.deserialize(message(timetick.toByteArray()));

        assertThat(createEvents).singleElement().isInstanceOf(MilvusChangeEvent.DDL.class);
        assertThat(((MilvusChangeEvent.DDL) createEvents.get(0)).getDdlType()).isEqualTo("CREATE_COLLECTION");
        assertThat(dropEvents).singleElement().isInstanceOf(MilvusChangeEvent.DDL.class);
        assertThat(((MilvusChangeEvent.DDL) dropEvents.get(0)).getDdlType()).isEqualTo("DROP_COLLECTION");
        assertThat(timetickEvents).singleElement().isInstanceOf(MilvusChangeEvent.TimeTick.class);
        assertThat(timetickEvents.get(0).getTso()).isEqualTo(44L);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldThrowOnEmptyMessage() {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        assertThatThrownBy(() -> deserializer
                .deserialize(new RawMilvusMessage(TOPIC, 0, 1L, null, new byte[0], 0L)))
                .isInstanceOf(MilvusWireFormatMismatchException.class)
                .hasMessageContaining("message value is null or empty");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldThrowOnUnrecognizedProtoMsgType() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.PROTO_SINGLE, pivot);

        TimeTickMsg withUnknownType = TimeTickMsg.newBuilder()
                .setBase(MsgBase.newBuilder()
                        .setMsgTypeValue(9999)
                        .setTimestamp(88L)
                        .build())
                .build();

        assertThatThrownBy(() -> deserializer.deserialize(message(withUnknownType.toByteArray())))
                .isInstanceOf(MilvusWireFormatMismatchException.class)
                .hasMessageContaining("Unhandled MsgType");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldDeserializeMsgpackBatchInsertAndDelete() throws Exception {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.MSGPACK_BATCH, pivot);

        byte[] payload = msgpackBatch();

        List<MilvusChangeEvent> events = deserializer.deserialize(message(payload));

        assertThat(events).hasSize(3);
        assertThat(events.get(0)).isInstanceOf(MilvusChangeEvent.Insert.class);
        MilvusChangeEvent.Insert row0 = (MilvusChangeEvent.Insert) events.get(0);
        assertThat(rowGet(row0, "id")).isEqualTo(1L);
        assertThat(rowGet(row0, "title")).isEqualTo("Dune");
        MilvusChangeEvent.Insert row1 = (MilvusChangeEvent.Insert) events.get(1);
        assertThat(rowGet(row1, "id")).isEqualTo(2L);
        assertThat(rowGet(row1, "title")).isEqualTo("Hyperion");
        assertThat(events.get(2)).isInstanceOf(MilvusChangeEvent.Delete.class);
        assertThat(((MilvusChangeEvent.Delete) events.get(2)).getPrimaryKeys()).isEqualTo(List.of(1L, 2L));
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldThrowOnMalformedMsgpackPayload() {
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(
                WireFormat.MSGPACK_BATCH, pivot);

        assertThatThrownBy(() -> deserializer.deserialize(message(new byte[]{ 0x01, 0x02, 0x03 })))
                .isInstanceOf(MilvusWireFormatMismatchException.class)
                .hasMessageContaining("msgpack_batch payload is not an array");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldSerializeSparseFloatVectorToJson() {
        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(1);
        buf.putFloat(0.5f);
        buf.putInt(42);
        buf.putFloat(1.0f);
        buf.flip();

        SparseFloatArray sparse = SparseFloatArray.newBuilder()
                .addContents(com.google.protobuf.ByteString.copyFrom(buf))
                .build();

        String json = MilvusProtoDeserializer.sparseFloatVectorToJson(sparse);

        assertThat(json).startsWith("{").endsWith("}");
        assertThat(json).contains("\"1\":");
        assertThat(json).contains("\"42\":");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldReturnEmptyJsonForEmptySparseVector() {
        SparseFloatArray sparse = SparseFloatArray.newBuilder().build();
        assertThat(MilvusProtoDeserializer.sparseFloatVectorToJson(sparse)).isEqualTo("{}");
    }

    private static RawMilvusMessage message(byte[] value) {
        return new RawMilvusMessage(TOPIC, 0, 1L, null, value, 0L);
    }

    private static MsgBase base(MsgType type, long timestamp) {
        return MsgBase.newBuilder()
                .setMsgType(type)
                .setTimestamp(timestamp)
                .build();
    }

    private static FieldData longField(String name, long... values) {
        LongArray.Builder longs = LongArray.newBuilder();
        for (long value : values) {
            longs.addData(value);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.Int64)
                .setScalars(ScalarField.newBuilder().setLongData(longs.build()).build())
                .build();
    }

    private static FieldData stringField(String name, String... values) {
        StringArray.Builder strings = StringArray.newBuilder();
        for (String value : values) {
            strings.addData(value);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.VarChar)
                .setScalars(ScalarField.newBuilder().setStringData(strings.build()).build())
                .build();
    }

    private static FieldData floatVectorField(String name, int dim, float... values) {
        FloatArray.Builder floats = FloatArray.newBuilder();
        for (float value : values) {
            floats.addData(value);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.FloatVector)
                .setVectors(VectorField.newBuilder().setDim(dim).setFloatVector(floats.build()).build())
                .build();
    }

    private static byte[] msgpackBatch() throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packArrayHeader(3);
        packer.packInt(MsgType.Insert.getNumber());
        packer.packString(TOPIC);
        packer.packArrayHeader(2);

        packInsertMessage(packer);
        packDeleteMessage(packer);

        packer.close();
        return packer.toByteArray();
    }

    private static void packInsertMessage(MessageBufferPacker packer) throws IOException {
        packer.packMapHeader(6);
        packer.packString("msgType");
        packer.packInt(MsgType.Insert.getNumber());
        packer.packString("collectionName");
        packer.packString("books");
        packer.packString("vchannel");
        packer.packString("books_v0");
        packer.packString("ts");
        packer.packLong(321L);
        packer.packString("numRows");
        packer.packInt(2);
        packer.packString("fieldsData");
        packer.packArrayHeader(2);

        packLongColumn(packer, "id", 1L, 2L);
        packStringColumn(packer, "title", "Dune", "Hyperion");
    }

    private static void packDeleteMessage(MessageBufferPacker packer) throws IOException {
        packer.packMapHeader(5);
        packer.packString("msgType");
        packer.packInt(MsgType.Delete.getNumber());
        packer.packString("collectionName");
        packer.packString("books");
        packer.packString("vchannel");
        packer.packString("books_v0");
        packer.packString("ts");
        packer.packLong(322L);
        packer.packString("primaryKeys");
        packer.packArrayHeader(2);
        packer.packLong(1L);
        packer.packLong(2L);
    }

    private static void packLongColumn(MessageBufferPacker packer, String fieldName, long... values)
            throws IOException {
        packer.packMapHeader(4);
        packer.packString("fieldName");
        packer.packString(fieldName);
        packer.packString("type");
        packer.packInt(DataType.Int64.getNumber());
        packer.packString("dim");
        packer.packInt(0);
        packer.packString("values");
        packer.packArrayHeader(values.length);
        for (long value : values) {
            packer.packLong(value);
        }
    }

    private static void packStringColumn(MessageBufferPacker packer, String fieldName, String... values)
            throws IOException {
        packer.packMapHeader(4);
        packer.packString("fieldName");
        packer.packString(fieldName);
        packer.packString("type");
        packer.packInt(DataType.VarChar.getNumber());
        packer.packString("dim");
        packer.packInt(0);
        packer.packString("values");
        packer.packArrayHeader(values.length);
        for (String value : values) {
            packer.packString(value);
        }
    }
}
