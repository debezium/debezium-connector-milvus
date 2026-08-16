/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.util.Collect;
import io.debezium.util.Strings;
import io.milvus.grpc.ArrayArray;
import io.milvus.grpc.BoolArray;
import io.milvus.grpc.BytesArray;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DoubleArray;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.FloatArray;
import io.milvus.grpc.IDs;
import io.milvus.grpc.IntArray;
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

/**
 * Deserializes a raw Milvus MQ message into typed {@link MilvusChangeEvent}s.
 *
 * <p>
 * Two wire formats are supported, selected by the {@code wireFormat} passed at
 * construction time:
 * </p>
 * <ul>
 * <li><b>{@code proto_single}</b> — each raw message payload is exactly one
 * protobuf-encoded {@code msg.Msg} record (Insert / Delete / DDL /
 * TimeTick). The concrete type is determined by extracting the {@code MsgBase}
 * header first (field 1, common to all Milvus messages) and then dispatching
 * via {@code msgType} to the correct parser. This is unambiguous and avoids the
 * trial-parse strategy's implicit assumptions about field overlap.</li>
 * <li><b>{@code msgpack_batch}</b> — each raw message payload is a MsgPack
 * array {@code [msgType:int, pchannel:string, messages:array]} where each
 * element is a MsgPack map carrying the fields needed to build an event.
 * This mirrors the {@code TsMsg} serialization Milvus uses on the
 * msgpack-based msgstream.</li>
 * </ul>
 *
 * <p>
 * Insert payloads are columnar ({@code fieldsData}); they are pivoted into one
 * {@link MilvusChangeEvent.Insert} per row via {@link MilvusColumnarPivot}.
 * All parse failures are funnelled through
 * {@link MilvusWireFormatMismatchException} carrying the MQ coordinates so the
 * offending message can be located.
 * </p>
 */
public class MilvusProtoDeserializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusProtoDeserializer.class);

    public static final String FORMAT_MSGPACK_BATCH = "msgpack_batch";
    public static final String FORMAT_PROTO_SINGLE = "proto_single";

    private final String wireFormat;
    private final MilvusColumnarPivot pivot;

    public MilvusProtoDeserializer(String wireFormat, MilvusColumnarPivot pivot) {
        this.wireFormat = wireFormat;
        this.pivot = pivot;
    }

    /**
     * Deserialize a raw MQ message into a list of change events.
     *
     * @param message transport-agnostic raw message; must not be null and must
     *                carry a non-null, non-empty {@code value}
     * @return events — possibly empty (e.g. TimeTick or empty batch); never null
     * @throws MilvusWireFormatMismatchException if the payload cannot be parsed
     *                                           as the configured wire format, or
     *                                           if an unknown/unhandled MsgType is
     *                                           encountered
     */
    public List<MilvusChangeEvent> deserialize(RawMilvusMessage message)
            throws MilvusWireFormatMismatchException {
        if (message == null || message.getValue() == null || message.getValue().length == 0) {
            throw new MilvusWireFormatMismatchException(
                    wireFormat, "empty", safeTopic(message), safePartition(message), safeOffset(message),
                    "message value is null or empty");
        }

        return FORMAT_PROTO_SINGLE.equals(wireFormat)
                ? deserializeProtoSingle(message)
                : deserializeMsgPackBatch(message);
    }

    private List<MilvusChangeEvent> deserializeProtoSingle(RawMilvusMessage message)
            throws MilvusWireFormatMismatchException {
        byte[] bytes = message.getValue();
        try {
            TimeTickMsg probe = TimeTickMsg.parseFrom(bytes);
            if (!probe.hasBase()) {
                throw mismatch(message, "Cannot extract MsgBase from protobuf payload — missing field 1", null);
            }
            MsgBase base = probe.getBase();
            MsgType msgType = base.getMsgType();

            return switch (msgType) {
                case Insert -> {
                    InsertRequest ins = InsertRequest.parseFrom(bytes);
                    yield toInsertEvents(ins, message);
                }
                case Delete -> {
                    DeleteRequest del = DeleteRequest.parseFrom(bytes);
                    yield List.of(toDeleteEvent(del, message));
                }
                case CreateCollection -> {
                    CreateCollectionRequest cc = CreateCollectionRequest.parseFrom(bytes);
                    yield List.of(toDdlEvent(cc.getCollectionName(), cc.getBase(), "CREATE_COLLECTION", message));
                }
                case DropCollection -> {
                    DropCollectionRequest dc = DropCollectionRequest.parseFrom(bytes);
                    yield List.of(toDdlEvent(dc.getCollectionName(), dc.getBase(), "DROP_COLLECTION", message));
                }
                case TimeTick -> List.of(toTimeTickEvent(base, message));
                case CreatePartition, DropPartition -> {
                    LOGGER.debug("Skipping partition DDL msgType={} at topic={} offset={}",
                            msgType, safeTopic(message), safeOffset(message));
                    yield List.of();
                }
                default ->
                    throw mismatch(message,
                            "Unhandled MsgType=" + msgType.name() + " (raw=" + base.getMsgTypeValue() + "). "
                                    + "If this is a new Milvus message type, add an explicit handler or "
                                    + "a skip rule with a metric.",
                            null);
            };
        }
        catch (MilvusWireFormatMismatchException e) {
            throw e;
        }
        catch (InvalidProtocolBufferException e) {
            throw mismatch(message, "malformed protobuf: " + e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw mismatch(message, "Unhandled MsgType (unknown enum value): " + e.getMessage(), e);
        }
    }

    private List<MilvusChangeEvent> toInsertEvents(InsertRequest ins, RawMilvusMessage message)
            throws MilvusWireFormatMismatchException {
        long tso = effectiveTso(baseTimestamp(ins.getBase()), ins.getTimestampsList());
        String collection = ins.getCollectionName();
        String pchannel = safeTopic(message);
        String vchannel = Strings.defaultIfEmpty(ins.getShardName(), pchannel);

        List<MilvusFieldData> columns = new ArrayList<>();
        for (FieldData fd : ins.getFieldsDataList()) {
            columns.add(toFieldData(fd));
        }
        long numRows = ins.getNumRows();
        if (numRows == 0 && !Collect.isNullOrEmpty(columns)) {
            numRows = columns.get(0).getValues().size();
        }
        List<MilvusRow> rows = pivot.pivot(
                columns, (int) numRows,
                wireFormat, safeTopic(message), safePartition(message), safeOffset(message));

        List<MilvusChangeEvent> events = new ArrayList<>(rows.size());
        for (MilvusRow row : rows) {
            events.add(new MilvusChangeEvent.Insert(collection, pchannel, vchannel, tso, row));
        }
        return events;
    }

    private MilvusChangeEvent toDeleteEvent(DeleteRequest del, RawMilvusMessage message)
            throws MilvusWireFormatMismatchException {
        long tso = effectiveTso(baseTimestamp(del.getBase()), del.getTimestampsList());
        String pchannel = safeTopic(message);
        String vchannel = Strings.defaultIfEmpty(del.getShardName(), pchannel);

        Object primaryKeys;
        if (del.hasPrimaryKeys()) {
            IDs ids = del.getPrimaryKeys();
            if (ids.hasIntId()) {
                primaryKeys = ids.getIntId().getDataList();
            }
            else if (ids.hasStrId()) {
                primaryKeys = ids.getStrId().getDataList();
            }
            else {
                throw mismatch(message,
                        "DeleteRequest.primaryKeys is set but neither intId nor strId is populated", null);
            }
        }
        else if (del.getInt64PrimaryKeysCount() > 0) {
            primaryKeys = del.getInt64PrimaryKeysList();
        }
        else {
            throw mismatch(message,
                    "DeleteRequest has no primary keys (neither primaryKeys IDs nor int64_primary_keys)", null);
        }
        return new MilvusChangeEvent.Delete(del.getCollectionName(), pchannel, vchannel, tso, primaryKeys);
    }

    private MilvusChangeEvent toTimeTickEvent(MsgBase base, RawMilvusMessage message) {
        return new MilvusChangeEvent.TimeTick(null, safeTopic(message), safeTopic(message), baseTimestamp(base));
    }

    private MilvusChangeEvent toDdlEvent(String collection, MsgBase base, String ddlType, RawMilvusMessage message) {
        return new MilvusChangeEvent.DDL(collection, safeTopic(message), safeTopic(message),
                baseTimestamp(base), ddlType, collection);
    }

    private static long baseTimestamp(MsgBase base) {
        return base == null ? 0L : base.getTimestamp();
    }

    /**
     * Milvus does not populate {@code MsgBase.timestamp} on the insert half of an
     * upsert; the commit TSO is then only present in the per-row {@code timestamps}
     * field. A zero TSO must never reach the ordering engine, which would drop the
     * event as infinitely late. All rows in one DML message share the same TSO, so
     * the first entry is authoritative.
     */
    private static long effectiveTso(long baseTso, List<Long> rowTimestamps) {
        if (baseTso != 0L || rowTimestamps == null || rowTimestamps.isEmpty()) {
            return baseTso;
        }
        return rowTimestamps.get(0);
    }

    /**
     * Convert a protobuf {@link FieldData} column into a transport-neutral
     * {@link MilvusFieldData}, extracting scalar or vector values into a plain
     * {@code List<Object>}.
     */
    private MilvusFieldData toFieldData(FieldData fd) {
        DataType type = fd.getType();
        String name = Strings.defaultIfEmpty(fd.getFieldName(), String.valueOf(fd.getFieldId()));
        List<Object> values = new ArrayList<>();

        if (fd.hasVectors()) {
            VectorField vectors = fd.getVectors();
            long dim = vectors.getDim();
            if (vectors.hasFloatVector()) {
                List<Float> flat = vectors.getFloatVector().getDataList();
                int d = dim > 0 ? (int) dim : flat.size();
                for (int i = 0; i + d <= flat.size(); i += d) {
                    float[] vec = new float[d];
                    for (int j = 0; j < d; j++) {
                        vec[j] = flat.get(i + j);
                    }
                    values.add(vec);
                }
            }
            else if (vectors.hasBinaryVector() || vectors.hasInt8Vector()) {
                ByteString raw = vectors.hasBinaryVector() ? vectors.getBinaryVector() : vectors.getInt8Vector();
                int d = dim > 0 ? (int) dim : raw.size();
                for (int i = 0; i + d <= raw.size(); i += d) {
                    values.add(raw.substring(i, i + d).toByteArray());
                }
            }
            else if (vectors.hasFloat16Vector()) {
                ByteString raw = vectors.getFloat16Vector();
                int d = dim > 0 ? (int) (dim * 2) : raw.size();
                for (int i = 0; i + d <= raw.size(); i += d) {
                    values.add(raw.substring(i, i + d).toByteArray());
                }
            }
            else if (vectors.hasBfloat16Vector()) {
                ByteString raw = vectors.getBfloat16Vector();
                int d = dim > 0 ? (int) (dim * 2) : raw.size();
                for (int i = 0; i + d <= raw.size(); i += d) {
                    values.add(raw.substring(i, i + d).toByteArray());
                }
            }
            else if (vectors.hasSparseFloatVector()) {
                values.add(sparseFloatVectorToJson(vectors.getSparseFloatVector()));
            }
            else {
                LOGGER.warn("Unknown vector sub-type for field '{}'; passing raw bytes", name);
                values.add(new byte[0]);
            }
            return new MilvusFieldData(name, type, values, dim);
        }

        if (fd.hasScalars()) {
            ScalarField scalars = fd.getScalars();
            switch (scalars.getDataCase()) {
                case BOOL_DATA -> {
                    BoolArray arr = scalars.getBoolData();
                    for (int i = 0; i < arr.getDataCount(); i++) {
                        values.add(arr.getData(i));
                    }
                }
                case INT_DATA -> {
                    IntArray arr = scalars.getIntData();
                    for (int i = 0; i < arr.getDataCount(); i++) {
                        values.add(arr.getData(i));
                    }
                }
                case LONG_DATA -> {
                    LongArray arr = scalars.getLongData();
                    for (int i = 0; i < arr.getDataCount(); i++) {
                        values.add(arr.getData(i));
                    }
                }
                case FLOAT_DATA -> {
                    FloatArray arr = scalars.getFloatData();
                    for (int i = 0; i < arr.getDataCount(); i++) {
                        values.add(arr.getData(i));
                    }
                }
                case DOUBLE_DATA -> {
                    DoubleArray arr = scalars.getDoubleData();
                    for (int i = 0; i < arr.getDataCount(); i++) {
                        values.add(arr.getData(i));
                    }
                }
                case STRING_DATA -> {
                    StringArray arr = scalars.getStringData();
                    for (int i = 0; i < arr.getDataCount(); i++) {
                        values.add(arr.getData(i));
                    }
                }
                case JSON_DATA -> {
                    JSONArray arr = scalars.getJsonData();
                    for (int i = 0; i < arr.getDataCount(); i++) {
                        values.add(arr.getData(i).toStringUtf8());
                    }
                }
                case ARRAY_DATA -> {
                    ArrayArray arr = scalars.getArrayData();
                    for (int i = 0; i < arr.getDataCount(); i++) {
                        values.add(extractScalarFieldFirstValue(arr.getData(i)));
                    }
                }
                case BYTES_DATA -> {
                    BytesArray arr = scalars.getBytesData();
                    for (int i = 0; i < arr.getDataCount(); i++) {
                        values.add(arr.getData(i).toByteArray());
                    }
                }
                case DATA_NOT_SET -> {
                }
                default -> {
                    LOGGER.warn("Unhandled ScalarField.DataCase={} for field '{}'; values will be empty",
                            scalars.getDataCase(), name);
                }
            }
        }
        return new MilvusFieldData(name, type, values, 0);
    }

    /**
     * Extracts the first (and typically only) value from a nested
     * {@link ScalarField} that represents one element of an Array-type field.
     * Returns a plain Java object.
     */
    private static Object extractScalarFieldFirstValue(ScalarField sf) {
        return switch (sf.getDataCase()) {
            case BOOL_DATA -> sf.getBoolData().getDataCount() > 0
                    ? sf.getBoolData().getData(0)
                    : null;
            case INT_DATA -> sf.getIntData().getDataCount() > 0
                    ? sf.getIntData().getData(0)
                    : null;
            case LONG_DATA -> sf.getLongData().getDataCount() > 0
                    ? sf.getLongData().getData(0)
                    : null;
            case FLOAT_DATA -> sf.getFloatData().getDataCount() > 0
                    ? sf.getFloatData().getData(0)
                    : null;
            case DOUBLE_DATA -> sf.getDoubleData().getDataCount() > 0
                    ? sf.getDoubleData().getData(0)
                    : null;
            case STRING_DATA -> sf.getStringData().getDataCount() > 0
                    ? sf.getStringData().getData(0)
                    : null;
            default -> null;
        };
    }

    /**
     * <p>
     * The binary encoding stores each non-zero entry as 8 bytes:
     * {@code [uint32 index (LE)][float32 value (LE)]}.
     * </p>
     */
    static String sparseFloatVectorToJson(SparseFloatArray sparse) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (ByteString content : sparse.getContentsList()) {
            byte[] bytes = content.toByteArray();
            ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            while (buf.remaining() >= 8) {
                long index = Integer.toUnsignedLong(buf.getInt());
                float value = buf.getFloat();
                if (!first) {
                    sb.append(',');
                }
                sb.append('"').append(index).append("\":").append(value);
                first = false;
            }
        }
        sb.append('}');
        return sb.toString();
    }

    private List<MilvusChangeEvent> deserializeMsgPackBatch(RawMilvusMessage message)
            throws MilvusWireFormatMismatchException {
        byte[] bytes = message.getValue();
        List<MilvusChangeEvent> events = new ArrayList<>();
        try (org.msgpack.core.MessageUnpacker unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(bytes)) {
            if (!unpacker.getNextFormat().getValueType().isArrayType()) {
                throw mismatch(message, "msgpack_batch payload is not an array", null);
            }
            int outerLen = unpacker.unpackArrayHeader();
            int msgType = -1;
            String pchannel = safeTopic(message);
            int messagesIdx = -1;
            for (int i = 0; i < outerLen; i++) {
                org.msgpack.value.Value v = unpacker.unpackValue();
                if (i == 0 && v.isIntegerValue()) {
                    msgType = v.asIntegerValue().toInt();
                }
                else if (i == 1 && v.isStringValue()) {
                    pchannel = v.asStringValue().asString();
                }
                else if (v.isArrayValue()) {
                    messagesIdx = i;
                    for (org.msgpack.value.Value elem : v.asArrayValue()) {
                        if (elem.isMapValue()) {
                            events.addAll(deserializeMsgPackMessage(elem.asMapValue(), pchannel, message));
                        }
                    }
                }
            }
            if (messagesIdx < 0 && msgType >= 0) {
                LOGGER.debug("msgpack_batch had no inner messages array at topic={} offset={}",
                        safeTopic(message), safeOffset(message));
            }
        }
        catch (MilvusWireFormatMismatchException e) {
            throw e;
        }
        catch (Exception e) {
            throw mismatch(message, "malformed msgpack: " + e.getMessage(), e);
        }
        return events;
    }

    private List<MilvusChangeEvent> deserializeMsgPackMessage(
                                                              org.msgpack.value.MapValue map, String pchannel, RawMilvusMessage message)
            throws MilvusWireFormatMismatchException {
        org.msgpack.value.Value msgTypeVal = map.map().get(strKey("msgType"));
        int msgType = (msgTypeVal != null && msgTypeVal.isIntegerValue())
                ? msgTypeVal.asIntegerValue().toInt()
                : MsgType.TimeTick.getNumber();
        String collection = mapString(map, "collectionName");
        String vchannel = Strings.defaultIfEmpty(mapString(map, "vchannel"), pchannel);
        long tso = mapLong(map, "ts");
        if (tso == 0L) {
            tso = mapLong(map, "timestamp");
        }

        if (msgType == MsgType.Insert.getNumber()) {
            return msgpackInsert(map, collection, pchannel, vchannel, tso, message);
        }
        if (msgType == MsgType.Delete.getNumber()) {
            return List.of(new MilvusChangeEvent.Delete(collection, pchannel, vchannel, tso,
                    mapLongList(map, "primaryKeys")));
        }
        if (msgType == MsgType.CreateCollection.getNumber()
                || msgType == MsgType.DropCollection.getNumber()) {
            String ddlType = (msgType == MsgType.CreateCollection.getNumber())
                    ? "CREATE_COLLECTION"
                    : "DROP_COLLECTION";
            return List.of(new MilvusChangeEvent.DDL(collection, pchannel, vchannel, tso, ddlType, collection));
        }
        if (msgType == MsgType.TimeTick.getNumber()) {
            return List.of(new MilvusChangeEvent.TimeTick(collection, pchannel, vchannel, tso));
        }
        if (msgType == MsgType.CreatePartition.getNumber()
                || msgType == MsgType.DropPartition.getNumber()) {
            LOGGER.debug("Skipping partition DDL msgType={} in msgpack_batch at topic={} offset={}",
                    msgType, safeTopic(message), safeOffset(message));
            return List.of();
        }
        LOGGER.warn("Unknown msgType={} in msgpack_batch at topic={} offset={}",
                msgType, safeTopic(message), safeOffset(message));
        return List.of();
    }

    private List<MilvusChangeEvent> msgpackInsert(
                                                  org.msgpack.value.MapValue map, String collection, String pchannel, String vchannel,
                                                  long tso, RawMilvusMessage message)
            throws MilvusWireFormatMismatchException {

        org.msgpack.value.Value fields = map.map().get(strKey("fieldsData"));
        List<MilvusFieldData> columns = new ArrayList<>();
        int numRows = (int) mapLong(map, "numRows");
        if (fields != null && fields.isArrayValue()) {
            for (org.msgpack.value.Value col : fields.asArrayValue()) {
                if (col.isMapValue()) {
                    columns.add(toFieldDataFromMsgpack(col.asMapValue()));
                }
            }
        }
        if (numRows == 0 && !Collect.isNullOrEmpty(columns)) {
            numRows = columns.get(0).getValues().size();
        }
        List<MilvusRow> rows = pivot.pivot(
                columns, numRows,
                wireFormat, safeTopic(message), safePartition(message), safeOffset(message));
        List<MilvusChangeEvent> events = new ArrayList<>(rows.size());
        for (MilvusRow row : rows) {
            events.add(new MilvusChangeEvent.Insert(collection, pchannel, vchannel, tso, row));
        }
        return events;
    }

    private MilvusFieldData toFieldDataFromMsgpack(org.msgpack.value.MapValue map) {
        String name = mapString(map, "fieldName");
        int typeNum = (int) mapLong(map, "type");
        DataType type = DataType.forNumber(typeNum);
        if (type == null) {
            type = DataType.None;
        }
        long dim = mapLong(map, "dim");
        List<Object> values = new ArrayList<>();

        org.msgpack.value.Value vectorVal = map.map().get(strKey("vectorData"));
        if (vectorVal != null && vectorVal.isBinaryValue()) {
            byte[] raw = vectorVal.asBinaryValue().asByteArray();
            if (type == DataType.FloatVector && dim > 0) {
                int bytesPerVec = (int) dim * Float.BYTES;
                ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
                while (buf.remaining() >= bytesPerVec) {
                    float[] vec = new float[(int) dim];
                    for (int j = 0; j < dim; j++) {
                        vec[j] = buf.getFloat();
                    }
                    values.add(vec);
                }
            }
            else if (type == DataType.BinaryVector && dim > 0) {
                int bytesPerVec = (int) Math.ceil(dim / 8.0);
                for (int i = 0; i + bytesPerVec <= raw.length; i += bytesPerVec) {
                    byte[] vec = new byte[bytesPerVec];
                    System.arraycopy(raw, i, vec, 0, bytesPerVec);
                    values.add(vec);
                }
            }
            else if (type == DataType.Float16Vector || type == DataType.BFloat16Vector) {
                int bytesPerVec = dim > 0 ? (int) (dim * 2) : raw.length;
                for (int i = 0; i + bytesPerVec <= raw.length; i += bytesPerVec) {
                    byte[] vec = new byte[bytesPerVec];
                    System.arraycopy(raw, i, vec, 0, bytesPerVec);
                    values.add(vec);
                }
            }
            else if (type == DataType.SparseFloatVector) {
                ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                while (buf.remaining() >= 8) {
                    long index = Integer.toUnsignedLong(buf.getInt());
                    float value = buf.getFloat();
                    if (!first) {
                        sb.append(',');
                    }
                    sb.append('"').append(index).append("\":").append(value);
                    first = false;
                }
                sb.append('}');
                values.add(sb.toString());
            }
            else {
                values.add(raw);
            }
        }
        else {
            org.msgpack.value.Value dataVal = map.map().get(strKey("values"));
            if (dataVal != null && dataVal.isArrayValue()) {
                for (org.msgpack.value.Value v : dataVal.asArrayValue()) {
                    values.add(msgpackScalar(v, type));
                }
            }
        }
        return new MilvusFieldData(name, type, values, dim);
    }

    /**
     * Convert a single MsgPack value cell into a Java object. The {@code type}
     * hint is used for disambiguation where MsgPack's type system is coarser
     * than Milvus's (e.g. JSON is a string in MsgPack but should be kept as
     * a {@code String}; Array fields may carry nested arrays).
     */
    private static Object msgpackScalar(org.msgpack.value.Value v, DataType type) {
        if (v.isNilValue()) {
            return null;
        }
        if (v.isBooleanValue()) {
            return v.asBooleanValue().getBoolean();
        }
        if (v.isIntegerValue()) {
            long l = v.asIntegerValue().toLong();
            if (type == DataType.Int32 && l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                return (int) l;
            }
            return l;
        }
        if (v.isFloatValue()) {
            double d = v.asFloatValue().toDouble();
            if (type == DataType.Float) {
                return (float) d;
            }
            return d;
        }
        if (v.isStringValue()) {
            return v.asStringValue().asString();
        }
        if (v.isBinaryValue()) {
            return v.asBinaryValue().asByteArray();
        }
        if (v.isArrayValue()) {
            List<Object> list = new ArrayList<>();
            for (org.msgpack.value.Value elem : v.asArrayValue()) {
                list.add(msgpackScalar(elem, DataType.None));
            }
            return list;
        }
        return v.toString();
    }

    private static org.msgpack.value.Value strKey(String s) {
        return org.msgpack.value.ValueFactory.newString(s);
    }

    private static String mapString(org.msgpack.value.MapValue map, String key) {
        org.msgpack.value.Value v = map.map().get(strKey(key));
        return (v != null && v.isStringValue()) ? v.asStringValue().asString() : null;
    }

    private static long mapLong(org.msgpack.value.MapValue map, String key) {
        org.msgpack.value.Value v = map.map().get(strKey(key));
        return (v != null && v.isIntegerValue()) ? v.asIntegerValue().toLong() : 0L;
    }

    private static List<Long> mapLongList(org.msgpack.value.MapValue map, String key) {
        org.msgpack.value.Value v = map.map().get(strKey(key));
        List<Long> out = new ArrayList<>();
        if (v != null && v.isArrayValue()) {
            for (org.msgpack.value.Value e : v.asArrayValue()) {
                if (e.isIntegerValue()) {
                    out.add(e.asIntegerValue().toLong());
                }
            }
        }
        return out;
    }

    private MilvusWireFormatMismatchException mismatch(RawMilvusMessage message, String detail, Throwable cause) {
        if (cause != null) {
            return new MilvusWireFormatMismatchException(
                    wireFormat, "unknown", safeTopic(message), safePartition(message), safeOffset(message),
                    detail, cause);
        }
        return new MilvusWireFormatMismatchException(
                wireFormat, "unknown", safeTopic(message), safePartition(message), safeOffset(message),
                detail);
    }

    private static String safeTopic(RawMilvusMessage m) {
        return m == null ? "<unknown>" : m.getTopic();
    }

    private static int safePartition(RawMilvusMessage m) {
        return m == null ? -1 : m.getPartition();
    }

    private static long safeOffset(RawMilvusMessage m) {
        return m == null ? -1L : m.getOffset();
    }
}
