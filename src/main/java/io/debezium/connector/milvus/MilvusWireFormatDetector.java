/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.msgpack.core.MessagePack;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.Collect;
import io.debezium.util.Strings;
import io.milvus.grpc.MsgType;

import milvus.proto.msg.Msg.CreateCollectionRequest;
import milvus.proto.msg.Msg.DeleteRequest;
import milvus.proto.msg.Msg.DropCollectionRequest;
import milvus.proto.msg.Msg.InsertRequest;
import milvus.proto.msg.Msg.TimeTickMsg;

/**
 * Detects the Milvus MQ wire format used by the source cluster.
 *
 * <p>
 * Detection probes the beginning of each configured pchannel and inspects the
 * first non-timetick payload. The format is expected to be stable cluster-wide,
 * so mixed detections are treated as fatal.
 * </p>
 */
public class MilvusWireFormatDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusWireFormatDetector.class);

    static final Duration PROBE_TIMEOUT = Duration.ofSeconds(5);

    private final MilvusConnectorConfig config;
    private final MessageConsumerFactory messageConsumerFactory;

    @FunctionalInterface
    interface MessageConsumerFactory {
        MilvusMessageConsumer create();
    }

    public MilvusWireFormatDetector(MilvusConnectorConfig config) {
        this(config, () -> new KafkaMilvusMessageConsumer(config));
    }

    MilvusWireFormatDetector(MilvusConnectorConfig config, MessageConsumerFactory messageConsumerFactory) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.messageConsumerFactory = Objects.requireNonNull(messageConsumerFactory,
                "messageConsumerFactory must not be null");
    }

    /**
     * Probes the given pchannels and returns the cluster wire format.
     *
     * <p>
     * Convenience overload that starts the probe from {@code EARLIEST}.
     * Prefer {@link #detect(Set, Map)} on warm restart so the probe reads messages
     * at the actual processing position rather than potentially pre-upgrade
     * messages.
     * </p>
     */
    public String detect(Set<String> pchannels) {
        return detect(pchannels, Map.of());
    }

    /**
     * Probes the given pchannels and returns the cluster wire format.
     *
     * <p>
     * When a stored offset exists for a pchannel the probe starts there, so a
     * warm restart inspects the messages that will actually be processed next.
     * An idle pchannel carries nothing but timeticks after the stored offset, so
     * if that probe finds no data message the pchannel is re-probed from
     * {@code EARLIEST} before falling back to the configured/default format.
     * </p>
     *
     * @param pchannels     pchannels to probe; null or empty returns the fallback
     * @param storedOffsets last-committed Kafka offsets keyed by
     *                      {@link TopicPartition};
     *                      empty map falls back to {@code EARLIEST}
     */
    public String detect(Set<String> pchannels, Map<TopicPartition, Long> storedOffsets) {
        String configured = normalizedConfiguredFormat();
        if (Collect.isNullOrEmpty(pchannels)) {
            return fallbackFormat(configured, "No pchannels configured for wire-format probing");
        }

        try (MilvusMessageConsumer consumer = messageConsumerFactory.create()) {
            String detected = null;
            for (String pchannel : pchannels) {
                String channelFormat = probeChannel(consumer, pchannel, storedOffsets);
                if (channelFormat == null && hasStoredOffset(pchannel, storedOffsets)) {
                    LOGGER.info("No non-timetick messages found on pchannel '{}' after the stored offset; "
                            + "re-probing from the earliest available message", pchannel);
                    channelFormat = probeChannel(consumer, pchannel, Map.of());
                }
                if (channelFormat == null) {
                    continue;
                }
                if (detected == null) {
                    detected = channelFormat;
                }
                else if (!detected.equals(channelFormat)) {
                    throw new MilvusWireFormatMismatchException(
                            detected, channelFormat, pchannel, config.getKafkaPartitionIndex(), -1L,
                            "Mixed wire formats detected across pchannels");
                }
            }

            if (detected == null) {
                return fallbackFormat(configured, "No non-timetick messages found during probe");
            }
            if (!"auto".equals(configured) && !configured.equals(detected)) {
                throw new MilvusWireFormatMismatchException(
                        configured, detected, "<probe>", config.getKafkaPartitionIndex(), -1L,
                        "Configured wire format does not match detected payload");
            }
            return detected;
        }
    }

    private static boolean hasStoredOffset(String pchannel, Map<TopicPartition, Long> storedOffsets) {
        return storedOffsets != null && storedOffsets.keySet().stream()
                .anyMatch(tp -> tp.topic().equals(pchannel));
    }

    private String probeChannel(MilvusMessageConsumer consumer, String pchannel,
                                Map<TopicPartition, Long> storedOffsets) {
        if (hasStoredOffset(pchannel, storedOffsets)) {
            consumer.assignAndSeek(storedOffsets);
        }
        else {
            consumer.assignAndSeek(Set.of(pchannel), SeekPosition.EARLIEST, null);
        }

        Instant deadline = Instant.now().plus(PROBE_TIMEOUT);
        RawMilvusMessage firstUnrecognized = null;
        int unrecognized = 0;
        while (Instant.now().isBefore(deadline)) {
            Duration remaining = Duration.between(Instant.now(), deadline);
            Duration timeout = remaining.isNegative() || remaining.isZero() ? Duration.ofMillis(1) : remaining;
            List<RawMilvusMessage> messages = consumer.poll(timeout);
            for (RawMilvusMessage message : messages) {
                PayloadKind kind = detectPayloadKind(message.getValue());
                if (kind == PayloadKind.TIMETICK) {
                    continue;
                }
                if (kind == PayloadKind.UNKNOWN) {
                    if (firstUnrecognized == null) {
                        firstUnrecognized = message;
                    }
                    unrecognized++;
                    continue;
                }
                if (unrecognized > 0) {
                    LOGGER.warn("Skipped {} unrecognizable message(s) on pchannel '{}' during wire-format probe"
                            + " (first at partition={}, offset={})", unrecognized, pchannel,
                            firstUnrecognized.getPartition(), firstUnrecognized.getOffset());
                }
                String format = kind == PayloadKind.MSGPACK_BATCH
                        ? MilvusProtoDeserializer.FORMAT_MSGPACK_BATCH
                        : MilvusProtoDeserializer.FORMAT_PROTO_SINGLE;
                validateConfiguredFormatIfExplicit(message, format);
                return format;
            }
        }
        if (firstUnrecognized != null) {
            throw new MilvusWireFormatMismatchException(
                    normalizedConfiguredFormat(), "unknown", firstUnrecognized.getTopic(),
                    firstUnrecognized.getPartition(), firstUnrecognized.getOffset(),
                    "Unrecognizable payload encountered during wire-format probe and no recognizable message"
                            + " followed within " + PROBE_TIMEOUT.toSeconds() + "s");
        }
        return null;
    }

    private void validateConfiguredFormatIfExplicit(RawMilvusMessage message, String detectedFormat) {
        String configured = normalizedConfiguredFormat();
        if (!"auto".equals(configured) && !configured.equals(detectedFormat)) {
            throw new MilvusWireFormatMismatchException(
                    configured, detectedFormat, message.getTopic(), message.getPartition(), message.getOffset(),
                    "Configured wire format does not match detected payload");
        }
    }

    private String fallbackFormat(String configuredFormat, String reason) {
        if (!"auto".equals(configuredFormat)) {
            LOGGER.info("{}; using explicitly configured wire format {}", reason, configuredFormat);
            return configuredFormat;
        }
        LOGGER.warn("{}; defaulting wire format to {}", reason, MilvusProtoDeserializer.FORMAT_MSGPACK_BATCH);
        return MilvusProtoDeserializer.FORMAT_MSGPACK_BATCH;
    }

    private String normalizedConfiguredFormat() {
        String configured = config.getWireFormat();
        return configured == null ? "auto" : configured.trim().toLowerCase();
    }

    private PayloadKind detectPayloadKind(byte[] data) {
        if (data == null || data.length == 0) {
            return PayloadKind.UNKNOWN;
        }
        if (isMsgPackTimeTick(data) || isProtoTimeTick(data)) {
            return PayloadKind.TIMETICK;
        }
        if (isMsgPackBatch(data)) {
            return PayloadKind.MSGPACK_BATCH;
        }
        if (isProtoSingle(data)) {
            return PayloadKind.PROTO_SINGLE;
        }
        return PayloadKind.UNKNOWN;
    }

    private boolean isMsgPackBatch(byte[] data) {
        try (org.msgpack.core.MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data)) {
            Value root = unpacker.unpackValue();
            if (!root.isArrayValue()) {
                return false;
            }
            ArrayValue array = root.asArrayValue();
            if (array.size() < 3) {
                return false;
            }
            Value first = array.get(0);
            Value second = array.get(1);
            Value third = array.get(2);
            if (!first.isIntegerValue() || !second.isStringValue() || !third.isArrayValue()) {
                return false;
            }
            for (Value value : third.asArrayValue()) {
                if (!value.isMapValue()) {
                    return false;
                }
            }
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private boolean isProtoSingle(byte[] data) {
        return isProtoInsert(data) || isProtoDelete(data) || isProtoCreateCollection(data)
                || isProtoDropCollection(data);
    }

    private boolean isProtoTimeTick(byte[] data) {
        try {
            TimeTickMsg message = TimeTickMsg.parseFrom(data);
            return message.hasBase() && message.getBase().getMsgType() == MsgType.TimeTick;
        }
        catch (Exception e) {
            return false;
        }
    }

    private boolean isMsgPackTimeTick(byte[] data) {
        try (org.msgpack.core.MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data)) {
            Value root = unpacker.unpackValue();
            if (!root.isArrayValue()) {
                return false;
            }
            ArrayValue outer = root.asArrayValue();
            if (outer.size() < 3 || !outer.get(2).isArrayValue()) {
                return false;
            }
            ArrayValue messages = outer.get(2).asArrayValue();
            if (messages.size() == 0) {
                return false;
            }
            for (Value message : messages) {
                if (!message.isMapValue() || !isMsgPackTimeTickMessage(message.asMapValue())) {
                    return false;
                }
            }
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private boolean isMsgPackTimeTickMessage(MapValue map) {
        Value msgType = map.map().get(org.msgpack.value.ValueFactory.newString("msgType"));
        return msgType != null
                && msgType.isIntegerValue()
                && msgType.asIntegerValue().toInt() == MsgType.TimeTick.getNumber();
    }

    private boolean isProtoInsert(byte[] data) {
        try {
            InsertRequest request = InsertRequest.parseFrom(data);
            return request.hasBase()
                    && request.getBase().getMsgType() == MsgType.Insert
                    && (request.getNumRows() > 0 || request.getFieldsDataCount() > 0);
        }
        catch (Exception e) {
            return false;
        }
    }

    private boolean isProtoDelete(byte[] data) {
        try {
            DeleteRequest request = DeleteRequest.parseFrom(data);
            return request.hasBase()
                    && request.getBase().getMsgType() == MsgType.Delete
                    && request.getInt64PrimaryKeysCount() > 0;
        }
        catch (Exception e) {
            return false;
        }
    }

    private boolean isProtoCreateCollection(byte[] data) {
        try {
            CreateCollectionRequest request = CreateCollectionRequest.parseFrom(data);
            return request.hasBase()
                    && request.getBase().getMsgType() == MsgType.CreateCollection
                    && !Strings.isNullOrEmpty(request.getCollectionName());
        }
        catch (Exception e) {
            return false;
        }
    }

    private boolean isProtoDropCollection(byte[] data) {
        try {
            DropCollectionRequest request = DropCollectionRequest.parseFrom(data);
            return request.hasBase()
                    && request.getBase().getMsgType() == MsgType.DropCollection
                    && !Strings.isNullOrEmpty(request.getCollectionName());
        }
        catch (Exception e) {
            return false;
        }
    }

    private enum PayloadKind {
        MSGPACK_BATCH,
        PROTO_SINGLE,
        TIMETICK,
        UNKNOWN
    }
}
