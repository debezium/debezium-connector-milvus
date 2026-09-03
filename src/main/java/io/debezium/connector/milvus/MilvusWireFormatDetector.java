/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.time.Instant;
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

import io.debezium.connector.milvus.MilvusConnectorConfig.WireFormat;
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
 * Detection probes each configured pchannel and inspects the first payload that
 * is neither a timetick nor unrecognizable. On a warm restart the probe seeks to
 * the stored MQ offset, so the first message it sees is the last one already
 * processed (streaming itself resumes one past it); that message is guaranteed
 * to reflect the format in use, unlike potentially pre-upgrade payloads at the
 * head of the topic. When nothing recognizable follows the stored offset within
 * {@link #PROBE_TIMEOUT}, the pchannel is re-probed from {@code EARLIEST}. The
 * format is expected to be stable cluster-wide, so mixed detections are treated
 * as fatal.
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
     * Prefer {@link #detect(Set, Map)} on warm restart so the probe reads the
     * last already-processed message rather than potentially pre-upgrade
     * payloads at the head of the topic.
     * </p>
     */
    public WireFormat detect(Set<String> pchannels) {
        return detect(pchannels, Map.of());
    }

    /**
     * Probes the given pchannels and returns the cluster wire format.
     *
     * <p>
     * When a stored offset exists for a pchannel the probe seeks to it, so the
     * first message inspected on a warm restart is the last one already
     * processed (streaming resumes one past it). If that probe finds no
     * recognizable data message, whether because an idle pchannel carries
     * nothing but timeticks after the stored offset or because only
     * unrecognizable payloads follow it, the pchannel is re-probed from
     * {@code EARLIEST}. Unrecognizable payloads are only fatal when no probe
     * finds a recognizable message; the exception then points at the first
     * unrecognizable payload after the stored offset when there is one, since
     * that is where streaming will run into it.
     * </p>
     *
     * @param pchannels     pchannels to probe; null or empty returns the fallback
     * @param storedOffsets last-committed Kafka offsets keyed by
     *                      {@link TopicPartition};
     *                      empty map falls back to {@code EARLIEST}
     * @return the detected format, never {@link WireFormat#AUTO}
     */
    public WireFormat detect(Set<String> pchannels, Map<TopicPartition, Long> storedOffsets) {
        WireFormat configured = config.getWireFormat();
        if (Collect.isNullOrEmpty(pchannels)) {
            return fallbackFormat(configured, "No pchannels configured for wire-format probing");
        }

        try (MilvusMessageConsumer consumer = messageConsumerFactory.create()) {
            WireFormat detected = null;
            for (String pchannel : pchannels) {
                WireFormat channelFormat = probePchannel(consumer, pchannel, storedOffsets);
                if (channelFormat == null) {
                    continue;
                }
                if (detected == null) {
                    detected = channelFormat;
                }
                else if (detected != channelFormat) {
                    throw new MilvusWireFormatMismatchException(
                            detected.getValue(), channelFormat.getValue(), pchannel, config.getKafkaPartitionIndex(),
                            -1L, "Mixed wire formats detected across pchannels");
                }
            }

            if (detected == null) {
                return fallbackFormat(configured, "No data messages found during probe");
            }
            if (configured != WireFormat.AUTO && configured != detected) {
                throw new MilvusWireFormatMismatchException(
                        configured.getValue(), detected.getValue(), "<probe>", config.getKafkaPartitionIndex(), -1L,
                        "Configured wire format does not match detected payload");
            }
            return detected;
        }
    }

    /**
     * Probes a single pchannel, starting from its stored offset when one exists
     * and re-probing from {@code EARLIEST} when that first pass finds no
     * recognizable data message.
     */
    private WireFormat probePchannel(MilvusMessageConsumer consumer, String pchannel,
                                     Map<TopicPartition, Long> storedOffsets) {
        ProbeResult result = probe(consumer, pchannel, storedOffsets);
        if (result.format() == null && hasStoredOffset(pchannel, storedOffsets)) {
            LOGGER.info("No recognizable data message found on pchannel '{}' after the stored offset ({}); "
                    + "re-probing from the earliest available message", pchannel,
                    result.sawUnrecognized()
                            ? "only timeticks and " + result.unrecognized() + " unrecognizable payload(s)"
                            : "only timeticks");
            ProbeResult earliest = probe(consumer, pchannel, Map.of());
            if (earliest.format() != null) {
                warnSkippedUnrecognized(pchannel, result);
                warnSkippedUnrecognized(pchannel, earliest);
                return earliest.format();
            }
            if (!result.sawUnrecognized()) {
                result = earliest;
            }
        }
        if (result.format() != null) {
            warnSkippedUnrecognized(pchannel, result);
            return result.format();
        }
        if (result.sawUnrecognized()) {
            RawMilvusMessage first = result.firstUnrecognized();
            throw new MilvusWireFormatMismatchException(
                    config.getWireFormat().getValue(), "unknown", first.getTopic(), first.getPartition(), first.getOffset(),
                    "Unrecognizable payload encountered during wire-format probe and no recognizable message"
                            + " was found within " + PROBE_TIMEOUT.toSeconds() + "s");
        }
        return null;
    }

    private static boolean hasStoredOffset(String pchannel, Map<TopicPartition, Long> storedOffsets) {
        return storedOffsets != null && storedOffsets.keySet().stream()
                .anyMatch(tp -> tp.topic().equals(pchannel));
    }

    /**
     * Runs one probe pass over a pchannel, from the stored offset when
     * {@code storedOffsets} carries one for it and from {@code EARLIEST}
     * otherwise, and stops at the first recognizable data message.
     */
    private ProbeResult probe(MilvusMessageConsumer consumer, String pchannel,
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
            for (RawMilvusMessage message : consumer.poll(timeout)) {
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
                WireFormat format = kind == PayloadKind.MSGPACK_BATCH
                        ? WireFormat.MSGPACK_BATCH
                        : WireFormat.PROTO_SINGLE;
                validateConfiguredFormatIfExplicit(message, format);
                return new ProbeResult(format, firstUnrecognized, unrecognized);
            }
        }
        return new ProbeResult(null, firstUnrecognized, unrecognized);
    }

    private static void warnSkippedUnrecognized(String pchannel, ProbeResult result) {
        if (result.sawUnrecognized()) {
            LOGGER.warn("Skipped {} unrecognizable message(s) on pchannel '{}' during wire-format probe"
                    + " (first at partition={}, offset={})", result.unrecognized(), pchannel,
                    result.firstUnrecognized().getPartition(), result.firstUnrecognized().getOffset());
        }
    }

    private void validateConfiguredFormatIfExplicit(RawMilvusMessage message, WireFormat detectedFormat) {
        WireFormat configured = config.getWireFormat();
        if (configured != WireFormat.AUTO && configured != detectedFormat) {
            throw new MilvusWireFormatMismatchException(
                    configured.getValue(), detectedFormat.getValue(), message.getTopic(), message.getPartition(),
                    message.getOffset(), "Configured wire format does not match detected payload");
        }
    }

    private WireFormat fallbackFormat(WireFormat configuredFormat, String reason) {
        if (configuredFormat != WireFormat.AUTO) {
            LOGGER.info("{}; using explicitly configured wire format {}", reason, configuredFormat.getValue());
            return configuredFormat;
        }
        LOGGER.warn("{}; defaulting wire format to {}", reason, WireFormat.MSGPACK_BATCH.getValue());
        return WireFormat.MSGPACK_BATCH;
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

    /**
     * Outcome of one probe pass over a pchannel.
     *
     * @param format            the format of the first recognizable data message,
     *                          or {@code null} when the pass saw none
     * @param firstUnrecognized the first unrecognizable payload the pass skipped,
     *                          or {@code null} when it skipped none
     * @param unrecognized      how many unrecognizable payloads the pass skipped
     */
    private record ProbeResult(WireFormat format, RawMilvusMessage firstUnrecognized, int unrecognized) {

        boolean sawUnrecognized() {
            return firstUnrecognized != null;
        }
    }
}
