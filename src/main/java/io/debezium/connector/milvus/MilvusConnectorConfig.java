/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.Group;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Strings;

/**
 * Configuration definition for the Milvus Debezium connector.
 *
 * <p>
 * Extends {@link RelationalDatabaseConnectorConfig} to follow the same
 * pattern as PostgreSQL and MySQL connectors, enabling full use of the
 * relational schema infrastructure.
 * </p>
 */
public class MilvusConnectorConfig extends RelationalDatabaseConnectorConfig {

    private static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 0;

    public enum SnapshotMode implements EnumeratedValue {
        INITIAL("initial"),
        NEVER("never"),
        RECOVERY("recovery"),
        WHEN_NEEDED("when_needed");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static SnapshotMode parse(String value) {
            return parse(value, INITIAL);
        }

        public static SnapshotMode parse(String value, SnapshotMode defaultValue) {
            if (value == null) {
                return defaultValue;
            }
            for (SnapshotMode mode : values()) {
                if (mode.value.equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            return defaultValue;
        }
    }

    public static final Field MILVUS_URI = Field.create("milvus.uri")
            .withDisplayName("Milvus URI")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Milvus gRPC URI (e.g., http://localhost:19530). Required.")
            .withValidation(Field::isRequired);

    public static final Field MILVUS_TOKEN = Field.create("milvus.token")
            .withDisplayName("Milvus Token")
            .withType(Type.PASSWORD)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Authentication token for Milvus. Optional.");

    public static final Field MILVUS_DATABASE = Field.create("milvus.database")
            .withDisplayName("Milvus Database")
            .withType(Type.STRING)
            .withDefault("default")
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Milvus database name. Defaults to 'default'.");

    public static final Field COLLECTION_INCLUDE_LIST = Field.create("milvus.collection.include.list")
            .withDisplayName("Include list")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("A comma-separated list of regular expressions matching collection names whose change events "
                    + "should be captured. If not set, all collections are captured. "
                    + "Entries may be literal names or Java regular expressions (e.g. 'orders.*' to match every "
                    + "collection whose name begins with 'orders'). "
                    + "Cannot be used together with milvus.collection.exclude.list.");

    public static final Field COLLECTION_EXCLUDE_LIST = Field.create("milvus.collection.exclude.list")
            .withDisplayName("Exclude list")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("A comma-separated list of regular expressions matching collection names whose change events "
                    + "should be excluded. Entries may be literal names or Java regular expressions. "
                    + "Cannot be used together with milvus.collection.include.list.");

    public static final Field METADATA_TIMEOUT_MS = Field.create("milvus.metadata.timeout.ms")
            .withDisplayName("Metadata timeout (ms)")
            .withType(Type.LONG)
            .withDefault(5000L)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Timeout in milliseconds for Milvus metadata API calls.");

    public static final Field STARTUP_VALIDATION_ENABLED = Field.create("milvus.startup.validation.enabled")
            .withDisplayName("Startup validation enabled")
            .withType(Type.BOOLEAN)
            .withDefault(true)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether to perform startup validation. Defaults to true.");

    public static final Field ETCD_ENDPOINTS = Field.create("milvus.etcd.endpoints")
            .withDisplayName("etcd endpoints")
            .withType(Type.STRING)
            .withDefault("http://localhost:2379")
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Comma-separated list of etcd endpoints. Defaults to http://localhost:2379.");

    public static final Field ETCD_ROOT_PATH = Field.create("milvus.etcd.root.path")
            .withDisplayName("etcd root path")
            .withType(Type.STRING)
            .withDefault("by-dev")
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Root path prefix in etcd. Defaults to 'by-dev'.");

    public static final Field ETCD_CHECKPOINT_PATH = Field.create("milvus.etcd.checkpoint.path")
            .withDisplayName("etcd checkpoint path")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("Optional override for the etcd checkpoint key path. "
                    + "Must contain a '%s' placeholder for the pchannel name. "
                    + "If not set, the path '{milvus.etcd.root.path}/data-coord/checkpoint/binlog/channel/{pchannel}' is used.");

    public static final Field SNAPSHOT_MODE_FIELD = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withType(Type.STRING)
            .withDefault("initial")
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Snapshot mode: 'initial', 'never', 'recovery', or 'when_needed'. "
                    + "Defaults to 'initial'.");

    public static final Field KAFKA_BOOTSTRAP_SERVERS = Field.create("milvus.kafka.bootstrap.servers")
            .withDisplayName("Kafka bootstrap servers")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Kafka bootstrap servers used by Milvus as its MQ backend.");

    public static final Field KAFKA_CONSUMER_GROUP_ID = Field.create("milvus.kafka.consumer.group.id")
            .withDisplayName("Kafka consumer group id")
            .withType(Type.STRING)
            .withDefault("debezium-milvus")
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Kafka consumer group id used by the connector for manual channel assignment.");

    public static final Field KAFKA_MAX_POLL_INTERVAL_MS = Field.create("milvus.kafka.max.poll.interval.ms")
            .withDisplayName("Kafka max poll interval (ms)")
            .withType(Type.INT)
            .withDefault(300000)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Maximum time between polls before the consumer is considered dead. Default 300000.");

    public static final Field KAFKA_KEY_DESERIALIZER = Field.create("milvus.kafka.key.deserializer")
            .withDisplayName("Kafka key deserializer")
            .withType(Type.STRING)
            .withDefault("org.apache.kafka.common.serialization.ByteArrayDeserializer")
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Kafka key deserializer class for consuming Milvus MQ topics. "
                    + "Defaults to ByteArrayDeserializer.");

    public static final Field KAFKA_VALUE_DESERIALIZER = Field.create("milvus.kafka.value.deserializer")
            .withDisplayName("Kafka value deserializer")
            .withType(Type.STRING)
            .withDefault("org.apache.kafka.common.serialization.ByteArrayDeserializer")
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Kafka value deserializer class for consuming Milvus MQ topics. "
                    + "Defaults to ByteArrayDeserializer.");

    public static final Field KAFKA_PARTITION_INDEX = Field.create("milvus.kafka.partition.index")
            .withDisplayName("Kafka partition index")
            .withType(Type.INT)
            .withDefault(0)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Partition index for Milvus pchannels on Kafka. Default 0.");

    public static final Field WIRE_FORMAT = Field.create("milvus.wire.format")
            .withDisplayName("Wire format")
            .withType(Type.STRING)
            .withDefault("auto")
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Wire format: 'auto', 'msgpack_batch', or 'proto_single'.");

    public static final Field TIMETICK_STALL_TIMEOUT_MS = Field.create("milvus.timetick.stall.timeout.ms")
            .withDisplayName("Timetick stall timeout (ms)")
            .withType(Type.LONG)
            .withDefault(30000L)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Timeout before forcing a flush on timetick stall. Default 30000.");

    public static final Field UPSERT_MODE = Field.create("milvus.upsert.mode")
            .withDisplayName("Upsert mode")
            .withType(Type.STRING)
            .withDefault("passthrough")
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Upsert representation: 'passthrough' or 'correlate'.");

    public static final Field SNAPSHOT_BATCH_SIZE = Field.create("milvus.snapshot.batch.size")
            .withDisplayName("Snapshot batch size")
            .withType(Type.INT)
            .withDefault(1000)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Batch size for snapshot queries. Default 1000.");

    public static final Field BUFFER_MAX_EVENTS = Field.create("milvus.buffer.max.events")
            .withDisplayName("Buffer max events")
            .withType(Type.INT)
            .withDefault(10000)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Maximum number of events buffered by the timetick ordering engine "
                    + "before backpressure is applied. Default 10000.");

    public static final Field BUFFER_MAX_BYTES = Field.create("milvus.buffer.max.bytes")
            .withDisplayName("Buffer max bytes")
            .withType(Type.LONG)
            .withDefault(67108864L)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Maximum approximate bytes buffered by the timetick ordering engine "
                    + "before backpressure is applied. Default 67108864 (64 MB).");

    public static final Field PCHANNEL_NAME = Field.create("milvus.pchannel.name")
            .withDisplayName("Physical channel name")
            .withType(Type.STRING)
            .withDefault("by-dev-rootcoord-dml_0")
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("Milvus physical channel (pchannel) topic name. "
                    + "Defaults to 'by-dev-rootcoord-dml_0'.");

    /**
     * Override the parent's {@code heartbeat.action.query} field (inherited via
     * {@link RelationalDatabaseConnectorConfig}) to reject it outright.
     *
     * <p>The generic heartbeat action-query feature ({@code DatabaseHeartbeatImpl})
     * requires a {@code JdbcConnection} to run the query against. Milvus is not a
     * JDBC/relational data source and this connector wires no
     * {@code HeartbeatConnectionProvider}, so silently accepting this property would
     * fail with a {@code NullPointerException} at task startup instead of a clear
     * config error. Interval-based heartbeats ({@code heartbeat.interval.ms},
     * {@code heartbeat.topics.prefix}) remain fully supported.</p>
     */
    public static final Field HEARTBEAT_ACTION_QUERY = DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY
            .withValidation(MilvusConnectorConfig::validateHeartbeatActionQueryUnsupported);

    private static int validateHeartbeatActionQueryUnsupported(Configuration config, Field field, Field.ValidationOutput problems) {
        String value = config.getString(field);
        if (!Strings.isNullOrBlank(value)) {
            problems.accept(field, value, "heartbeat.action.query is not supported by the Milvus connector: "
                    + "Milvus is not a relational/JDBC data source, so there is no connection available "
                    + "to execute an action query against. Remove this property.");
            return 1;
        }
        return 0;
    }

    /**
     * Override the parent's {@code decimal.handling.mode} field to change the default
     * from {@code precise} to {@code double}.
     *
     * <p>Milvus {@code Float} and {@code Double} fields are IEEE-754 floating-point
     * values — not SQL DECIMAL/NUMERIC. Using {@code precise} (BigDecimal/bytes) as
     * the default would silently break consumers that expect native Java floats.
     * Users who truly need lossless decimal encoding can still set
     * {@code decimal.handling.mode=precise} explicitly.</p>
     */
    public static final Field DECIMAL_HANDLING_MODE_FIELD = RelationalDatabaseConnectorConfig.DECIMAL_HANDLING_MODE
            .withDefault(DecimalHandlingMode.DOUBLE.getValue());

    private final String milvusUri;
    private final String milvusToken;
    private final String milvusDatabase;
    private final List<String> collectionIncludeList;
    private final List<String> collectionExcludeList;
    private final long metadataTimeoutMs;
    private final boolean startupValidationEnabled;
    private final List<String> etcdEndpoints;
    private final String etcdRootPath;
    private final String etcdCheckpointPath;
    private final SnapshotMode snapshotMode;
    private final String kafkaBootstrapServers;
    private final String kafkaConsumerGroupId;
    private final int kafkaMaxPollIntervalMs;
    private final String kafkaKeyDeserializer;
    private final String kafkaValueDeserializer;
    private final int kafkaPartitionIndex;
    private final String wireFormat;
    private final long timetickStallTimeoutMs;
    private final String upsertMode;
    private final int snapshotBatchSize;
    private final int maxBufferedEvents;
    private final long maxBufferedBytes;
    private final int maxBatchSize;
    private final long pollIntervalMs;
    private final String pchannelName;

    public MilvusConnectorConfig(Configuration config) {
        super(
                config,
                new AllTablesFilter(),
                x -> x.table(),
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.SCHEMA,
                false);
        this.milvusUri = config.getString(MILVUS_URI);
        this.milvusToken = config.getString(MILVUS_TOKEN);
        this.milvusDatabase = config.getString(MILVUS_DATABASE);
        this.collectionIncludeList = Strings.listOfTrimmed(config.getString(COLLECTION_INCLUDE_LIST),
                Function.identity());
        this.collectionExcludeList = Strings.listOfTrimmed(config.getString(COLLECTION_EXCLUDE_LIST),
                Function.identity());
        this.metadataTimeoutMs = config.getLong(METADATA_TIMEOUT_MS);
        this.startupValidationEnabled = config.getBoolean(STARTUP_VALIDATION_ENABLED);
        this.etcdEndpoints = Strings.listOfTrimmed(config.getString(ETCD_ENDPOINTS),
                Function.identity());
        this.etcdRootPath = config.getString(ETCD_ROOT_PATH);
        this.etcdCheckpointPath = config.getString(ETCD_CHECKPOINT_PATH);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE_FIELD), SnapshotMode.INITIAL);
        this.kafkaBootstrapServers = config.getString(KAFKA_BOOTSTRAP_SERVERS);
        this.kafkaConsumerGroupId = config.getString(KAFKA_CONSUMER_GROUP_ID);
        this.kafkaMaxPollIntervalMs = config.getInteger(KAFKA_MAX_POLL_INTERVAL_MS);
        this.kafkaKeyDeserializer = config.getString(KAFKA_KEY_DESERIALIZER);
        this.kafkaValueDeserializer = config.getString(KAFKA_VALUE_DESERIALIZER);
        this.kafkaPartitionIndex = config.getInteger(KAFKA_PARTITION_INDEX);
        this.wireFormat = config.getString(WIRE_FORMAT);
        this.timetickStallTimeoutMs = config.getLong(TIMETICK_STALL_TIMEOUT_MS);
        this.upsertMode = config.getString(UPSERT_MODE);
        this.snapshotBatchSize = config.getInteger(SNAPSHOT_BATCH_SIZE);
        this.maxBufferedEvents = config.getInteger(BUFFER_MAX_EVENTS);
        this.maxBufferedBytes = config.getLong(BUFFER_MAX_BYTES);
        this.maxBatchSize = config.getInteger(CommonConnectorConfig.MAX_BATCH_SIZE);
        this.pollIntervalMs = config.getLong(CommonConnectorConfig.POLL_INTERVAL_MS);
        this.pchannelName = config.getString(PCHANNEL_NAME);
    }

    public String getMilvusUri() {
        return milvusUri;
    }

    public String getMilvusToken() {
        return milvusToken;
    }

    public String getMilvusDatabase() {
        return milvusDatabase;
    }

    public List<String> getCollectionIncludeList() {
        return collectionIncludeList;
    }

    public List<String> getCollectionExcludeList() {
        return collectionExcludeList;
    }

    public long getMetadataTimeoutMs() {
        return metadataTimeoutMs;
    }

    public boolean isStartupValidationEnabled() {
        return startupValidationEnabled;
    }

    public List<String> getEtcdEndpoints() {
        return etcdEndpoints;
    }

    public String getEtcdRootPath() {
        return etcdRootPath;
    }

    public String getEtcdCheckpointPath() {
        return etcdCheckpointPath;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaConsumerGroupId() {
        return kafkaConsumerGroupId;
    }

    public int getKafkaMaxPollIntervalMs() {
        return kafkaMaxPollIntervalMs;
    }

    public String getKafkaKeyDeserializer() {
        return kafkaKeyDeserializer;
    }

    public String getKafkaValueDeserializer() {
        return kafkaValueDeserializer;
    }

    public int getKafkaPartitionIndex() {
        return kafkaPartitionIndex;
    }

    public String getWireFormat() {
        return wireFormat;
    }

    public long getTimetickStallTimeoutMs() {
        return timetickStallTimeoutMs;
    }

    public String getUpsertMode() {
        return upsertMode;
    }

    public int getSnapshotBatchSize() {
        return snapshotBatchSize;
    }

    public int getMaxBufferedEvents() {
        return maxBufferedEvents;
    }

    public long getMaxBufferedBytes() {
        return maxBufferedBytes;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public String getPchannelName() {
        return pchannelName;
    }

    public String getLogicalName() {
        String prefix = getConfig().getString(TOPIC_PREFIX);
        return prefix != null ? prefix : "milvus";
    }

    @Override
    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    public Optional<EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    @Override
    public DecimalMode getDecimalMode() {
        return DecimalHandlingMode
                .parse(this.getConfig().getString(DECIMAL_HANDLING_MODE_FIELD),
                        DecimalHandlingMode.DOUBLE.getValue())
                .asDecimalMode();
    }

    @Override
    public String getContextName() {
        return Module.name();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        MilvusSourceInfoStructMaker maker = new MilvusSourceInfoStructMaker();
        maker.init(Module.name(), Module.version(), this);
        return maker;
    }

    @Override
    public TopicNamingStrategy getTopicNamingStrategy(Field field) {
        return super.getTopicNamingStrategy(field);
    }

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Milvus")
            .group(Group.CONNECTION,
                    MILVUS_URI,
                    MILVUS_TOKEN,
                    MILVUS_DATABASE,
                    ETCD_ENDPOINTS,
                    ETCD_ROOT_PATH,
                    ETCD_CHECKPOINT_PATH,
                    KAFKA_BOOTSTRAP_SERVERS,
                    KAFKA_CONSUMER_GROUP_ID,
                    KAFKA_MAX_POLL_INTERVAL_MS,
                    KAFKA_KEY_DESERIALIZER,
                    KAFKA_VALUE_DESERIALIZER,
                    KAFKA_PARTITION_INDEX)
            .group(Group.FILTERS,
                    COLLECTION_INCLUDE_LIST,
                    COLLECTION_EXCLUDE_LIST)
            .group(Group.CONNECTOR_SNAPSHOT,
                    SNAPSHOT_MODE_FIELD,
                    SNAPSHOT_BATCH_SIZE)
            .group(Group.CONNECTOR,
                    WIRE_FORMAT,
                    UPSERT_MODE,
                    PCHANNEL_NAME)
            .group(Group.CONNECTOR_ADVANCED,
                    METADATA_TIMEOUT_MS,
                    STARTUP_VALIDATION_ENABLED,
                    TIMETICK_STALL_TIMEOUT_MS,
                    BUFFER_MAX_EVENTS,
                    BUFFER_MAX_BYTES)
            .group(Group.ADVANCED_HEARTBEAT,
                    HEARTBEAT_ACTION_QUERY)
            // Milvus is not a JDBC datasource; remove the inherited relational fields
            // that are marked required() so that BaseSourceTask field validation
            // does not fail with "A value is required".
            .excluding(
                    RelationalDatabaseConnectorConfig.HOSTNAME,
                    RelationalDatabaseConnectorConfig.PORT,
                    RelationalDatabaseConnectorConfig.USER,
                    RelationalDatabaseConnectorConfig.PASSWORD,
                    RelationalDatabaseConnectorConfig.DATABASE_NAME)
            .create();

    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * Simple table filter that accepts all tables (Milvus has no system schemas
     * to exclude at the relational level).
     */
    private static class AllTablesFilter implements TableFilter {
        @Override
        public boolean isIncluded(TableId t) {
            return true;
        }
    }
}
