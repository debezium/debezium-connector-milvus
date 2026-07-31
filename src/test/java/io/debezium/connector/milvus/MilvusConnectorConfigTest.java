/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;

public class MilvusConnectorConfigTest {

    private static final String TEST_URI = "http://localhost:19530";
    private static final String TEST_TOPIC_PREFIX = "milvus-test";

    private Map<String, String> baseConfig() {
        return Map.of(
                "milvus.uri", TEST_URI,
                "topic.prefix", TEST_TOPIC_PREFIX);
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldLoadRequiredConfig() {
        Configuration config = Configuration.from(baseConfig());
        MilvusConnectorConfig connectorConfig = new MilvusConnectorConfig(config);

        assertThat(connectorConfig.getMilvusUri()).isEqualTo(TEST_URI);
        assertThat(connectorConfig.getLogicalName()).isEqualTo(TEST_TOPIC_PREFIX);
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldApplyDefaults() {
        Configuration config = Configuration.from(baseConfig());
        MilvusConnectorConfig connectorConfig = new MilvusConnectorConfig(config);

        assertThat(connectorConfig.getMilvusDatabase()).isEqualTo("default");
        assertThat(connectorConfig.getMetadataTimeoutMs()).isEqualTo(5000L);
        assertThat(connectorConfig.isStartupValidationEnabled()).isTrue();
        assertThat(connectorConfig.getEtcdRootPath()).isEqualTo("by-dev");
        assertThat(connectorConfig.getEtcdCheckpointPath()).isNull();
        assertThat(connectorConfig.getSnapshotMode().getValue()).isEqualTo("initial");
        assertThat(connectorConfig.getWireFormat()).isEqualTo("auto");
        assertThat(connectorConfig.getTimetickStallTimeoutMs()).isEqualTo(30000L);
        assertThat(connectorConfig.getUpsertMode()).isEqualTo("passthrough");
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldParseOverrideConfig() {
        Map<String, String> props = new HashMap<>(baseConfig());
        props.put("milvus.database", "testdb");
        props.put("milvus.collection.include.list", "articles,products");
        props.put("milvus.metadata.timeout.ms", "10000");
        props.put("milvus.startup.validation.enabled", "false");
        props.put("milvus.etcd.root.path", "custom-root");
        props.put("milvus.etcd.checkpoint.path", "custom/checkpoint/%s");
        props.put("snapshot.mode", "never");
        props.put("milvus.kafka.bootstrap.servers", "kafka:9092");
        props.put("milvus.wire.format", "msgpack_batch");
        props.put("milvus.timetick.stall.timeout.ms", "60000");
        props.put("milvus.upsert.mode", "correlate");

        Configuration config = Configuration.from(props);
        MilvusConnectorConfig connectorConfig = new MilvusConnectorConfig(config);

        assertThat(connectorConfig.getMilvusDatabase()).isEqualTo("testdb");
        assertThat(connectorConfig.getCollectionIncludeList()).containsExactly("articles", "products");
        assertThat(connectorConfig.getMetadataTimeoutMs()).isEqualTo(10000L);
        assertThat(connectorConfig.isStartupValidationEnabled()).isFalse();
        assertThat(connectorConfig.getEtcdRootPath()).isEqualTo("custom-root");
        assertThat(connectorConfig.getEtcdCheckpointPath()).isEqualTo("custom/checkpoint/%s");
        assertThat(connectorConfig.getSnapshotMode().getValue()).isEqualTo("never");
        assertThat(connectorConfig.getKafkaBootstrapServers()).isEqualTo("kafka:9092");
        assertThat(connectorConfig.getWireFormat()).isEqualTo("msgpack_batch");
        assertThat(connectorConfig.getTimetickStallTimeoutMs()).isEqualTo(60000L);
        assertThat(connectorConfig.getUpsertMode()).isEqualTo("correlate");
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldExposeConfigDef() {
        assertThatNoException().isThrownBy(() -> {
            ConfigDef configDef = MilvusConnectorConfig.configDef();
            assertThat(configDef).isNotNull();
            assertThat(configDef.names()).contains("milvus.uri", "topic.prefix", "milvus.database", "milvus.etcd.checkpoint.path");
        });
    }

    @Test
    @FixFor("debezium/dbz#2328")
    void shouldExposeStandardHeartbeatFields() {
        ConfigDef configDef = MilvusConnectorConfig.configDef();

        assertThat(configDef.names()).contains("heartbeat.interval.ms", "heartbeat.topics.prefix");
    }

    @Test
    @FixFor("debezium/dbz#2328")
    void shouldApplyHeartbeatIntervalWhenConfigured() {
        Map<String, String> props = new HashMap<>(baseConfig());
        props.put("heartbeat.interval.ms", "5000");

        Configuration config = Configuration.from(props);
        MilvusConnectorConfig connectorConfig = new MilvusConnectorConfig(config);

        assertThat(connectorConfig.getHeartbeatInterval()).isEqualTo(Duration.ofMillis(5000));
    }

    @Test
    @FixFor("debezium/dbz#2328")
    void shouldRejectHeartbeatActionQuery() {
        Map<String, String> props = new HashMap<>(baseConfig());
        props.put("heartbeat.action.query", "SELECT 1");
        Configuration config = Configuration.from(props);

        List<String> problems = new ArrayList<>();
        boolean valid = config.validateAndRecord(MilvusConnectorConfig.ALL_FIELDS, problems::add);

        assertThat(valid).isFalse();
        assertThat(problems).anyMatch(p -> p.contains("heartbeat.action.query"));
    }

    @Test
    @FixFor("debezium/dbz#2328")
    void shouldValidateSuccessfullyWithoutHeartbeatActionQuery() {
        Configuration config = Configuration.from(baseConfig());

        List<String> problems = new ArrayList<>();
        boolean valid = config.validateAndRecord(MilvusConnectorConfig.ALL_FIELDS, problems::add);

        assertThat(valid).isTrue();
        assertThat(problems).isEmpty();
    }
}
