package io.confluent.kafkarest;

import static io.confluent.kafkarest.KafkaRestConfig.METRICS_REPORTER_CLASSES_CONFIG;
import static io.confluent.kafkarest.KafkaRestMetricsContext.KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT;
import static io.confluent.kafkarest.KafkaRestMetricsContext.RESOURCE_LABEL_CLUSTER_ID;
import static io.confluent.kafkarest.KafkaRestMetricsContext.RESOURCE_LABEL_COMMIT_ID;
import static io.confluent.kafkarest.KafkaRestMetricsContext.RESOURCE_LABEL_VERSION;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.rest.metrics.RestMetricsContext;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Test;

public class KafkaRestConfigTest {

  @Test
  public void getMetricsContext_propagateMetricsConfigs() {
    Properties properties = new Properties();
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "context_cluster_id");

    RestMetricsContext config = new KafkaRestConfig(properties).getMetricsContext();

    assertEquals(AppInfoParser.getCommitId(), config.getLabel(RESOURCE_LABEL_COMMIT_ID));
    assertEquals(AppInfoParser.getVersion(), config.getLabel(RESOURCE_LABEL_VERSION));
    assertEquals("context_cluster_id", config.getLabel(RESOURCE_LABEL_CLUSTER_ID));
  }

  @Test
  public void getProducerProperties_propagateMetrics_defaultClusterId() {
    Properties properties = new Properties();

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();

    assertEquals(
        KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT,
        producerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @Test
  public void getProducerProperties_propagateMetricsProperties() {
    Properties properties = new Properties();
    properties.put(
        METRICS_REPORTER_CLASSES_CONFIG,
        "metrics.reporter.1, metrics.reporter.2, metrics.reporter.3");
    properties.put(reporter_config("api.key"), "my_api_key");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "producer_cluster_id");

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();
    assertEquals(
        Arrays.asList("metrics.reporter.1", "metrics.reporter.2", "metrics.reporter.3"),
        producerProperties.get(METRICS_REPORTER_CLASSES_CONFIG));
    assertEquals("my_api_key", producerProperties.get(reporter_config("api.key")));
    assertEquals(
        AppInfoParser.getCommitId(),
        producerProperties.get(context_config(RESOURCE_LABEL_COMMIT_ID)));
    assertEquals(
        AppInfoParser.getVersion(), producerProperties.get(context_config(RESOURCE_LABEL_VERSION)));
    assertEquals(
        "producer_cluster_id", producerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @Test
  public void getConsumerProperties_propagatesSchemaRegistryProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "https://schemaregistry:8085");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties consumerProperties = config.getConsumerProperties();
    assertEquals("https://schemaregistry:8085", consumerProperties.get("schema.registry.url"));
  }

  @Test
  public void getConsumerProperties_propagateMetrics_defaultClusterId() {
    Properties properties = new Properties();

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties consumerProperties = config.getConsumerProperties();

    assertEquals(
        KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT,
        consumerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @Test
  public void getConsumerProperties_propagateMetricsProperties() {
    Properties properties = new Properties();
    properties.put(
        METRICS_REPORTER_CLASSES_CONFIG,
        "metrics.reporter.1, metrics.reporter.2, metrics.reporter.3");
    properties.put(reporter_config("api.key"), "my_api_key");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "consumer_cluster_id");

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties consumerProperties = config.getConsumerProperties();
    assertEquals(
        Arrays.asList("metrics.reporter.1", "metrics.reporter.2", "metrics.reporter.3"),
        consumerProperties.get(METRICS_REPORTER_CLASSES_CONFIG));
    assertEquals("my_api_key", consumerProperties.get("confluent.telemetry.api.key"));
    assertEquals(
        AppInfoParser.getCommitId(),
        consumerProperties.get(context_config(RESOURCE_LABEL_COMMIT_ID)));
    assertEquals(
        AppInfoParser.getVersion(), consumerProperties.get(context_config(RESOURCE_LABEL_VERSION)));
    assertEquals(
        "consumer_cluster_id", consumerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @Test
  public void getAdminProperties_propagateMetrics_defaultClusterId() {
    Properties properties = new Properties();

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties adminProperties = config.getAdminProperties();

    assertEquals(
        KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT,
        adminProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @Test
  public void getAdminProperties_propagateMetricsProperties() {
    Properties properties = new Properties();
    properties.put(
        METRICS_REPORTER_CLASSES_CONFIG,
        "metrics.reporter.1, metrics.reporter.2, metrics.reporter.3");
    properties.put(reporter_config("api.key"), "my_api_key");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "admin_cluster_id");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties adminProperties = config.getAdminProperties();
    assertEquals(
        Arrays.asList("metrics.reporter.1", "metrics.reporter.2", "metrics.reporter.3"),
        adminProperties.get(METRICS_REPORTER_CLASSES_CONFIG));
    assertEquals("my_api_key", adminProperties.get("confluent.telemetry.api.key"));
    assertEquals(
        AppInfoParser.getCommitId(), adminProperties.get(context_config(RESOURCE_LABEL_COMMIT_ID)));
    assertEquals(
        AppInfoParser.getVersion(), adminProperties.get(context_config(RESOURCE_LABEL_VERSION)));
    assertEquals(
        "admin_cluster_id", adminProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @Test
  public void getConsumerProperties_propagatesSchemaRegistryConfigs() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getConsumerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("basic.auth.user.info"));
  }

  @Test
  public void getConsumerProperties_propagatesClientSchemaRegistryConfigs() {
    Properties properties = new Properties();
    properties.put("client.schema.registry.url", "foobar");
    properties.put("client.schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getConsumerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @Test
  public void getConsumerProperties_propagatesConsumerSchemaRegistryConfigs() {
    Properties properties = new Properties();
    properties.put("consumer.schema.registry.url", "foobar");
    properties.put("consumer.schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getConsumerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @Test
  public void getProducerProperties_propagatesSchemaRegistryConfigs() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @Test
  public void getProducerProperties_propagatesClientSchemaRegistryConfigs() {
    Properties properties = new Properties();
    properties.put("client.schema.registry.url", "foobar");
    properties.put("client.schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @Test
  public void getProducerProperties_propagatesProducerSchemaRegistryConfigs() {
    Properties properties = new Properties();
    properties.put("producer.schema.registry.url", "foobar");
    properties.put("producer.schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @Test
  public void getJsonSerializerConfigs() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("producer.acks", 1);
    properties.put("producer.json.indent.output", true);
    KafkaRestConfig config = new KafkaRestConfig(properties);

    assertEquals(singletonMap("json.indent.output", true), config.getJsonSerializerConfigs());
  }

  @Test
  public void getAvroSerializerConfigs() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("producer.acks", 1);
    KafkaRestConfig config = new KafkaRestConfig(properties);

    assertEquals(
        ImmutableMap.of(
            "schema.registry.url", "foobar",
            "auto.register.schemas", false,
            "use.latest.version", false),
        config.getAvroSerializerConfigs());
  }

  @Test
  public void getJsonschemaSerializerConfigs() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("client.json.fail.invalid.schema", true);
    properties.put("producer.acks", 1);
    properties.put("producer.json.fail.invalid.schema", true);
    KafkaRestConfig config = new KafkaRestConfig(properties);

    assertEquals(
        ImmutableMap.of(
            "schema.registry.url", "foobar",
            "auto.register.schemas", false,
            "use.latest.version", false,
            "json.fail.invalid.schema", true),
        config.getJsonschemaSerializerConfigs());
  }

  @Test
  public void getProtobufSerializerConfigs() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("producer.acks", 1);
    properties.put(
        "producer.reference.subject.name.strategy", DefaultReferenceSubjectNameStrategy.class);
    KafkaRestConfig config = new KafkaRestConfig(properties);

    assertEquals(
        ImmutableMap.of(
            "schema.registry.url",
            "foobar",
            "auto.register.schemas",
            false,
            "use.latest.version",
            false,
            "reference.subject.name.strategy",
            DefaultReferenceSubjectNameStrategy.class),
        config.getProtobufSerializerConfigs());
  }

  private String context_config(String suffix) {
    return CommonClientConfigs.METRICS_CONTEXT_PREFIX + suffix;
  }

  private String reporter_config(String suffix) {
    return KafkaRestConfig.TELEMETRY_PREFIX + suffix;
  }
}
