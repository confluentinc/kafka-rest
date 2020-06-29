package io.confluent.kafkarest;

import static io.confluent.kafkarest.KafkaRestMetricsContext.*;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import io.confluent.rest.metrics.RestMetricsContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.Test;

public class KafkaRestConfigTest {

  @Test
  public void getMetricsContext_propagateMetricsConfigs() {
    Properties properties = new Properties();
    properties.put(telemetry_config("bootstrap.servers"), "broker1:9092");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "context_cluster_id");

    RestMetricsContext config = new KafkaRestConfig(properties).getMetricsContext();

    assertEquals(AppInfoParser.getCommitId(), config.getLabel(RESOURCE_LABEL_COMMIT_ID));
    assertEquals(AppInfoParser.getVersion(), config.getLabel(RESOURCE_LABEL_VERSION));
    assertEquals("context_cluster_id", config.getLabel(RESOURCE_LABEL_CLUSTER_ID));
  }

  @Test
  public void getProducerProperties_propagatesSchemaRegistryProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "https://schemaregistry:8085");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("https://schemaregistry:8085",
            producerProperties.get("schema.registry.url"));
  }


  @Test
  public void getProducerProperties_propagateMetrics_defaultClusterId() {
    Properties properties = new Properties();

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();

    assertEquals(KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT,
            producerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }


  @Test
  public void getProducerProperties_propagateMetricsProperties() {
    Properties properties = new Properties();
    properties.put(telemetry_config("bootstrap.servers"), "broker1:9092");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "producer_cluster_id");

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("broker1:9092",
            producerProperties.get(telemetry_config("bootstrap.servers")));
    assertEquals(AppInfoParser.getCommitId(),
            producerProperties.get(context_config(RESOURCE_LABEL_COMMIT_ID)));
    assertEquals(AppInfoParser.getVersion(),
            producerProperties.get(context_config(RESOURCE_LABEL_VERSION)));
    assertEquals("producer_cluster_id",
            producerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }


  @Test
  public void getConsumerProperties_propagatesSchemaRegistryProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "https://schemaregistry:8085");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties consumerProperties = config.getConsumerProperties();
    assertEquals("https://schemaregistry:8085",
            consumerProperties.get("schema.registry.url"));
  }

  @Test
  public void getConsumerProperties_propagateMetrics_defaultClusterId() {
    Properties properties = new Properties();

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties consumerProperties = config.getConsumerProperties();

    assertEquals(KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT,
            consumerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @Test
  public void getConsumerProperties_propagateMetricsProperties() {
    Properties properties = new Properties();
    properties.put(telemetry_config("bootstrap.servers"), "broker1:9092");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "consumer_cluster_id");

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties consumerProperties = config.getConsumerProperties();
    assertEquals("broker1:9092",
            consumerProperties.get(telemetry_config("bootstrap.servers")));
    assertEquals(AppInfoParser.getCommitId(),
            consumerProperties.get(context_config(RESOURCE_LABEL_COMMIT_ID)));
    assertEquals(AppInfoParser.getVersion(),
            consumerProperties.get(context_config(RESOURCE_LABEL_VERSION)));
    assertEquals("consumer_cluster_id",
            consumerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @Test
  public void getAdminProperties_propagateMetrics_defaultClusterId() {
    Properties properties = new Properties();

    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties adminProperties = config.getAdminProperties();

    assertEquals(KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT,
            adminProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @Test
  public void getAdminProperties_propagateMetricsProperties() {
    Properties properties = new Properties();
    properties.put(telemetry_config("bootstrap.servers"), "broker1:9092");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "admin_cluster_id");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties adminProperties = config.getAdminProperties();
    assertEquals("broker1:9092",
            adminProperties.get(telemetry_config("bootstrap.servers")));
    assertEquals(AppInfoParser.getCommitId(),
            adminProperties.get(context_config(RESOURCE_LABEL_COMMIT_ID)));
    assertEquals(AppInfoParser.getVersion(),
            adminProperties.get(context_config(RESOURCE_LABEL_VERSION)));
    assertEquals("admin_cluster_id",
            adminProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  private String telemetry_config(String suffix) {
    return KafkaRestConfig.TELEMETRY_PREFIX + suffix;
  }

  private String context_config(String suffix) {
    return CommonClientConfigs.METRICS_CONTEXT_PREFIX + suffix;
  }

}
