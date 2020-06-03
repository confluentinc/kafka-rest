package io.confluent.kafkarest;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.Test;

public class KafkaRestConfigTest {

  @Test
  public void getProducerProperties_propagatesSchemaRegistryProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "https://schemaregistry:8085");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("https://schemaregistry:8085", producerProperties.get("schema.registry.url"));
  }


  @Test
  public void getProducerProperties_propagateMetricsProperties() {
    Properties properties = new Properties();
    properties.put("confluent.telemetry.bootstrap.servers", "broker1:9092");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("broker1:9092", producerProperties.get("confluent.telemetry.bootstrap.servers"));
    assertEquals(AppInfoParser.getCommitId(), producerProperties.get("metrics.context.resource.commit.id"));
    assertEquals(AppInfoParser.getVersion(), producerProperties.get("metrics.context.resource.version"));
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
  public void getConsumerProperties_propagateMetricsProperties() {
    Properties properties = new Properties();
    properties.put("confluent.telemetry.bootstrap.servers", "broker1:9092");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties consumerProperties = config.getConsumerProperties();
    assertEquals("broker1:9092", consumerProperties.get("confluent.telemetry.bootstrap.servers"));
    assertEquals(AppInfoParser.getCommitId(), consumerProperties.get("metrics.context.resource.commit.id"));
    assertEquals(AppInfoParser.getVersion(), consumerProperties.get("metrics.context.resource.version"));
  }

  @Test
  public void getAdminProperties_propagateMetricsProperties() {
    Properties properties = new Properties();
    properties.put("confluent.telemetry.bootstrap.servers", "broker1:9092");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties adminProperties = config.getAdminProperties();
    assertEquals("broker1:9092", adminProperties.get("confluent.telemetry.bootstrap.servers"));
    assertEquals(AppInfoParser.getCommitId(), adminProperties.get("metrics.context.resource.commit.id"));
    assertEquals(AppInfoParser.getVersion(), adminProperties.get("metrics.context.resource.version"));
  }
}
