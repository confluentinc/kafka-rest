package io.confluent.kafkarest;

import static org.junit.Assert.assertEquals;

import io.confluent.rest.RestConfigException;
import java.util.Properties;
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
  public void getConsumerProperties_propagatesSchemaRegistryProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "https://schemaregistry:8085");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    Properties consumerProperties = config.getConsumerProperties();
    assertEquals("https://schemaregistry:8085", consumerProperties.get("schema.registry.url"));
  }
}
