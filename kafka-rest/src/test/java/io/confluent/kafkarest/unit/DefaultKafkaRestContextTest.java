package io.confluent.kafkarest.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class DefaultKafkaRestContextTest {

  private KafkaRestContext context;

  @Test
  public void testGetKafkaConsumerManagerThreadSafety() throws InterruptedException {

    Properties props = new Properties();
    // Required to satisfy config definition
    props.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5234");
    KafkaRestConfig restConfig = new KafkaRestConfig(props);

    context = new DefaultKafkaRestContext(restConfig);

    Set<Object> refs = new CopyOnWriteArraySet<>();

    ExecutorService executor = Executors.newFixedThreadPool(100);
    // Captures reference as it's invoked.
    for (int i = 0; i < 100; i++) {
      executor.submit(() -> refs.add(context.getKafkaConsumerManager()));
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

    assertEquals(1, refs.size());
  }

  @Test
  public void testGetSchemaRegistryClientEnabled() {
    Properties props = new Properties();
    props.put(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost:5234");
    KafkaRestConfig restConfig = new KafkaRestConfig(props);
    context = new DefaultKafkaRestContext(restConfig);
    assertNotNull(context.getSchemaRegistryClient());
  }

  @Test
  public void testGetSchemaRegistryClientDisabled() {
    Properties props = new Properties();
    props.put(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG, "");
    KafkaRestConfig restConfig = new KafkaRestConfig(props);
    context = new DefaultKafkaRestContext(restConfig);
    assertEquals(context.getSchemaRegistryClient(), null);
  }
}
