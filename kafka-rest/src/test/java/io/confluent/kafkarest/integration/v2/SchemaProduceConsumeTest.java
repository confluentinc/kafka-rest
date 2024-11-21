package io.confluent.kafkarest.integration.v2;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceRequest;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import io.confluent.kafkarest.entities.v2.SchemaConsumerRecord;
import io.confluent.kafkarest.entities.v2.SchemaTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.SchemaTopicProduceRequest.SchemaTopicProduceRecord;
import io.confluent.kafkarest.testing.DefaultKafkaRestTestEnvironment;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Tag("IntegrationTest")
public abstract class SchemaProduceConsumeTest {

  private static final String TOPIC = "topic-1";

  private static final String CONSUMER_GROUP = "group-1";

  @RegisterExtension
  public final DefaultKafkaRestTestEnvironment testEnv = new DefaultKafkaRestTestEnvironment();

  protected abstract EmbeddedFormat getFormat();

  protected abstract String getContentType();

  protected abstract ParsedSchema getKeySchema();

  protected abstract ParsedSchema getValueSchema();

  protected abstract List<SchemaTopicProduceRecord> getProduceRecords();

  @Test
  public void produceThenConsume_returnsExactlyProduced() throws Exception {
    testEnv
        .kafkaCluster()
        .createTopic(TOPIC, /* numPartitions= */ 1, /* replicationFactor= */ (short) 3);

    Response createConsumerInstanceResponse =
        testEnv
            .kafkaRest()
            .target()
            .path(String.format("/consumers/%s", CONSUMER_GROUP))
            .request()
            .post(
                Entity.entity(
                    new CreateConsumerInstanceRequest(
                        /* id= */ null,
                        /* name= */ null,
                        getFormat() != null ? getFormat().name() : null,
                        /* autoOffsetReset= */ null,
                        /* autoCommitEnable= */ null,
                        /* responseMinBytes= */ null,
                        /* requestWaitMs= */ null),
                    Versions.KAFKA_V2_JSON));

    assertEquals(Status.OK.getStatusCode(), createConsumerInstanceResponse.getStatus());

    CreateConsumerInstanceResponse createConsumerInstance =
        createConsumerInstanceResponse.readEntity(CreateConsumerInstanceResponse.class);

    Response subscribeResponse =
        testEnv
            .kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/subscription",
                    CONSUMER_GROUP, createConsumerInstance.getInstanceId()))
            .request()
            .post(
                Entity.entity(
                    new ConsumerSubscriptionRecord(singletonList(TOPIC), null),
                    Versions.KAFKA_V2_JSON));

    assertEquals(Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    // Needs to consume empty once before producing.
    testEnv
        .kafkaRest()
        .target()
        .path(
            String.format(
                "/consumers/%s/instances/%s/records",
                CONSUMER_GROUP, createConsumerInstance.getInstanceId()))
        .request()
        .accept(getContentType())
        .get();

    SchemaTopicProduceRequest produceRequest =
        new SchemaTopicProduceRequest(
            getProduceRecords(),
            getKeySchema().canonicalString(),
            null,
            getValueSchema().canonicalString(),
            null);

    ProduceResponse produceResponse =
        testEnv
            .kafkaRest()
            .target()
            .path(String.format("/topics/%s", TOPIC))
            .request()
            .post(Entity.entity(produceRequest, getContentType()))
            .readEntity(ProduceResponse.class);
    assertEquals(Status.OK, produceResponse.getRequestStatus());

    Response readRecordsResponse =
        testEnv
            .kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/records",
                    CONSUMER_GROUP, createConsumerInstance.getInstanceId()))
            .request()
            .accept(getContentType())
            .get();

    assertEquals(Status.OK.getStatusCode(), readRecordsResponse.getStatus());

    List<SchemaConsumerRecord> readRecords =
        readRecordsResponse.readEntity(new GenericType<List<SchemaConsumerRecord>>() {});

    assertMapEquals(producedToMap(getProduceRecords()), consumedToMap(readRecords));
  }

  private static Map<JsonNode, JsonNode> producedToMap(List<SchemaTopicProduceRecord> records) {
    HashMap<JsonNode, JsonNode> map = new HashMap<>();
    for (SchemaTopicProduceRecord record : records) {
      map.put(record.getKey(), record.getValue());
    }
    return unmodifiableMap(map);
  }

  private static Map<JsonNode, JsonNode> consumedToMap(List<SchemaConsumerRecord> records) {
    HashMap<JsonNode, JsonNode> map = new HashMap<>();
    for (SchemaConsumerRecord record : records) {
      map.put(record.getKey(), record.getValue());
    }
    return unmodifiableMap(map);
  }

  private static <K, V> void assertMapEquals(Map<K, V> expected, Map<K, V> actual) {
    HashSet<K> extra = new HashSet<>();
    HashSet<K> missing = new HashSet<>();
    HashSet<K> different = new HashSet<>();

    for (Entry<K, V> entry : expected.entrySet()) {
      if (!actual.containsKey(entry.getKey())) {
        missing.add(entry.getKey());
      } else if (!actual.get(entry.getKey()).equals(entry.getValue())) {
        different.add(entry.getKey());
      }
    }

    for (K key : actual.keySet()) {
      if (!expected.containsKey(key)) {
        extra.add(key);
      }
    }

    if (!extra.isEmpty() || !missing.isEmpty() || !different.isEmpty()) {
      fail(
          String.format(
              "Expected and actual are not equal. Extra: %s, Missing: %s, Different: %s",
              extra, missing, different));
    }
  }
}
