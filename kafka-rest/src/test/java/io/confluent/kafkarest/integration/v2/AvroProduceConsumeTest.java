package io.confluent.kafkarest.integration.v2;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.SchemaConsumerRecord;
import io.confluent.kafkarest.entities.SchemaTopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

public final class AvroProduceConsumeTest extends ClusterTestHarness {

  private static final AvroConverter AVRO_CONVERTER = new AvroConverter();

  private static final String TOPIC = "topic-1";

  private static final String CONSUMER_GROUP = "group-1";

  private static final Schema KEY_SCHEMA =
      new Schema.Parser().parse(""
          + "{"
          + "  \"type\": \"int\","
          + "  \"name\": \"key\""
          + "}");

  private static final Schema VALUE_SCHEMA =
      new Schema.Parser().parse(""
          + "{"
          + "  \"type\": \"record\", "
          + "  \"name\": \"ValueRecord\","
          + "  \"fields\":[{"
          + "    \"name\": \"value\", "
          + "    \"type\": \"int\""
          + "  }]"
          + "}");

  private static final List<SchemaTopicProduceRecord> PRODUCE_RECORDS =
      Arrays.asList(
          new SchemaTopicProduceRecord(
              new IntNode(1),
              AVRO_CONVERTER.toJson(
                  new GenericRecordBuilder(VALUE_SCHEMA).set("value", 11).build()).getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              new IntNode(2),
              AVRO_CONVERTER.toJson(
                  new GenericRecordBuilder(VALUE_SCHEMA).set("value", 12).build()).getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              new IntNode(3),
              AVRO_CONVERTER.toJson(
                  new GenericRecordBuilder(VALUE_SCHEMA).set("value", 13).build()).getJson(),
              /* partition= */ 0));

  public AvroProduceConsumeTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ true);
  }

  @Test
  public void produceThenConsume_returnsExactlyProduced() {
    createTopic(TOPIC, /* numPartitions= */ 1, /* replicationFactor= */ (short) 1);

    Response createConsumerInstanceResponse =
        request(String.format("/consumers/%s", CONSUMER_GROUP))
            .post(
                Entity.entity(
                    new ConsumerInstanceConfig(EmbeddedFormat.AVRO), Versions.KAFKA_V2_JSON));

    assertEquals(Status.OK.getStatusCode(), createConsumerInstanceResponse.getStatus());

    CreateConsumerInstanceResponse createConsumerInstance =
        createConsumerInstanceResponse.readEntity(CreateConsumerInstanceResponse.class);

    Response subscribeResponse =
        request(
            String.format(
                "/consumers/%s/instances/%s/subscription",
                CONSUMER_GROUP,
                createConsumerInstance.getInstanceId()))
            .post(
                Entity.entity(
                    new ConsumerSubscriptionRecord(
                        singletonList(TOPIC), null), Versions.KAFKA_V2_JSON));

    assertEquals(Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    // Needs to consume empty once before producing.
    request(
        String.format(
            "/consumers/%s/instances/%s/records",
            CONSUMER_GROUP,
            createConsumerInstance.getInstanceId()))
        .accept(Versions.KAFKA_V2_JSON_AVRO)
        .get();

    TopicProduceRequest<SchemaTopicProduceRecord> produceRequest = new TopicProduceRequest<>();
    produceRequest.setRecords(PRODUCE_RECORDS);
    produceRequest.setKeySchema(KEY_SCHEMA.toString());
    produceRequest.setValueSchema(VALUE_SCHEMA.toString());

    Response produceResponse =
        request(String.format("/topics/%s", TOPIC))
            .post(Entity.entity(produceRequest, Versions.KAFKA_V2_JSON_AVRO));

    assertEquals(Status.OK.getStatusCode(), produceResponse.getStatus());

    Response readRecordsResponse =
        request(
            String.format(
                "/consumers/%s/instances/%s/records",
                CONSUMER_GROUP,
                createConsumerInstance.getInstanceId()))
            .accept(Versions.KAFKA_V2_JSON_AVRO)
            .get();

    assertEquals(Status.OK.getStatusCode(), readRecordsResponse.getStatus());

    List<SchemaConsumerRecord> readRecords =
        readRecordsResponse.readEntity(new GenericType<List<SchemaConsumerRecord>>() {
        });

    assertMapEquals(producedToMap(PRODUCE_RECORDS), consumedToMap(readRecords));
  }

  private static final Map<JsonNode, JsonNode> producedToMap(
      List<SchemaTopicProduceRecord> records) {
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
              extra,
              missing,
              different));
    }
  }
}
