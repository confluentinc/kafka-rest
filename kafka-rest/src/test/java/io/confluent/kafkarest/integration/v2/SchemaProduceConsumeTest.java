/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.integration.v2;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
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
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("IntegrationTest")
public abstract class SchemaProduceConsumeTest extends ClusterTestHarness {

  private static final String TOPIC = "topic-1";

  private static final String CONSUMER_GROUP = "group-1";

  private static final Logger log = LoggerFactory.getLogger(SchemaProduceConsumeTest.class);

  protected abstract EmbeddedFormat getFormat();

  protected abstract String getContentType();

  protected abstract ParsedSchema getKeySchema();

  protected abstract ParsedSchema getValueSchema();

  protected abstract List<SchemaTopicProduceRecord> getProduceRecords();

  public SchemaProduceConsumeTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ true);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceThenConsume_returnsExactlyProduced(String quorum) throws Exception {
    createTopic(TOPIC, /* numPartitions= */ 1, /* replicationFactor= */ (short) 1);

    Response createConsumerInstanceResponse =
        request(String.format("/consumers/%s", CONSUMER_GROUP))
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
        request(
                String.format(
                    "/consumers/%s/instances/%s/subscription",
                    CONSUMER_GROUP, createConsumerInstance.getInstanceId()))
            .post(
                Entity.entity(
                    new ConsumerSubscriptionRecord(singletonList(TOPIC), null),
                    Versions.KAFKA_V2_JSON));

    assertEquals(Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    // Needs to consume empty once before producing.
    request(
            String.format(
                "/consumers/%s/instances/%s/records",
                CONSUMER_GROUP, createConsumerInstance.getInstanceId()))
        .accept(getContentType())
        .get();

    SchemaTopicProduceRequest produceRequest =
        new SchemaTopicProduceRequest(
            getProduceRecords(),
            getKeySchema().canonicalString(),
            null,
            getValueSchema().canonicalString(),
            null);

    Response genericResponse =
        request(String.format("/topics/%s", TOPIC))
            .post(Entity.entity(produceRequest, getContentType()));

    ProduceResponse produceResponse;
    try {
      produceResponse = genericResponse.readEntity(ProduceResponse.class);
    } catch (ConstraintViolationException e) {
      // Debug for jenkins only, intermittent failure where the response contains a field called
      // error_response that isn't part of a v2 ProduceResponse (probably from a v2 ErrorResponse)
      log.error(
          "Can't parse produce response class: {} status: {} ",
          genericResponse.getClass(),
          genericResponse.getStatus());
      log.error(
          "Reading entity using actual class: {}",
          genericResponse.readEntity(genericResponse.getClass()));
      e.printStackTrace();
      throw e;
    }
    assertEquals(Status.OK, produceResponse.getRequestStatus());

    Response readRecordsResponse =
        request(
                String.format(
                    "/consumers/%s/instances/%s/records",
                    CONSUMER_GROUP, createConsumerInstance.getInstanceId()))
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
