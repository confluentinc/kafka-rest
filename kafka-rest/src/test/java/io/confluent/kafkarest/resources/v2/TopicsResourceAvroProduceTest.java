/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.resources.v2;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceRequest.ProduceRecord;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class TopicsResourceAvroProduceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private static final String TOPIC_NAME = "topic1";
  private static final String RAW_KEY_SCHEMA = "{\"name\":\"int\",\"type\": \"int\"}";
  private static final String RAW_VALUE_SCHEMA =
      "{\"type\": \"record\", "
          + "\"name\":\"test\","
          + "\"fields\":[{"
          + "  \"name\":\"field\", "
          + "  \"type\": \"int\""
          + "}]}";
  private static final AvroSchema KEY_SCHEMA =
      new AvroSchema(new Schema.Parser().parse(RAW_KEY_SCHEMA));
  private static final AvroSchema VALUE_SCHEMA =
      new AvroSchema(new Schema.Parser().parse(RAW_VALUE_SCHEMA));

  private static final JsonNode[] TEST_KEYS = {
    TestUtils.jsonTree("1"), TestUtils.jsonTree("2"),
  };

  private static final JsonNode[] TEST_VALUES = {
    TestUtils.jsonTree("{\"field\": 1}"), TestUtils.jsonTree("{\"field\": 2}"),
  };

  private final List<ProduceRecord> RECORDS =
      Arrays.asList(
          ProduceRecord.create(/* partition= */ 0, TEST_KEYS[0], TEST_VALUES[0]),
          ProduceRecord.create(/* partition= */ 0, TEST_KEYS[1], TEST_VALUES[1]));

  private static final TopicPartition PARTITION = new TopicPartition(TOPIC_NAME, 0);
  private final List<RecordMetadata> PRODUCE_RESULTS =
      Arrays.asList(
          new RecordMetadata(PARTITION, 0L, 0L, 0L, 0L, 1, 1),
          new RecordMetadata(PARTITION, 0L, 1L, 0L, 0L, 1, 1));
  private static final List<PartitionOffset> OFFSETS =
      Arrays.asList(new PartitionOffset(0, 0L, null, null), new PartitionOffset(0, 1L, null, null));

  @Mock private SchemaManager schemaManager;

  @Mock private RecordSerializer recordSerializer;

  @Mock private ProduceController produceController;

  public TopicsResourceAvroProduceTest() throws RestConfigException {
    addResource(
        new ProduceToTopicAction(
            () -> schemaManager, () -> recordSerializer, () -> produceController));
  }

  private Response produceToTopic(ProduceRequest request, List<RecordMetadata> results) {
    RegisteredSchema registeredKeySchema =
        RegisteredSchema.create(
            TOPIC_NAME + "key", /* schemaId= */ 1, /* schemaVersion= */ 1, KEY_SCHEMA);
    RegisteredSchema registeredValueSchema =
        RegisteredSchema.create(
            TOPIC_NAME + "value", /* schemaId= */ 2, /* schemaVersion= */ 1, VALUE_SCHEMA);

    expect(
            schemaManager.getSchema(
                /* topicName= */ TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.AVRO),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of(RAW_KEY_SCHEMA),
                /* isKey= */ true))
        .andStubReturn(registeredKeySchema);
    expect(
            schemaManager.getSchema(
                /* topicName= */ TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.AVRO),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of(RAW_VALUE_SCHEMA),
                /* isKey= */ false))
        .andStubReturn(registeredValueSchema);
    expect(
            schemaManager.getSchema(
                /* topicName= */ TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.of(1),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true))
        .andStubReturn(registeredKeySchema);
    expect(
            schemaManager.getSchema(
                /* topicName= */ TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.of(2),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ false))
        .andStubReturn(registeredValueSchema);

    for (int i = 0; i < request.getRecords().size(); i++) {
      ProduceRecord record = request.getRecords().get(i);
      ByteString serializedKey = ByteString.copyFromUtf8(String.valueOf(record.getKey()));
      ByteString serializedValue = ByteString.copyFromUtf8(String.valueOf(record.getValue()));

      expect(
              recordSerializer.serialize(
                  EmbeddedFormat.AVRO,
                  TopicsResourceAvroProduceTest.TOPIC_NAME,
                  Optional.of(registeredKeySchema),
                  record.getKey().orElse(NullNode.getInstance()),
                  /* isKey= */ true))
          .andReturn(Optional.of(serializedKey));
      expect(
              recordSerializer.serialize(
                  EmbeddedFormat.AVRO,
                  TopicsResourceAvroProduceTest.TOPIC_NAME,
                  Optional.of(registeredValueSchema),
                  record.getValue().orElse(NullNode.getInstance()),
                  /* isKey= */ false))
          .andReturn(Optional.of(serializedValue));

      expect(
              produceController.produce(
                  /* clusterId= */ eq(""),
                  eq(TopicsResourceAvroProduceTest.TOPIC_NAME),
                  eq(record.getPartition()),
                  /* headers= */ eq(ImmutableMultimap.of()),
                  eq(Optional.of(serializedKey)),
                  eq(Optional.of(serializedValue)),
                  /* timestamp= */ isA(Instant.class)))
          .andReturn(
              CompletableFuture.completedFuture(ProduceResult.fromRecordMetadata(results.get(i))));
    }

    replay(schemaManager, recordSerializer, produceController);

    Response response =
        request("/topics/" + TOPIC_NAME, Versions.KAFKA_V2_JSON)
            .post(Entity.entity(request, Versions.KAFKA_V2_JSON_AVRO));

    verify(schemaManager, recordSerializer, produceController);

    return response;
  }

  @Test
  public void testProduceToTopicWithPartitionAndKey() {
    ProduceRequest request =
        ProduceRequest.create(
            RECORDS,
            /* keySchemaId= */ null,
            RAW_KEY_SCHEMA,
            /* valueSchemaId= */ null,
            RAW_VALUE_SCHEMA);

    Response rawResponse = produceToTopic(request, PRODUCE_RESULTS);
    assertOKResponse(rawResponse, Versions.KAFKA_V2_JSON);
    ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);

    assertEquals(OFFSETS, response.getOffsets());
    assertEquals((Integer) 1, response.getKeySchemaId());
    assertEquals((Integer) 2, response.getValueSchemaId());

    EasyMock.reset(schemaManager, recordSerializer, produceController);

    // Test using schema IDs
    ProduceRequest request2 =
        ProduceRequest.create(
            RECORDS,
            /* keySchemaId= */ 1,
            /* keySchema= */ null,
            /* valueSchemaId= */ 2,
            /* valueSchema= */ null);

    Response rawResponse2 = produceToTopic(request2, PRODUCE_RESULTS);
    assertOKResponse(rawResponse2, Versions.KAFKA_V2_JSON);
  }
}
