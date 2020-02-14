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

package io.confluent.kafkarest.resources.v1;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.AvroConsumerState;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.SchemaConsumerRecord;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.v1.CommitOffsetsResponse;
import io.confluent.kafkarest.entities.v1.CreateConsumerInstanceResponse;
import io.confluent.rest.RestConfigException;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
import org.junit.Test;

public class ConsumerResourceAvroTest extends AbstractConsumerResourceTest {

  public ConsumerResourceAvroTest() throws RestConfigException {
    super();
  }

  @Test
  public void testReadCommit() {
    List<? extends ConsumerRecord<JsonNode, JsonNode>> expectedReadLimit = Arrays.asList(
        new SchemaConsumerRecord(
            topicName, TestUtils.jsonTree("\"key1\""), TestUtils.jsonTree("\"value1\""), 0, 10)
    );
    List<? extends ConsumerRecord<JsonNode, JsonNode>> expectedReadNoLimit = Arrays.asList(
        new SchemaConsumerRecord(
            topicName, TestUtils.jsonTree("\"key2\""), TestUtils.jsonTree("\"value2\""), 1, 15),
        new SchemaConsumerRecord(
            topicName, TestUtils.jsonTree("\"key3\""), TestUtils.jsonTree("\"value3\""), 2, 20)
    );
    List<TopicPartitionOffset> expectedOffsets = Arrays.asList(
        new TopicPartitionOffset(topicName, 0, 10, 10),
        new TopicPartitionOffset(topicName, 1, 15, 15),
        new TopicPartitionOffset(topicName, 2, 20, 20)
    );

    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES_AVRO) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        expectCreateGroup(new ConsumerInstanceConfig(EmbeddedFormat.AVRO));
        expectReadTopic(topicName, AvroConsumerState.class, 10, expectedReadLimit, null);
        expectReadTopic(topicName, AvroConsumerState.class, expectedReadNoLimit, null);
        expectCommit(expectedOffsets, null);
        EasyMock.replay(consumerManager);

        Response response = request("/consumers/" + groupName, mediatype.header)
            .post(Entity.entity(new ConsumerInstanceConfig(EmbeddedFormat.AVRO), requestMediatype));
        assertOKResponse(response, mediatype.expected);
        final CreateConsumerInstanceResponse createResponse =
            TestUtils.tryReadEntityOrLog(response, CreateConsumerInstanceResponse.class);

        // Read with size limit
        String readUrl = instanceBasePath(createResponse) + "/topics/" + topicName;
        Invocation.Builder builder = getJerseyTest().target(readUrl)
            .queryParam("max_bytes", 10).request();
        if (mediatype.header != null) {
          builder.accept(mediatype.header);
        }
        Response readLimitResponse = builder.get();
        // Most specific default is different when retrieving embedded data
        String expectedMediatype
            = mediatype.header != null ? mediatype.expected : Versions.KAFKA_V1_JSON_AVRO;
        assertOKResponse(readLimitResponse, expectedMediatype);
        final List<SchemaConsumerRecord> readLimitResponseRecords =
            TestUtils.tryReadEntityOrLog(readLimitResponse, new GenericType<List<SchemaConsumerRecord>>() {});
        assertEquals(expectedReadLimit, readLimitResponseRecords);

        // Read without size limit
        Response readResponse = request(readUrl, mediatype.header).get();
        assertOKResponse(readResponse, expectedMediatype);
        final List<SchemaConsumerRecord> readResponseRecords =
            TestUtils
                .tryReadEntityOrLog(readResponse, new GenericType<List<SchemaConsumerRecord>>() {
                });
        assertEquals(expectedReadNoLimit, readResponseRecords);

        String commitUrl = instanceBasePath(createResponse) + "/offsets/";
        Response commitResponse = request(commitUrl, mediatype.header)
            .post(Entity.entity(null, requestMediatype));
        assertOKResponse(response, mediatype.expected);
        final CommitOffsetsResponse committedOffsets =
            TestUtils.tryReadEntityOrLog(commitResponse, CommitOffsetsResponse.class);
        assertEquals(CommitOffsetsResponse.fromOffsets(expectedOffsets), committedOffsets);

        EasyMock.verify(consumerManager);
        EasyMock.reset(consumerManager);
      }
    }
  }
}
