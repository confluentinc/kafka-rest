/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.unit;

import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.BinaryConsumerState;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.rest.RestConfigException;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class ConsumerResourceBinaryTest extends AbstractConsumerResourceTest {

  public ConsumerResourceBinaryTest() throws RestConfigException {
    super();
  }

  @Test
  public void testReadCommit() {
    List<? extends ConsumerRecord<byte[], byte[]>> expectedReadLimit = Arrays.asList(
        new BinaryConsumerRecord(topicName, "key1".getBytes(), "value1".getBytes(), 0, 10)
    );
    List<? extends ConsumerRecord<byte[], byte[]>> expectedReadNoLimit = Arrays.asList(
        new BinaryConsumerRecord(topicName, "key2".getBytes(), "value2".getBytes(), 1, 15),
        new BinaryConsumerRecord(topicName, "key3".getBytes(), "value3".getBytes(), 2, 20)
    );
    List<TopicPartitionOffset> expectedOffsets = Arrays.asList(
        new TopicPartitionOffset(topicName, 0, 10, 10),
        new TopicPartitionOffset(topicName, 1, 15, 15),
        new TopicPartitionOffset(topicName, 2, 20, 20)
    );

    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES_BINARY) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        expectCreateGroup(new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        expectReadTopic(topicName, BinaryConsumerState.class, 10, expectedReadLimit, null);
        expectReadTopic(topicName, BinaryConsumerState.class, expectedReadNoLimit, null);
        expectCommit(expectedOffsets, null);
        EasyMock.replay(consumerManager);

        Response response = request("/consumers/" + groupName, mediatype.header)
            .post(Entity.entity(null, requestMediatype));
        assertOKResponse(response, mediatype.expected);
        final CreateConsumerInstanceResponse createResponse =
                TestUtils.tryReadEntityOrLog(response,CreateConsumerInstanceResponse.class);

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
            = mediatype.header != null ? mediatype.expected : Versions.KAFKA_V1_JSON_BINARY;
        assertOKResponse(readLimitResponse, expectedMediatype);
        final List<BinaryConsumerRecord> readLimitResponseRecords =
                TestUtils.tryReadEntityOrLog(readLimitResponse,new GenericType<List<BinaryConsumerRecord>>() {
            });
        assertEquals(expectedReadLimit, readLimitResponseRecords);

        // Read without size limit
        Response readResponse = request(readUrl, mediatype.header).get();
        assertOKResponse(readResponse, expectedMediatype);
        final List<BinaryConsumerRecord> readResponseRecords =
                TestUtils.tryReadEntityOrLog(readResponse,new GenericType<List<BinaryConsumerRecord>>() {
            });
        assertEquals(expectedReadNoLimit, readResponseRecords);

        String commitUrl = instanceBasePath(createResponse) + "/offsets/";
        Response commitResponse = request(commitUrl, mediatype.header)
            .post(Entity.entity(null, requestMediatype));
        assertOKResponse(response, mediatype.expected);
        final List<TopicPartitionOffset> committedOffsets =
                TestUtils.tryReadEntityOrLog(commitResponse,new GenericType<List<TopicPartitionOffset>>() {
            });
        assertEquals(expectedOffsets, committedOffsets);

        EasyMock.verify(consumerManager);
        EasyMock.reset(consumerManager);
      }
    }
  }
}
