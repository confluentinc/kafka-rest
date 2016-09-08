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

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.AbstractConsumerRecord;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.rest.RestConfigException;
import org.easymock.EasyMock;
import org.junit.Test;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class PartitionsResourceAvroConsumeTest extends PartitionsResourceAbstractConsumeTest {

  public PartitionsResourceAvroConsumeTest() throws RestConfigException {
    super();
  }

  @Test
  public void testConsumeOk() {
    final List<? extends AbstractConsumerRecord<JsonNode, JsonNode>> records = Arrays.asList(
        new AvroConsumerRecord(
            TestUtils.jsonTree("\"key1\""), TestUtils.jsonTree("\"value1\""), "topic", 0, 10)
    );

    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES_AVRO) {

      expectConsume(EmbeddedFormat.AVRO, records);

      final Response response = request(topicName, partitionId, offset, mediatype.header);
      assertOKResponse(response, mediatype.expected);

      final List<AvroConsumerRecord> readResponseRecords =
          response.readEntity(new GenericType<List<AvroConsumerRecord>>() {});
      assertEquals(records, readResponseRecords);

      EasyMock.verify(simpleConsumerManager);
      EasyMock.reset(simpleConsumerManager);
    }
  }

}
