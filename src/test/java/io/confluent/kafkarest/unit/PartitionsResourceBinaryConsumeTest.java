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

import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.AbstractConsumerRecord;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
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

public class PartitionsResourceBinaryConsumeTest extends PartitionsResourceAbstractConsumeTest {

  public PartitionsResourceBinaryConsumeTest() throws RestConfigException {
    super();
  }

  @Test
  public void testConsumeOk() {
    List<? extends AbstractConsumerRecord<byte[], byte[]>> records = Arrays.asList(
        new BinaryConsumerRecord("key1".getBytes(), "value1".getBytes(), "topic", 0, 10)
    );

    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES_BINARY) {

      expectConsume(EmbeddedFormat.BINARY, records);

      final Response response = request(topicName, partitionId, offset, mediatype.header);

      // TODO : find a way to factor this exception with ConsumerResourceBinaryTest.java:89
      final String expectedMediatype
          = mediatype.header != null ? mediatype.expected : Versions.KAFKA_V1_JSON_BINARY;
      assertOKResponse(response, expectedMediatype);

      final List<BinaryConsumerRecord> readResponseRecords =
          response.readEntity(new GenericType<List<BinaryConsumerRecord>>() {});
      assertEquals(records, readResponseRecords);

      EasyMock.verify(simpleConsumerManager);
      EasyMock.reset(simpleConsumerManager);
    }
  }

}
