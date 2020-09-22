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

package io.confluent.kafkarest.integration;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceRequest.ProduceRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.Test;

public class ProducerTopicAutoCreationTest extends AbstractProducerTest {

  private static final String topicName = "nonexistant";

  private static TextNode base64Encode(String value) {
    return TextNode.valueOf(BaseEncoding.base64().encode(value.getBytes(StandardCharsets.UTF_8)));
  }

  
  private final List<ProduceRecord> topicRecords =
      Arrays.asList(
          ProduceRecord.create(base64Encode("key"), base64Encode("value")),
          ProduceRecord.create(base64Encode("key"), base64Encode("value2")),
          ProduceRecord.create(base64Encode("key"), base64Encode("value3")),
          ProduceRecord.create(base64Encode("key"), base64Encode("value4")));

  private final List<PartitionOffset> partitionOffsets = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(0, 2L, null, null),
      new PartitionOffset(0, 3L, null, null)
  );

  public Properties overrideBrokerProperties(int i, Properties props) {
    Properties refined = (Properties) props.clone();
    refined.setProperty("auto.create.topics.enable", "true");
    return refined;
  }

  @Test
  public void testProduceToMissingTopic() {
    ProduceRequest request = ProduceRequest.create(topicRecords);
    // Should create topic
    testProduceToTopic(
        topicName,
        request,
        record ->
            record.getValue()
                .map(value -> ByteString.copyFrom(BaseEncoding.base64().decode(value.asText())))
                .orElse(null),
        (topic, data) -> ByteString.copyFrom(data),
        (topic, data) -> ByteString.copyFrom(data),
        partitionOffsets,
        false,
        request.getRecords());
  }
}
