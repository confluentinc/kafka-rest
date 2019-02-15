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

package io.confluent.kafkarest.integration;

import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.TopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import kafka.serializer.Decoder;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AbstractProducerTest extends ClusterTestHarness {

  protected <K, V> void testProduceToTopic(String topicName,
                                           List<? extends TopicProduceRecord> records,
                                           Decoder<K> keyDecoder, Decoder<K> valueDecoder,
                                           List<PartitionOffset> offsetResponses,
                                           boolean matchPartitions) {
    TopicProduceRequest payload = new TopicProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName)
        .post(Entity.entity(payload, getEmbeddedContentType()));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse produceResponse = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    if (matchPartitions) {
      TestUtils.assertPartitionsEqual(offsetResponses, produceResponse.getOffsets());
    }
    TestUtils.assertPartitionOffsetsEqual(offsetResponses, produceResponse.getOffsets());
    TestUtils.assertTopicContains(zkConnect, topicName,
                                  payload.getRecords(), null,
                                  keyDecoder, valueDecoder, true);
  }


  protected <K, V> void testProduceToPartition(String topicName,
                                               int partition,
                                               List<? extends ProduceRecord<K, V>> records,
                                               Decoder<K> keyDecoder, Decoder<K> valueDecoder,
                                               List<PartitionOffset> offsetResponse) {
    PartitionProduceRequest payload = new PartitionProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName + "/partitions/0")
        .post(Entity.entity(payload, getEmbeddedContentType()));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse poffsetResponse
        = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    assertEquals(offsetResponse, poffsetResponse.getOffsets());
    TestUtils.assertTopicContains(zkConnect, topicName,
        payload.getRecords(), partition,
        keyDecoder, valueDecoder, true);
  }


  protected void testProduceToTopicFails(String topicName,
                                                List<? extends TopicProduceRecord> records) {
    TopicProduceRequest payload = new TopicProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName)
        .post(Entity.entity(payload, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse produceResponse = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    for (PartitionOffset pOffset : produceResponse.getOffsets()) {
      assertNotNull(pOffset.getError());
    }
  }

  protected String getEmbeddedContentType() {
    return Versions.KAFKA_MOST_SPECIFIC_DEFAULT;
  }
}
