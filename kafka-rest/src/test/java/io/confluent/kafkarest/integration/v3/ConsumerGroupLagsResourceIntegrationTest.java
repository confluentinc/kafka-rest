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

package io.confluent.kafkarest.integration.v3;

import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.NoSchemaRestProducer;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupLagResponse;
import io.confluent.kafkarest.integration.AbstractConsumerTest;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerGroupLagsResourceIntegrationTest extends AbstractConsumerTest {

  private static final String topic1 = "topic-1";
  private static final String group1 = "consumer-group-1";
  private final List<ProducerRecord<byte[], byte[]>> recordsOnlyValues = Arrays.asList(
      new ProducerRecord<>(topic1, "value".getBytes()),
      new ProducerRecord<>(topic1, "value2".getBytes()),
      new ProducerRecord<>(topic1, "value3".getBytes()),
      new ProducerRecord<>(topic1, "value4".getBytes())
  );

//  public ConsumerGroupLagsResourceIntegrationTest() {
//    super(/* numBrokers= */1, /*withSchemaRegistry= */ false);
//  }

  private static final GenericType<List<BinaryConsumerRecord>> binaryConsumerRecordType
      = new GenericType<List<BinaryConsumerRecord>>() {
  };

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    createTopic(topic1, numPartitions, (short) replicationFactor);
  }
  @Test
  public void blah() {
    String instanceUri = startConsumeMessages(group1, topic1, null, Versions.KAFKA_V2_JSON_BINARY);
    produceBinaryMessages(recordsOnlyValues);
    consumeMessages(
        instanceUri,
        recordsOnlyValues,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        binaryConsumerRecordType,
        null,
        BinaryConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
    Response response =
        request("/v3/clusters/" + getClusterId() + "/consumer-groups/consumer-group-1/lag")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

//  @Test
//  public void getConsumerGroupLag_returnsConsumerGroupLagIfOffsetsFound() {
//    String baseUrl = restConnect;
//    String clusterId = getClusterId();
//
//    createTopic(topic1, /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
//    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
//    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
//    KafkaConsumer<?, ?> consumer1 = createConsumer(group1, "client-1");
//    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-2", "client-2");
//    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-3", "client-3");
//    consumer1.subscribe(Arrays.asList(topic1, "topic-2", "topic-3"));
//    consumer2.subscribe(Arrays.asList(topic1, "topic-2", "topic-3"));
//    consumer3.subscribe(Arrays.asList(topic1, "topic-2", "topic-3"));
//    consumer1.poll(Duration.ofSeconds(1));
//    consumer2.poll(Duration.ofSeconds(1));
//    consumer3.poll(Duration.ofSeconds(1));
//
//    Response response =
//        request("/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/lag")
//            .accept(MediaType.APPLICATION_JSON)
//            .get();
//    // empty topics, no offsets
//    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
//
//
//
//    // ProducerPool producerPool = new ProducerPool(restConfig);
//    // BinaryPartitionProduceRequest request = newRequest("data1");
//    // producerPool.produce("topic-1", 0, EmbeddedFormat.BINARY, request.toProduceRequest())
//    // assertEquals(expected, response.readEntity(GetConsumerGroupLagResponse.class));
//  }
//
//  private KafkaConsumer<?, ?> createConsumer(String consumerGroup, String clientId) {
//    Properties properties = restConfig.getConsumerProperties();
//    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
//    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
//    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
//    return new KafkaConsumer<>(properties, new BytesDeserializer(), new BytesDeserializer());
//  }
}
