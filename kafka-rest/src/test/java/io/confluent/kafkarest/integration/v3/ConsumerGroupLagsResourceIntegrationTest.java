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
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupLagResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerGroupLagsResourceIntegrationTest extends ClusterTestHarness {

  public ConsumerGroupLagsResourceIntegrationTest() {
    super(/* numBrokers= */1, /*withSchemaRegistry= */ false);
  }

  @Test
  public void getConsumerGroupLag_returnsConsumerGroupLag() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    // ProducerPool producerPool = new ProducerPool(restConfig);
    // BinaryPartitionProduceRequest request = newRequest("data1");
    // producerPool.produce("topic-1", 0, EmbeddedFormat.BINARY, request.toProduceRequest())
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-2", "client-2");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-3", "client-3");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/lag")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    // assertEquals(expected, response.readEntity(GetConsumerGroupLagResponse.class));
  }

  private BinaryPartitionProduceRequest newRequest(String data) {
    return BinaryPartitionProduceRequest.create(
        Collections.singletonList(new BinaryPartitionProduceRecord(null, data)));
  }

  private KafkaConsumer<?, ?> createConsumer(String consumerGroup, String clientId) {
    Properties properties = restConfig.getConsumerProperties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    return new KafkaConsumer<>(properties, new BytesDeserializer(), new BytesDeserializer());
  }
}
