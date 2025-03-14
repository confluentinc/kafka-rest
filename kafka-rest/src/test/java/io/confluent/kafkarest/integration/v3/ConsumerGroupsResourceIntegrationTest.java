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

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static io.confluent.kafkarest.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.entities.ConsumerGroup.State;
import io.confluent.kafkarest.entities.ConsumerGroup.Type;
import io.confluent.kafkarest.entities.v3.ConsumerGroupData;
import io.confluent.kafkarest.entities.v3.ConsumerGroupDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerGroupsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ConsumerGroupsResourceIntegrationTest extends ClusterTestHarness {

  public ConsumerGroupsResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void listConsumerGroups_returnsConsumerGroups(String quorum) throws InterruptedException {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);

    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1", "classic");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2", "classic");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3", "classic");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));

    KafkaConsumer<?, ?> consumer4 = createConsumer("consumer-group-2", "client-1", "consumer");
    KafkaConsumer<?, ?> consumer5 = createConsumer("consumer-group-2", "client-2", "consumer");
    KafkaConsumer<?, ?> consumer6 = createConsumer("consumer-group-2", "client-3", "consumer");
    consumer4.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer5.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer6.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));

    KafkaConsumer<?, ?> consumer7 = createConsumer("consumer-group-3", "client-1", "classic");
    KafkaConsumer<?, ?> consumer8 = createConsumer("consumer-group-3", "client-2", "consumer");
    KafkaConsumer<?, ?> consumer9 = createConsumer("consumer-group-3", "client-3", "classic");
    consumer7.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer8.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer9.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));

    List<KafkaConsumer<?, ?>> consumers =
        Arrays.asList(
            consumer1, consumer2, consumer3, consumer4, consumer5, consumer6, consumer7, consumer8,
            consumer9);

    ListConsumerGroupsResponse expected =
        getExpectedListResponse(
            baseUrl,
            clusterId,
            new String[] {"range", "uniform", "uniform"},
            new State[] {State.STABLE, State.STABLE, State.STABLE},
            new Type[] {Type.CLASSIC, Type.CONSUMER, Type.CONSUMER},
            new boolean[] {false, false, true});

    pollUntilTrue(
        consumers,
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/consumer-groups")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          return Status.OK.getStatusCode() == response.getStatus()
              && unorderedEquals(expected, response.readEntity(ListConsumerGroupsResponse.class));
        });
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void listConsumerGroups_nonExistingCluster_returnsNotFound(String quorum) {
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1", "classic");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2", "classic");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3", "classic");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/foobar/consumer-groups").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void getConsumerGroup_returnsConsumerGroup(String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1", "classic");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2", "classic");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3", "classic");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));

    GetConsumerGroupResponse expected =
        getExpectedGroupResponse(baseUrl, clusterId, "range", State.STABLE, Type.CLASSIC, false);

    pollUntilTrue(
        Arrays.asList(consumer1, consumer2, consumer3),
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          return Status.OK.getStatusCode() == response.getStatus()
              && response.readEntity(GetConsumerGroupResponse.class).equals(expected);
        });
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void getConsumerGroup_nonExistingCluster_returnsNotFound(String quorum) {
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1", "classic");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2", "classic");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3", "classic");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/foobar/consumer-groups/consumer-group-1")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void getConsumerGroup_nonExistingConsumerGroup_returnsNotFound(String quorum) {
    String clusterId = getClusterId();
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1", "classic");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2", "classic");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3", "classic");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/foobar")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  private void pollUntilTrue(List<KafkaConsumer<?, ?>> consumers, Callable<Boolean> condition) {
    waitForCondition(
        () -> {
          for (KafkaConsumer<?, ?> consumer : consumers) {
            consumer.poll(Duration.ofSeconds(1));
          }
          return condition.call();
        },
        "Condition is not met within the timeout.");
  }

  private boolean unorderedEquals(
      ListConsumerGroupsResponse expected, ListConsumerGroupsResponse actual) {
    return (expected.getValue().getKind().equals(actual.getValue().getKind()))
        && (expected.getValue().getMetadata().equals(actual.getValue().getMetadata()))
        && (new HashSet<>(expected.getValue().getData())
            .equals(new HashSet<>(actual.getValue().getData())));
  }

  private KafkaConsumer<?, ?> createConsumer(
      String consumerGroup, String clientId, String groupProtocol) {
    Properties properties = restConfig.getConsumerProperties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    properties.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
    return new KafkaConsumer<>(properties, new BytesDeserializer(), new BytesDeserializer());
  }

  private ListConsumerGroupsResponse getExpectedListResponse(
      String baseUrl,
      String clusterId,
      String[] partitionAssignors,
      State[] states,
      Type[] types,
      boolean[] isMixed) {
    return ListConsumerGroupsResponse.create(
        ConsumerGroupDataList.builder()
            .setMetadata(
                ResourceCollection.Metadata.builder()
                    .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups")
                    .build())
            .setData(
                IntStream.range(0, states.length)
                    .mapToObj(
                        i ->
                            getListedConsumerGroupData(
                                baseUrl,
                                clusterId,
                                "consumer-group-" + (i + 1),
                                partitionAssignors[i],
                                states[i],
                                types[i],
                                isMixed[i]))
                    .collect(java.util.stream.Collectors.toList()))
            .build());
  }

  private ConsumerGroupData getListedConsumerGroupData(
      String baseUrl,
      String clusterId,
      String groupId,
      String partitionAssignor,
      State state,
      Type type,
      boolean isMixed) {
    return ConsumerGroupData.builder()
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups/" + groupId)
                .setResourceName("crn:///kafka=" + clusterId + "/consumer-group=" + groupId)
                .build())
        .setClusterId(clusterId)
        .setConsumerGroupId(groupId)
        .setSimple(false)
        .setPartitionAssignor(partitionAssignor)
        .setState(state)
        .setType(type)
        .setMixedConsumerGroup(isMixed)
        .setCoordinator(Relationship.create(baseUrl + "/v3/clusters/" + clusterId + "/brokers/0"))
        .setConsumers(
            Relationship.create(
                baseUrl
                    + "/v3/clusters/"
                    + clusterId
                    + "/consumer-groups/"
                    + groupId
                    + "/consumers"))
        .setLagSummary(
            Relationship.create(
                baseUrl
                    + "/v3/clusters/"
                    + clusterId
                    + "/consumer-groups/"
                    + groupId
                    + "/lag-summary"))
        .build();
  }

  private GetConsumerGroupResponse getExpectedGroupResponse(
      String baseUrl,
      String clusterId,
      String partitionAssignor,
      State state,
      Type type,
      boolean isMixed) {
    return GetConsumerGroupResponse.create(
        ConsumerGroupData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1")
                    .setResourceName(
                        "crn:///kafka=" + clusterId + "/consumer-group=consumer-group-1")
                    .build())
            .setClusterId(clusterId)
            .setConsumerGroupId("consumer-group-1")
            .setSimple(false)
            .setPartitionAssignor(partitionAssignor)
            .setState(state)
            .setType(type)
            .setMixedConsumerGroup(isMixed)
            .setCoordinator(
                Relationship.create(baseUrl + "/v3/clusters/" + clusterId + "/brokers/0"))
            .setConsumers(
                Relationship.create(
                    baseUrl
                        + "/v3/clusters/"
                        + clusterId
                        + "/consumer-groups/consumer-group-1/consumers"))
            .setLagSummary(
                Relationship.create(
                    baseUrl
                        + "/v3/clusters/"
                        + clusterId
                        + "/consumer-groups/consumer-group-1/lag-summary"))
            .build());
  }
}
