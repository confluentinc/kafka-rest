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

package io.confluent.kafkarest.resources.v3;

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ConsumerManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroup.State;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.v3.ConsumerData;
import io.confluent.kafkarest.entities.v3.ConsumerDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerResponse;
import io.confluent.kafkarest.entities.v3.ListConsumersResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumersResourceTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Broker BROKER_1 =
      Broker.create(
          CLUSTER_ID,
          /* brokerId= */ 1,
          /* host= */ "1.2.3.4",
          /* port= */ 1000,
          /* rack= */ null);

  private static final Consumer[] CONSUMERS =
      {
          Consumer.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-1")
              .setConsumerId("consumer-1")
              .setClientId("client-1")
              .setInstanceId("instance-1")
              .setHost("11.12.12.14")
              .setAssignedPartitions(
                  Arrays.asList(
                      Partition.create(
                          CLUSTER_ID,
                          /* topicName= */ "topic-1",
                          /* partitionId= */ 1,
                          /* replicas= */ emptyList()),
                      Partition.create(
                          CLUSTER_ID,
                          /* topicName= */ "topic-2",
                          /* partitionId= */ 2,
                          /* replicas= */ emptyList()),
                      Partition.create(
                          CLUSTER_ID,
                          /* topicName= */ "topic-3",
                          /* partitionId= */ 3,
                          /* replicas= */ emptyList())))
              .build(),
          Consumer.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-1")
              .setConsumerId("consumer-2")
              .setClientId("client-2")
              .setInstanceId("instance-2")
              .setHost("21.22.23.24")
              .setAssignedPartitions(
                  Arrays.asList(
                      Partition.create(
                          CLUSTER_ID,
                          /* topicName= */ "topic-1",
                          /* partitionId= */ 4,
                          /* replicas= */ emptyList()),
                      Partition.create(
                          CLUSTER_ID,
                          /* topicName= */ "topic-2",
                          /* partitionId= */ 5,
                          /* replicas= */ emptyList()),
                      Partition.create(
                          CLUSTER_ID,
                          /* topicName= */ "topic-3",
                          /* partitionId= */ 6,
                          /* replicas= */ emptyList())))
              .build(),
          Consumer.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-1")
              .setConsumerId("consumer-3")
              .setClientId("client-3")
              .setInstanceId("instance-3")
              .setHost("31.32.33.34")
              .setAssignedPartitions(
                  Arrays.asList(
                      Partition.create(
                          CLUSTER_ID,
                          /* topicName= */ "topic-1",
                          /* partitionId= */ 7,
                          /* replicas= */ emptyList()),
                      Partition.create(
                          CLUSTER_ID,
                          /* topicName= */ "topic-2",
                          /* partitionId= */ 8,
                          /* replicas= */ emptyList()),
                      Partition.create(
                          CLUSTER_ID,
                          /* topicName= */ "topic-3",
                          /* partitionId= */ 9,
                          /* replicas= */ emptyList())))
              .build()
      };

  private static final ConsumerGroup CONSUMER_GROUP =
      ConsumerGroup.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-1")
          .setSimple(true)
          .setPartitionAssignor("org.apache.kafka.clients.consumer.RangeAssignor")
          .setState(State.STABLE)
          .setCoordinator(BROKER_1)
          .setConsumers(Arrays.asList(CONSUMERS))
          .build();

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ConsumerManager consumerManager;

  private ConsumersResource consumersResource;

  @Before
  public void setUp() {
    consumersResource =
        new ConsumersResource(() -> consumerManager, new CrnFactoryImpl(""), new FakeUrlFactory());
  }

  @Test
  public void listConsumers_returnsConsumers() {
    expect(consumerManager.listConsumers(CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId()))
        .andReturn(completedFuture(Arrays.asList(CONSUMERS)));
    replay(consumerManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumersResource.listConsumers(response, CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId());

    ListConsumersResponse expected =
        ListConsumersResponse.create(
            ConsumerDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/consumer-groups/consumer-group-1/consumers")
                        .build())
                .setData(
                    Arrays.asList(
                        ConsumerData.fromConsumer(CONSUMERS[0])
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                            + "/consumers/consumer-1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-1"
                                            + "/consumer=consumer-1")
                                    .build())
                            .setAssignments(
                                Relationship.create(
                                    "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                        + "/consumers/consumer-1/assignments"))
                            .build(),
                        ConsumerData.fromConsumer(CONSUMERS[1])
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                            + "/consumers/consumer-2")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-1"
                                            + "/consumer=consumer-2")
                                    .build())
                            .setAssignments(
                                Relationship.create(
                                    "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                        + "/consumers/consumer-2/assignments"))
                            .build(),
                        ConsumerData.fromConsumer(CONSUMERS[2])
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                            + "/consumers/consumer-3")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-1"
                                            + "/consumer=consumer-3")
                                    .build())
                            .setAssignments(
                                Relationship.create(
                                    "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                        + "/consumers/consumer-3/assignments"))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getConsumer_returnsConsumer() {
    expect(
        consumerManager.getConsumer(
            CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId(), CONSUMERS[0].getConsumerId()))
        .andReturn(completedFuture(Optional.of(CONSUMERS[0])));
    replay(consumerManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumersResource.getConsumer(
        response, CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId(), CONSUMERS[0].getConsumerId());

    GetConsumerResponse expected =
        GetConsumerResponse.create(
            ConsumerData.fromConsumer(CONSUMERS[0])
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                + "/consumers/consumer-1")
                        .setResourceName(
                            "crn:///kafka=cluster-1/consumer-group=consumer-group-1"
                                + "/consumer=consumer-1")
                        .build())
                .setAssignments(
                    Relationship.create(
                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                            + "/consumers/consumer-1/assignments"))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getConsumer_nonExistingConsumer_throwsNotFound() {
    expect(
        consumerManager.getConsumer(
            CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId(), CONSUMERS[0].getConsumerId()))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumersResource.getConsumer(
        response, CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId(), CONSUMERS[0].getConsumerId());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
