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

package io.confluent.kafkarest.controllers;

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroup.State;
import io.confluent.kafkarest.entities.Partition;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerManagerImplTest {

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
  private ConsumerGroupManager consumerGroupManager;

  private ConsumerManagerImpl consumerManager;

  @Before
  public void setUp() {
    consumerManager = new ConsumerManagerImpl(consumerGroupManager);
  }

  @Test
  public void listConsumers_returnsConsumers() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId()))
        .andReturn(completedFuture(Optional.of(CONSUMER_GROUP)));
    replay(consumerGroupManager);

    List<Consumer> consumerGroups =
        consumerManager.listConsumers(CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId()).get();

    assertEquals(Arrays.asList(CONSUMERS), consumerGroups);
  }

  @Test
  public void listConsumerGroups_nonExistentConsumerGroup_throwsNotFound() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId()))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerGroupManager);

    try {
      consumerManager.listConsumers(CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getConsumer_returnsConsumer() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId()))
        .andReturn(completedFuture(Optional.of(CONSUMER_GROUP)));
    replay(consumerGroupManager);

    Consumer consumer =
        consumerManager.getConsumer(
            CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId(), CONSUMERS[0].getConsumerId())
            .get()
            .get();

    assertEquals(CONSUMERS[0], consumer);
  }

  @Test
  public void getConsumer_nonExistingConsumerGroup_throwsNotFound() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId()))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerGroupManager);

    try {
      consumerManager.getConsumer(
          CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId(), CONSUMERS[0].getConsumerId())
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getConsumer_nonExistingConsumer_returnsEmpty() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId()))
        .andReturn(completedFuture(Optional.of(CONSUMER_GROUP)));
    replay(consumerGroupManager);

    Optional<Consumer> consumer =
        consumerManager.getConsumer(CLUSTER_ID, CONSUMER_GROUP.getConsumerGroupId(), "foobar")
            .get();

    assertFalse(consumer.isPresent());
  }
}
