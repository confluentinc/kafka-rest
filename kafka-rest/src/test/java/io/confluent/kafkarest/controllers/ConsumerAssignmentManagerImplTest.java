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

import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerAssignment;
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
public class ConsumerAssignmentManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Consumer CONSUMER =
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
              .build();

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ConsumerManager consumerManager;

  private ConsumerAssignmentManagerImpl consumerAssignmentManager;

  @Before
  public void setUp() {
    consumerAssignmentManager = new ConsumerAssignmentManagerImpl(consumerManager);
  }

  @Test
  public void listConsumerAssignments_returnsConsumerAssignments() throws Exception {
    expect(
        consumerManager.getConsumer(
            CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId()))
        .andReturn(completedFuture(Optional.of(CONSUMER)));
    replay(consumerManager);

    List<ConsumerAssignment> assignments =
        consumerAssignmentManager.listConsumerAssignments(
            CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId()).get();

    assertEquals(
        Arrays.asList(
            ConsumerAssignment.builder()
                .setClusterId(CLUSTER_ID)
                .setConsumerGroupId(CONSUMER.getConsumerGroupId())
                .setConsumerId(CONSUMER.getConsumerId())
                .setTopicName("topic-1")
                .setPartitionId(1)
                .build(),
            ConsumerAssignment.builder()
                .setClusterId(CLUSTER_ID)
                .setConsumerGroupId(CONSUMER.getConsumerGroupId())
                .setConsumerId(CONSUMER.getConsumerId())
                .setTopicName("topic-2")
                .setPartitionId(2)
                .build(),
            ConsumerAssignment.builder()
                .setClusterId(CLUSTER_ID)
                .setConsumerGroupId(CONSUMER.getConsumerGroupId())
                .setConsumerId(CONSUMER.getConsumerId())
                .setTopicName("topic-3")
                .setPartitionId(3)
                .build()),
        assignments);
  }

  @Test
  public void listConsumerAssignments_nonExistentConsumer_throwsNotFound() throws Exception {
    expect(
        consumerManager.getConsumer(
            CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId()))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerManager);

    try {
      consumerAssignmentManager.listConsumerAssignments(
          CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getConsumerAssignments_returnsConsumerAssignment() throws Exception {
    expect(
        consumerManager.getConsumer(
            CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId()))
        .andReturn(completedFuture(Optional.of(CONSUMER)));
    replay(consumerManager);

    ConsumerAssignment assignment =
        consumerAssignmentManager.getConsumerAssignment(
            CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId(), "topic-1", 1)
            .get()
            .get();

    assertEquals(
        ConsumerAssignment.builder()
            .setClusterId(CLUSTER_ID)
            .setConsumerGroupId(CONSUMER.getConsumerGroupId())
            .setConsumerId(CONSUMER.getConsumerId())
            .setTopicName("topic-1")
            .setPartitionId(1)
            .build(),
        assignment);
  }

  @Test
  public void getConsumerAssignment_nonExistingConsumer_throwsNotFound() throws Exception {
    expect(
        consumerManager.getConsumer(
            CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId()))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerManager);

    try {
      consumerAssignmentManager.getConsumerAssignment(
          CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId(), "topic-1", 1)
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getConsumerAssignment_nonExistingConsumerAssignment_returnsEmpty() throws Exception {
    expect(
        consumerManager.getConsumer(
            CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId()))
        .andReturn(completedFuture(Optional.of(CONSUMER)));
    replay(consumerManager);

    Optional<ConsumerAssignment> assignment =
        consumerAssignmentManager.getConsumerAssignment(
            CLUSTER_ID, CONSUMER.getConsumerGroupId(), CONSUMER.getConsumerId(), "foobar", 100)
            .get();

    assertFalse(assignment.isPresent());
  }
}
