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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ConsumerAssignmentManager;
import io.confluent.kafkarest.entities.ConsumerAssignment;
import io.confluent.kafkarest.entities.v3.ConsumerAssignmentData;
import io.confluent.kafkarest.entities.v3.ConsumerAssignmentDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerAssignmentResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerAssignmentsResponse;
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
public class ConsumerAssignmentsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String CONSUMER_GROUP_ID = "consumer-group-1";
  private static final String CONSUMER_ID = "consumer-1";

  private static final ConsumerAssignment CONSUMER_ASSIGNMENT_1 =
      ConsumerAssignment.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setConsumerId(CONSUMER_ID)
          .setTopicName("topic-1")
          .setPartitionId(1)
          .build();
  private static final ConsumerAssignment CONSUMER_ASSIGNMENT_2 =
      ConsumerAssignment.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setConsumerId(CONSUMER_ID)
          .setTopicName("topic-2")
          .setPartitionId(2)
          .build();
  private static final ConsumerAssignment CONSUMER_ASSIGNMENT_3 =
      ConsumerAssignment.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setConsumerId(CONSUMER_ID)
          .setTopicName("topic-3")
          .setPartitionId(3)
          .build();

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ConsumerAssignmentManager consumerAssignmentManager;

  private ConsumerAssignmentsResource consumerAssignmentsResource;

  @Before
  public void setUp() {
    consumerAssignmentsResource =
        new ConsumerAssignmentsResource(
            () -> consumerAssignmentManager, new CrnFactoryImpl(""), new FakeUrlFactory());
  }

  @Test
  public void listConsumers_returnsConsumers() {
    expect(
        consumerAssignmentManager.listConsumerAssignments(
            CLUSTER_ID, CONSUMER_GROUP_ID, CONSUMER_ID))
        .andReturn(
            completedFuture(
                Arrays.asList(
                    CONSUMER_ASSIGNMENT_1, CONSUMER_ASSIGNMENT_2, CONSUMER_ASSIGNMENT_3)));
    replay(consumerAssignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerAssignmentsResource.listConsumerAssignments(
        response, CLUSTER_ID, CONSUMER_GROUP_ID, CONSUMER_ID);

    ListConsumerAssignmentsResponse expected =
        ListConsumerAssignmentsResponse.create(
            ConsumerAssignmentDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                + "/consumers/consumer-1/assignments")
                        .build())
                .setData(
                    Arrays.asList(
                        ConsumerAssignmentData.fromConsumerAssignment(CONSUMER_ASSIGNMENT_1)
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                            + "/consumers/consumer-1"
                                            + "/assignments/topic-1/partitions/1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-1"
                                            + "/consumer=consumer-1"
                                            + "/assignment=topic-1/partition=1")
                                    .build())
                            .setPartition(
                                Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/1"))
                            .build(),
                        ConsumerAssignmentData.fromConsumerAssignment(CONSUMER_ASSIGNMENT_2)
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                            + "/consumers/consumer-1"
                                            + "/assignments/topic-2/partitions/2")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-1"
                                            + "/consumer=consumer-1"
                                            + "/assignment=topic-2/partition=2")
                                    .build())
                            .setPartition(
                                Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-2/partitions/2"))
                            .build(),
                        ConsumerAssignmentData.fromConsumerAssignment(CONSUMER_ASSIGNMENT_3)
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                            + "/consumers/consumer-1"
                                            + "/assignments/topic-3/partitions/3")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-1"
                                            + "/consumer=consumer-1"
                                            + "/assignment=topic-3/partition=3")
                                    .build())
                            .setPartition(
                                Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-3/partitions/3"))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getConsumer_returnsConsumer() {
    expect(
        consumerAssignmentManager.getConsumerAssignment(
            CLUSTER_ID, CONSUMER_GROUP_ID, CONSUMER_ID, "topic-1", 1))
        .andReturn(completedFuture(Optional.of(CONSUMER_ASSIGNMENT_1)));
    replay(consumerAssignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerAssignmentsResource.getConsumerAssignment(
        response, CLUSTER_ID, CONSUMER_GROUP_ID, CONSUMER_ID, "topic-1", 1);

    GetConsumerAssignmentResponse expected =
        GetConsumerAssignmentResponse.create(
            ConsumerAssignmentData.fromConsumerAssignment(CONSUMER_ASSIGNMENT_1)
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                + "/consumers/consumer-1"
                                + "/assignments/topic-1/partitions/1")
                        .setResourceName(
                            "crn:///kafka=cluster-1/consumer-group=consumer-group-1"
                                + "/consumer=consumer-1"
                                + "/assignment=topic-1/partition=1")
                        .build())
                .setPartition(
                    Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/1"))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getConsumer_nonExistingConsumer_throwsNotFound() {
    expect(
        consumerAssignmentManager.getConsumerAssignment(
            CLUSTER_ID, CONSUMER_GROUP_ID, CONSUMER_ID, "topic-1", 1))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerAssignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerAssignmentsResource.getConsumerAssignment(
        response, CLUSTER_ID, CONSUMER_GROUP_ID, CONSUMER_ID, "topic-1", 1);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
