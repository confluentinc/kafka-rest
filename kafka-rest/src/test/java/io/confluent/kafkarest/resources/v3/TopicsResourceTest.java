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

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v3.CreateTopicRequest;
import io.confluent.kafkarest.entities.v3.CreateTopicResponse;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.PartitionsCountRequest;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.entities.v3.TopicDataList;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class TopicsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_INVALID_NAME = "<script>alert(1)</script>";

  private static final Topic TOPIC_1 =
      Topic.create(
          CLUSTER_ID,
          "topic-1",
          Arrays.asList(
              Partition.create(
                  CLUSTER_ID,
                  "topic-1",
                  /* partitionId= */ 0,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-1",
                          /* partitionId= */ 0,
                          /* brokerId= */ 1,
                          /* isLeader= */ true,
                          /* isInSync= */ true),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-1",
                          /* partitionId= */ 0,
                          /* brokerId= */ 2,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-1",
                          /* partitionId= */ 0,
                          /* brokerId= */ 3,
                          /* isLeader= */ false,
                          /* isInSync= */ false))),
              Partition.create(
                  CLUSTER_ID,
                  "topic-1",
                  /* partitionId= */ 1,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-1",
                          /* partitionId= */ 1,
                          /* brokerId= */ 1,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-1",
                          /* partitionId= */ 1,
                          /* brokerId= */ 2,
                          /* isLeader= */ true,
                          /* isInSync= */ true),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-1",
                          /* partitionId= */ 1,
                          /* brokerId= */ 3,
                          /* isLeader= */ false,
                          /* isInSync= */ false))),
              Partition.create(
                  CLUSTER_ID,
                  "topic-1",
                  /* partitionId= */ 2,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-1",
                          /* partitionId= */ 2,
                          /* brokerId= */ 1,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-1",
                          /* partitionId= */ 2,
                          /* brokerId= */ 2,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-1",
                          /* partitionId= */ 2,
                          /* brokerId= */ 3,
                          /* isLeader= */ true,
                          /* isInSync= */ true)))),
          /* replicationFactor= */ (short) 3,
          /* isInternal= */ false,
          /* authorizedOperations= */ emptySet());

  private static final Topic TOPIC_2 =
      Topic.create(
          CLUSTER_ID,
          "topic-2",
          Arrays.asList(
              Partition.create(
                  CLUSTER_ID,
                  "topic-2",
                  /* partitionId= */ 0,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-2",
                          /* partitionId= */ 0,
                          /* brokerId= */ 1,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-2",
                          /* partitionId= */ 0,
                          /* brokerId= */ 2,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-2",
                          /* partitionId= */ 0,
                          /* brokerId= */ 3,
                          /* isLeader= */ true,
                          /* isInSync= */ true))),
              Partition.create(
                  CLUSTER_ID,
                  "topic-2",
                  /* partitionId= */ 1,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-2",
                          /* partitionId= */ 1,
                          /* brokerId= */ 1,
                          /* isLeader= */ true,
                          /* isInSync= */ true),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-2",
                          /* partitionId= */ 1,
                          /* brokerId= */ 2,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-2",
                          /* partitionId= */ 1,
                          /* brokerId= */ 3,
                          /* isLeader= */ false,
                          /* isInSync= */ false))),
              Partition.create(
                  CLUSTER_ID,
                  "topic-2",
                  /* partitionId= */ 2,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-2",
                          /* partitionId= */ 2,
                          /* brokerId= */ 1,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-2",
                          /* partitionId= */ 2,
                          /* brokerId= */ 2,
                          /* isLeader= */ true,
                          /* isInSync= */ true),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-2",
                          /* partitionId= */ 2,
                          /* brokerId= */ 3,
                          /* isLeader= */ false,
                          /* isInSync= */ false)))),
          /* replicationFactor= */ (short) 3,
          /* isInternal= */ true,
          /* authorizedOperations= */ emptySet());

  private static final Topic TOPIC_3 =
      Topic.create(
          CLUSTER_ID,
          "topic-3",
          Arrays.asList(
              Partition.create(
                  CLUSTER_ID,
                  "topic-3",
                  /* partitionId= */ 0,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-3",
                          /* partitionId= */ 0,
                          /* brokerId= */ 1,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-3",
                          /* partitionId= */ 0,
                          /* brokerId= */ 2,
                          /* isLeader= */ true,
                          /* isInSync= */ true),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-3",
                          /* partitionId= */ 0,
                          /* brokerId= */ 3,
                          /* isLeader= */ false,
                          /* isInSync= */ false))),
              Partition.create(
                  CLUSTER_ID,
                  "topic-3",
                  /* partitionId= */ 1,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-3",
                          /* partitionId= */ 1,
                          /* brokerId= */ 1,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-3",
                          /* partitionId= */ 1,
                          /* brokerId= */ 2,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-3",
                          /* partitionId= */ 1,
                          /* brokerId= */ 3,
                          /* isLeader= */ true,
                          /* isInSync= */ true))),
              Partition.create(
                  CLUSTER_ID,
                  "topic-3",
                  /* partitionId= */ 2,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-3",
                          /* partitionId= */ 2,
                          /* brokerId= */ 1,
                          /* isLeader= */ true,
                          /* isInSync= */ true),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-3",
                          /* partitionId= */ 2,
                          /* brokerId= */ 2,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-3",
                          /* partitionId= */ 2,
                          /* brokerId= */ 3,
                          /* isLeader= */ false,
                          /* isInSync= */ false)))),
          /* replicationFactor= */ (short) 3,
          /* isInternal= */ false,
          /* authorizedOperations= */ emptySet());

  @Mock private TopicManager topicManager;

  private TopicsResource topicsResource;

  private static TopicData newTopicData(
      String topicName, boolean isInternal, int replicationFactor, int partitionsCount) {
    return TopicData.builder()
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(String.format("/v3/clusters/cluster-1/topics/%s", topicName))
                .setResourceName(String.format("crn:///kafka=cluster-1/topic=%s", topicName))
                .build())
        .setClusterId("cluster-1")
        .setTopicName(topicName)
        .setInternal(isInternal)
        .setReplicationFactor(replicationFactor)
        .setPartitions(
            Resource.Relationship.create(
                String.format("/v3/clusters/cluster-1/topics/%s/partitions", topicName)))
        .setConfigs(
            Resource.Relationship.create(
                String.format("/v3/clusters/cluster-1/topics/%s/configs", topicName)))
        .setPartitionReassignments(
            Resource.Relationship.create(
                String.format(
                    "/v3/clusters/cluster-1/topics/%s/partitions/-/reassignment", topicName)))
        .setPartitionsCount(partitionsCount)
        .setAuthorizedOperations(emptySet())
        .build();
  }

  @BeforeEach
  public void setUp() {
    topicsResource =
        new TopicsResource(
            () -> topicManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listTopics_existingCluster_returnsTopics() {
    expect(topicManager.listTopics(CLUSTER_ID, false))
        .andReturn(completedFuture(Arrays.asList(TOPIC_1, TOPIC_2, TOPIC_3)));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, CLUSTER_ID, false);

    ListTopicsResponse expected =
        ListTopicsResponse.create(
            TopicDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics")
                        .build())
                .setData(
                    Arrays.asList(
                        newTopicData("topic-1", false, 3, 3),
                        newTopicData("topic-2", true, 3, 3),
                        newTopicData("topic-3", false, 3, 3)))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listTopics_timeoutException_returnsTimeoutException() {
    expect(topicManager.listTopics(CLUSTER_ID, false))
        .andReturn(failedFuture(new TimeoutException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, CLUSTER_ID, false);

    assertEquals(TimeoutException.class, response.getException().getClass());
  }

  @Test
  public void listTopics_nonExistingCluster_returnsNotFound() {
    expect(topicManager.listTopics(CLUSTER_ID, false))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, CLUSTER_ID, false);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopics_existingCluster_returnsTopic() {
    expect(topicManager.getTopic(TOPIC_1.getClusterId(), TOPIC_1.getName(), false))
        .andReturn(completedFuture(Optional.of(TOPIC_1)));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName(), false);

    GetTopicResponse expected = GetTopicResponse.create(newTopicData("topic-1", false, 3, 3));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getTopics_nonExistingCluster_throwsNotFoundException() {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_1.getName(), false))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, CLUSTER_ID, TOPIC_1.getName(), false);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopics_existingCluster_nonExistingTopic_throwsNotFound() {
    expect(topicManager.getTopic(TOPIC_1.getClusterId(), TOPIC_1.getName(), false))
        .andReturn(completedFuture(Optional.empty()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName(), false);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void createTopic_nonExistingTopic_createsTopic() {
    expect(
            topicManager.createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                Optional.of(TOPIC_1.getPartitions().size()),
                Optional.of(TOPIC_1.getReplicationFactor()),
                /* replicasAssignments= */ Collections.emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false))
        .andReturn(
            completedFuture(
                Topic.builder()
                    .setClusterId(CLUSTER_ID)
                    .setName(TOPIC_1.getName())
                    .setReplicationFactor(TOPIC_1.getReplicationFactor())
                    .addAllPartitions(TOPIC_1.getPartitions())
                    .setInternal(false)
                    .setAuthorizedOperations(emptySet())
                    .build()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.createTopic(
        response,
        TOPIC_1.getClusterId(),
        CreateTopicRequest.builder()
            .setTopicName(TOPIC_1.getName())
            .setPartitionsCount(TOPIC_1.getPartitions().size())
            .setReplicationFactor(TOPIC_1.getReplicationFactor())
            .setConfigs(
                singletonList(CreateTopicRequest.ConfigEntry.create("cleanup.policy", "compact")))
            .build());

    CreateTopicResponse expected = CreateTopicResponse.create(newTopicData("topic-1", false, 3, 3));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void createTopic_nonExistingTopic_defaultPartitionsCount_createsTopic() {
    expect(
            topicManager.createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                /* partitionsCount= */ Optional.empty(),
                Optional.of(TOPIC_1.getReplicationFactor()),
                /* replicasAssignments= */ Collections.emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false))
        .andReturn(
            completedFuture(
                Topic.builder()
                    .setClusterId(CLUSTER_ID)
                    .setName(TOPIC_1.getName())
                    .setReplicationFactor((short) 3)
                    .addAllPartitions(TOPIC_1.getPartitions())
                    .setInternal(false)
                    .setAuthorizedOperations(emptySet())
                    .build()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.createTopic(
        response,
        TOPIC_1.getClusterId(),
        CreateTopicRequest.builder()
            .setTopicName(TOPIC_1.getName())
            .setReplicationFactor(TOPIC_1.getReplicationFactor())
            .setConfigs(
                singletonList(CreateTopicRequest.ConfigEntry.create("cleanup.policy", "compact")))
            .build());

    CreateTopicResponse expected = CreateTopicResponse.create(newTopicData("topic-1", false, 3, 3));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void createTopic_nonExistingTopic_defaultReplicationFactor_createsTopic() {
    expect(
            topicManager.createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                Optional.of(TOPIC_1.getPartitions().size()),
                /* replicationFactor= */ Optional.empty(),
                /* replicasAssignments= */ Collections.emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false))
        .andReturn(
            completedFuture(
                Topic.builder()
                    .setClusterId(CLUSTER_ID)
                    .setName(TOPIC_1.getName())
                    .setReplicationFactor((short) 3)
                    .addAllPartitions(TOPIC_1.getPartitions())
                    .setInternal(false)
                    .setAuthorizedOperations(emptySet())
                    .build()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.createTopic(
        response,
        TOPIC_1.getClusterId(),
        CreateTopicRequest.builder()
            .setTopicName(TOPIC_1.getName())
            .setPartitionsCount(TOPIC_1.getPartitions().size())
            .setConfigs(
                singletonList(CreateTopicRequest.ConfigEntry.create("cleanup.policy", "compact")))
            .build());

    CreateTopicResponse expected = CreateTopicResponse.create(newTopicData("topic-1", false, 3, 3));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void createTopic_nonExistingTopic_customReplicasAssignments_createsTopic() {
    ImmutableMap.Builder<Integer, List<Integer>> builder = new Builder<>();
    builder.put(0, Arrays.asList(1, 2));
    builder.put(1, Arrays.asList(2, 3));
    builder.put(2, Arrays.asList(3, 1));
    Map<Integer, List<Integer>> replicasAssignments = builder.build();
    ArrayList<Partition> responsePartitions = new ArrayList<>(3);
    responsePartitions.add(Partition.create(CLUSTER_ID, TOPIC_1.getName(), 0, emptyList()));
    responsePartitions.add(Partition.create(CLUSTER_ID, TOPIC_1.getName(), 1, emptyList()));
    responsePartitions.add(Partition.create(CLUSTER_ID, TOPIC_1.getName(), 2, emptyList()));

    expect(
            topicManager.createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                /* partitionsCount= */ Optional.of((int) 3),
                /* replicationFactor= */ Optional.of((short) 2),
                replicasAssignments,
                singletonMap("cleanup.policy", Optional.of("compact")),
                false))
        .andReturn(
            completedFuture(
                Topic.builder()
                    .setClusterId(CLUSTER_ID)
                    .setName(TOPIC_1.getName())
                    .setReplicationFactor((short) 2)
                    .addAllPartitions(responsePartitions)
                    .setInternal(false)
                    .setAuthorizedOperations(emptySet())
                    .build()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.createTopic(
        response,
        TOPIC_1.getClusterId(),
        CreateTopicRequest.builder()
            .setTopicName(TOPIC_1.getName())
            .setReplicasAssignments(replicasAssignments)
            .setConfigs(
                singletonList(CreateTopicRequest.ConfigEntry.create("cleanup.policy", "compact")))
            .build());

    CreateTopicResponse expected = CreateTopicResponse.create(newTopicData("topic-1", false, 2, 3));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void createTopic_existingTopic_throwsTopicExists() {
    expect(
            topicManager.createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                Optional.of(TOPIC_1.getPartitions().size()),
                Optional.of(TOPIC_1.getReplicationFactor()),
                /* replicasAssignments= */ Collections.emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false))
        .andReturn(failedFuture(new TopicExistsException("")));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.createTopic(
        response,
        TOPIC_1.getClusterId(),
        CreateTopicRequest.builder()
            .setTopicName(TOPIC_1.getName())
            .setPartitionsCount(TOPIC_1.getPartitions().size())
            .setReplicationFactor(TOPIC_1.getReplicationFactor())
            .setConfigs(
                singletonList(CreateTopicRequest.ConfigEntry.create("cleanup.policy", "compact")))
            .build());

    assertEquals(TopicExistsException.class, response.getException().getClass());
  }

  @Test
  public void createTopic_nonExistingCluster_throwsNotFound() {
    expect(
            topicManager.createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                Optional.of(TOPIC_1.getPartitions().size()),
                Optional.of(TOPIC_1.getReplicationFactor()),
                /* replicasAssignments= */ Collections.emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.createTopic(
        response,
        TOPIC_1.getClusterId(),
        CreateTopicRequest.builder()
            .setTopicName(TOPIC_1.getName())
            .setPartitionsCount(TOPIC_1.getPartitions().size())
            .setReplicationFactor(TOPIC_1.getReplicationFactor())
            .setConfigs(
                singletonList(CreateTopicRequest.ConfigEntry.create("cleanup.policy", "compact")))
            .build());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void createTopic_nonData_throwsInvalidPayload() {
    FakeAsyncResponse response = new FakeAsyncResponse();

    RestConstraintViolationException e =
        assertThrows(
            RestConstraintViolationException.class,
            () -> topicsResource.createTopic(response, TOPIC_1.getClusterId(), null));
    assertEquals("Payload error. Request body is empty. Data is required.", e.getMessage());
    assertEquals(42206, e.getErrorCode());
  }

  @Test
  public void createTopic_invalidTopicName_throwsInvalidPayload() {
    FakeAsyncResponse response = new FakeAsyncResponse();

    RestConstraintViolationException e =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                topicsResource.createTopic(
                    response,
                    TOPIC_1.getClusterId(),
                    CreateTopicRequest.builder()
                        .setTopicName(TOPIC_INVALID_NAME)
                        .setPartitionsCount(TOPIC_1.getPartitions().size())
                        .setReplicationFactor(TOPIC_1.getReplicationFactor())
                        .setConfigs(
                            singletonList(
                                CreateTopicRequest.ConfigEntry.create("cleanup.policy", "compact")))
                        .build()));
    assertEquals("Payload error. Invalid topic name.", e.getMessage());
    assertEquals(42206, e.getErrorCode());
  }

  @Test
  public void deleteTopic_existingTopic_deletesTopic() {
    expect(topicManager.deleteTopic(CLUSTER_ID, TOPIC_1.getName()))
        .andReturn(completedFuture(null));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.deleteTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName());

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void deleteTopic_nonExistingTopic_throwsUnknownTopicOfPartitionException() {
    expect(topicManager.deleteTopic(CLUSTER_ID, TOPIC_1.getName()))
        .andReturn(failedFuture(new UnknownTopicOrPartitionException("")));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.deleteTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName());

    assertEquals(UnknownTopicOrPartitionException.class, response.getException().getClass());
  }

  @Test
  public void deleteTopic_nonExistingCluster_throwsNotFound() {
    expect(topicManager.deleteTopic(CLUSTER_ID, TOPIC_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.deleteTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void testUpdatePartitions() {
    expect(topicManager.updateTopicPartitionsCount(TOPIC_1.getName(), 3))
        .andReturn(CompletableFuture.completedFuture(null));
    expect(topicManager.getTopic(TOPIC_1.getClusterId(), TOPIC_1.getName()))
        .andReturn(completedFuture(Optional.of(TOPIC_1)));

    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    PartitionsCountRequest request = PartitionsCountRequest.builder().setPartitionsCount(3).build();
    topicsResource.updatePartitionsCount(
        response, TOPIC_1.getClusterId(), TOPIC_1.getName(), request);

    GetTopicResponse expected = GetTopicResponse.create(newTopicData("topic-1", false, 3, 3));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void testUpdatePartitionsNoRequest() {

    FakeAsyncResponse response = new FakeAsyncResponse();
    PartitionsCountRequest request = null;

    RestConstraintViolationException e =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                topicsResource.updatePartitionsCount(
                    response, TOPIC_1.getClusterId(), TOPIC_1.getName(), request));
    assertEquals(
        "Payload error. Request body is empty. Partitions_count is required.", e.getMessage());
    assertEquals(42206, e.getErrorCode());
  }

  @Test
  public void testUpdatePartitionsUpdateFails() {

    CompletableFuture<Void> future = new CompletableFuture();
    future.completeExceptionally(new Exception("Oh no"));
    expect(topicManager.updateTopicPartitionsCount(TOPIC_1.getName(), 2)).andReturn(future);

    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    PartitionsCountRequest request = PartitionsCountRequest.builder().setPartitionsCount(2).build();

    topicsResource.updatePartitionsCount(
        response, TOPIC_1.getClusterId(), TOPIC_1.getName(), request);

    assertEquals("Oh no", response.getException().getMessage());
  }
}
