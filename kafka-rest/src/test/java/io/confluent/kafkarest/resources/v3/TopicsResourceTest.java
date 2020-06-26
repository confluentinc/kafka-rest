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
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v3.CreateTopicRequest;
import io.confluent.kafkarest.entities.v3.CreateTopicResponse;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.entities.v3.TopicDataList;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TopicsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";

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
                  /* partitionId= */2,
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
          /* isInternal= */ true);

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
                  /* partitionId= */2,
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
          /* isInternal= */ true);

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
                  /* partitionId= */2,
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
          /* isInternal= */ false);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private TopicManager topicManager;

  private TopicsResource topicsResource;

  @Before
  public void setUp() {
    topicsResource =
        new TopicsResource(
            () -> topicManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listTopics_existingCluster_returnsTopics() {
    expect(topicManager.listTopics(CLUSTER_ID))
        .andReturn(completedFuture(Arrays.asList(TOPIC_1, TOPIC_2, TOPIC_3)));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, CLUSTER_ID);

    ListTopicsResponse expected =
        ListTopicsResponse.create(
            TopicDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics")
                        .build())
                .setData(
                    Arrays.asList(
                        TopicData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/topics/topic-1")
                                    .setResourceName("crn:///kafka=cluster-1/topic=topic-1")
                                    .build())
                            .setClusterId("cluster-1")
                            .setTopicName("topic-1")
                            .setInternal(true)
                            .setReplicationFactor(3)
                            .setPartitions(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions"))
                            .setConfigs(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/configs"))
                            .setPartitionReassignments(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions"
                                        + "/-/reassignment"))
                            .build(),
                        TopicData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/topics/topic-2")
                                    .setResourceName("crn:///kafka=cluster-1/topic=topic-2")
                                    .build())
                            .setClusterId("cluster-1")
                            .setTopicName("topic-2")
                            .setInternal(true)
                            .setReplicationFactor(3)
                            .setPartitions(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-2/partitions"))
                            .setConfigs(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-2/configs"))
                            .setPartitionReassignments(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-2/partitions"
                                        + "/-/reassignment"))
                            .build(),
                        TopicData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/topics/topic-3")
                                    .setResourceName("crn:///kafka=cluster-1/topic=topic-3")
                                    .build())
                            .setClusterId("cluster-1")
                            .setTopicName("topic-3")
                            .setInternal(false)
                            .setReplicationFactor(3)
                            .setPartitions(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-3/partitions"))
                            .setConfigs(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-3/configs"))
                            .setPartitionReassignments(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-3/partitions"
                                        + "/-/reassignment"))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listTopics_timeoutException_returnsTimeoutException() {
    expect(topicManager.listTopics(CLUSTER_ID))
        .andReturn(failedFuture(new TimeoutException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, CLUSTER_ID);

    assertEquals(TimeoutException.class, response.getException().getClass());
  }

  @Test
  public void listTopics_nonExistingCluster_returnsNotFound() {
    expect(topicManager.listTopics(CLUSTER_ID))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, CLUSTER_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopics_existingCluster_returnsTopic() {
    expect(topicManager.getTopic(TOPIC_1.getClusterId(), TOPIC_1.getName())).
        andReturn(completedFuture(Optional.of(TOPIC_1)));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName());

    GetTopicResponse expected =
        GetTopicResponse.create(
            TopicData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/topic-1")
                        .setResourceName("crn:///kafka=cluster-1/topic=topic-1")
                        .build())
                .setClusterId("cluster-1")
                .setTopicName("topic-1")
                .setInternal(true)
                .setReplicationFactor(3)
                .setPartitions(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions"))
                .setConfigs(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/configs"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/-/reassignment"))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getTopics_nonExistingCluster_throwsNotFoundException() {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_1.getName())).
        andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, CLUSTER_ID, TOPIC_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopics_existingCluster_nonExistingTopic_throwsNotFound() {
    expect(topicManager.getTopic(TOPIC_1.getClusterId(), TOPIC_1.getName()))
        .andReturn(completedFuture(Optional.empty()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void createTopic_nonExistingTopic_createsTopic() {
    expect(
        topicManager.createTopic(
            CLUSTER_ID,
            TOPIC_1.getName(),
            TOPIC_1.getPartitions().size(),
            TOPIC_1.getReplicationFactor(),
            singletonMap("cleanup.policy", "compact")))
        .andReturn(completedFuture(null));
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

    CreateTopicResponse expected =
        CreateTopicResponse.create(
            TopicData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/topic-1")
                        .setResourceName("crn:///kafka=cluster-1/topic=topic-1")
                        .build())
                .setClusterId("cluster-1")
                .setTopicName("topic-1")
                .setInternal(false)
                .setReplicationFactor(3)
                .setPartitions(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions"))
                .setConfigs(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/configs"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/-/reassignment"))
                .build());

    assertEquals(expected, response.getValue());
  }


  @Test
  public void createTopic_existingTopic_throwsTopicExists() {
    expect(
        topicManager.createTopic(
            CLUSTER_ID,
            TOPIC_1.getName(),
            TOPIC_1.getPartitions().size(),
            TOPIC_1.getReplicationFactor(),
            singletonMap("cleanup.policy", "compact")))
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
        topicManager.createTopic(
            CLUSTER_ID,
            TOPIC_1.getName(),
            TOPIC_1.getPartitions().size(),
            TOPIC_1.getReplicationFactor(),
            singletonMap("cleanup.policy", "compact")))
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
}
