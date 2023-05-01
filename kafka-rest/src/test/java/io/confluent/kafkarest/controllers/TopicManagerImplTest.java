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

import static io.confluent.kafkarest.common.KafkaFutures.failedFuture;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class TopicManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Node NODE_1 = new Node(1, "broker-1", 9091);
  private static final Node NODE_2 = new Node(2, "broker-2", 9092);
  private static final Node NODE_3 = new Node(3, "broker-3", 9093);

  private static final Broker BROKER_1 = Broker.fromNode(CLUSTER_ID, NODE_1);
  private static final Broker BROKER_2 = Broker.fromNode(CLUSTER_ID, NODE_2);
  private static final Broker BROKER_3 = Broker.fromNode(CLUSTER_ID, NODE_3);

  private static final Cluster CLUSTER =
      Cluster.create(CLUSTER_ID, BROKER_1, Arrays.asList(BROKER_1, BROKER_2, BROKER_3));

  private static final List<TopicListing> TOPIC_LISTINGS =
      Arrays.asList(
          new TopicListing("topic-1", Uuid.ZERO_UUID, true),
          new TopicListing("topic-2", Uuid.ZERO_UUID, true),
          new TopicListing("topic-3", Uuid.ZERO_UUID, false));

  private static final TopicDescription TOPIC_DESCRIPTION_1 =
      new TopicDescription(
          "topic-1",
          /* internal= */ true,
          Arrays.asList(
              new TopicPartitionInfo(
                  0, NODE_1, Arrays.asList(NODE_1, NODE_2, NODE_3), singletonList(NODE_1)),
              new TopicPartitionInfo(
                  1, NODE_2, Arrays.asList(NODE_1, NODE_2, NODE_3), singletonList(NODE_2)),
              new TopicPartitionInfo(
                  2,
                  /* leader= */ null,
                  Arrays.asList(NODE_1, NODE_2, NODE_3),
                  singletonList(NODE_3))),
          /* authorizedOperations= */ new HashSet<>());

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
                          /* isLeader= */ false,
                          /* isInSync= */ true)))),
          /* replicationFactor= */ (short) 3,
          /* isInternal= */ true,
          /* authorizedOperations= */ emptySet());

  private static final Topic TOPIC_1_EMPTY_REPLICAS =
      Topic.create(
          CLUSTER_ID,
          "topic-1",
          Arrays.asList(
              Partition.create(
                  CLUSTER_ID, "topic-1", /* partitionId= */ 0, Collections.emptyList()),
              Partition.create(
                  CLUSTER_ID, "topic-1", /* partitionId= */ 1, Collections.emptyList()),
              Partition.create(
                  CLUSTER_ID, "topic-1", /* partitionId= */ 2, Collections.emptyList())),
          /* replicationFactor= */ (short) 3,
          /* isInternal= */ true,
          /* authorizedOperations= */ emptySet());

  private static final TopicDescription TOPIC_DESCRIPTION_2 =
      new TopicDescription(
          "topic-2",
          /* internal= */ true,
          Arrays.asList(
              new TopicPartitionInfo(
                  0, NODE_3, Arrays.asList(NODE_1, NODE_2, NODE_3), singletonList(NODE_3)),
              new TopicPartitionInfo(
                  1, NODE_1, Arrays.asList(NODE_1, NODE_2, NODE_3), singletonList(NODE_1)),
              new TopicPartitionInfo(
                  2, NODE_2, Arrays.asList(NODE_1, NODE_2, NODE_3), singletonList(NODE_2))),
          /* authorizedOperations= */ new HashSet<>());

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

  private static final TopicDescription TOPIC_DESCRIPTION_3 =
      new TopicDescription(
          "topic-3",
          /* internal= */ false,
          Arrays.asList(
              new TopicPartitionInfo(
                  0, NODE_2, Arrays.asList(NODE_1, NODE_2, NODE_3), singletonList(NODE_2)),
              new TopicPartitionInfo(
                  1, NODE_3, Arrays.asList(NODE_1, NODE_2, NODE_3), singletonList(NODE_3)),
              new TopicPartitionInfo(
                  2, NODE_1, Arrays.asList(NODE_1, NODE_2, NODE_3), singletonList(NODE_1))),
          /* authorizedOperations= */ new HashSet<>());

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

  private static final TopicDescription TOPIC_DESCRIPTION_4 =
      new TopicDescription(
          "topic-4",
          /* internal= */ false,
          singletonList(
              new TopicPartitionInfo(
                  0, NODE_2, Arrays.asList(NODE_1, NODE_2, NODE_3), singletonList(NODE_2))),
          singleton(AclOperation.CREATE));

  private static final Topic TOPIC_4 =
      Topic.create(
          CLUSTER_ID,
          "topic-4",
          singletonList(
              Partition.create(
                  CLUSTER_ID,
                  "topic-4",
                  /* partitionId= */ 0,
                  Arrays.asList(
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-4",
                          /* partitionId= */ 0,
                          /* brokerId= */ 1,
                          /* isLeader= */ false,
                          /* isInSync= */ false),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-4",
                          /* partitionId= */ 0,
                          /* brokerId= */ 2,
                          /* isLeader= */ true,
                          /* isInSync= */ true),
                      PartitionReplica.create(
                          CLUSTER_ID,
                          "topic-4",
                          /* partitionId= */ 0,
                          /* brokerId= */ 3,
                          /* isLeader= */ false,
                          /* isInSync= */ false)))),
          /* replicationFactor= */ (short) 3,
          /* isInternal= */ false,
          singleton(Acl.Operation.CREATE));

  @Mock private Admin adminClient;

  @Mock private ClusterManager clusterManager;

  @Mock private ListTopicsResult listTopicsResult;

  @Mock private DescribeTopicsResult describeTopicResult;

  @Mock private CreateTopicsResult createTopicsResult;

  @Mock private DeleteTopicsResult deleteTopicsResult;

  @Mock private CreatePartitionsResult createPartitionsResult;

  private TopicManagerImpl topicManager;

  @BeforeEach
  public void setUp() {
    topicManager = new TopicManagerImpl(adminClient, clusterManager);
  }

  @Test
  public void listTopics_existingCluster_returnsTopicsWithAuthOperations() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(listTopicsResult.listings())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonList(new TopicListing("topic-4", Uuid.ZERO_UUID, false))));
    expect(adminClient.describeTopics(isA(Collection.class), anyObject()))
        .andReturn(describeTopicResult);
    expect(describeTopicResult.allTopicNames())
        .andReturn(KafkaFuture.completedFuture(createTopicDescriptionMap(TOPIC_DESCRIPTION_4)));
    replay(clusterManager, adminClient, listTopicsResult, describeTopicResult);

    List<Topic> topics = topicManager.listTopics(CLUSTER_ID, true).get();

    assertEquals(Arrays.asList(TOPIC_4), topics);
  }

  @Test
  public void getTopic_existingCluster_returnsTopicsWithAuthOperations() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.describeTopics(isA(Collection.class), anyObject()))
        .andReturn(describeTopicResult);
    expect(describeTopicResult.allTopicNames())
        .andReturn(KafkaFuture.completedFuture(createTopicDescriptionMap(TOPIC_DESCRIPTION_4)));
    replay(clusterManager, adminClient, describeTopicResult);

    Optional<Topic> topic = topicManager.getTopic(CLUSTER_ID, "topic-4", true).get();

    assertEquals(TOPIC_4, topic.get());
  }

  @Test
  public void listTopics_existingCluster_returnsTopics() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(listTopicsResult.listings()).andReturn(KafkaFuture.completedFuture(TOPIC_LISTINGS));
    expect(adminClient.describeTopics(isA(Collection.class), anyObject()))
        .andReturn(describeTopicResult);
    expect(describeTopicResult.allTopicNames())
        .andReturn(
            KafkaFuture.completedFuture(
                createTopicDescriptionMap(
                    TOPIC_DESCRIPTION_1, TOPIC_DESCRIPTION_2, TOPIC_DESCRIPTION_3)));
    replay(clusterManager, adminClient, listTopicsResult, describeTopicResult);

    List<Topic> topics = topicManager.listTopics(CLUSTER_ID).get();

    assertEquals(Arrays.asList(TOPIC_1, TOPIC_2, TOPIC_3), topics);
  }

  @Test
  public void listTopics_timeoutException_throwsTimeoutException() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(listTopicsResult.listings()).andReturn(failedFuture(new TimeoutException()));
    replay(clusterManager, adminClient, listTopicsResult);

    try {
      topicManager.listTopics(CLUSTER_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }

  @Test
  public void listLocalTopics_returnsTopics() throws Exception {
    expect(clusterManager.getLocalCluster()).andReturn(completedFuture(CLUSTER));
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(listTopicsResult.listings()).andReturn(KafkaFuture.completedFuture(TOPIC_LISTINGS));
    expect(adminClient.describeTopics(isA(Collection.class), anyObject()))
        .andReturn(describeTopicResult);
    expect(describeTopicResult.allTopicNames())
        .andReturn(
            KafkaFuture.completedFuture(
                createTopicDescriptionMap(
                    TOPIC_DESCRIPTION_1, TOPIC_DESCRIPTION_2, TOPIC_DESCRIPTION_3)));
    replay(clusterManager, adminClient, listTopicsResult, describeTopicResult);

    List<Topic> topics = topicManager.listLocalTopics().get();

    assertEquals(Arrays.asList(TOPIC_1, TOPIC_2, TOPIC_3), topics);
  }

  @Test
  public void listTopic_nonExistingCluster_throwsNotFoundException() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      topicManager.listTopics(CLUSTER_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getTopic_existingTopic_returnsTopic() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.describeTopics(isA(Collection.class), anyObject()))
        .andReturn(describeTopicResult);
    expect(describeTopicResult.allTopicNames())
        .andReturn(KafkaFuture.completedFuture(createTopicDescriptionMap(TOPIC_DESCRIPTION_1)));
    replay(clusterManager, adminClient, describeTopicResult);

    Topic topic = topicManager.getTopic(CLUSTER_ID, TOPIC_1.getName()).get().get();

    assertEquals(TOPIC_1, topic);
  }

  @Test
  public void getTopic_nonExistingCluster_throwsNotFoundException() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    expect(adminClient.describeTopics(isA(Collection.class), anyObject()))
        .andReturn(describeTopicResult);
    replay(clusterManager);

    try {
      topicManager.listTopics(CLUSTER_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getTopic_nonExistingTopic_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.describeTopics(isA(Collection.class), anyObject()))
        .andReturn(describeTopicResult);
    expect(describeTopicResult.allTopicNames())
        .andReturn(KafkaFuture.completedFuture(new HashMap<>()));
    replay(clusterManager, adminClient, describeTopicResult);

    Optional<Topic> topic = topicManager.getTopic(CLUSTER_ID, TOPIC_1.getName()).get();

    assertFalse(topic.isPresent());
  }

  @Test
  public void getLocalTopic_existingTopic_returnsTopic() throws Exception {
    expect(clusterManager.getLocalCluster()).andReturn(completedFuture(CLUSTER));
    expect(adminClient.describeTopics(isA(Collection.class), anyObject()))
        .andReturn(describeTopicResult);
    expect(describeTopicResult.allTopicNames())
        .andReturn(KafkaFuture.completedFuture(createTopicDescriptionMap(TOPIC_DESCRIPTION_1)));
    replay(clusterManager, adminClient, describeTopicResult);

    Topic topic = topicManager.getLocalTopic(TOPIC_1.getName()).get().get();

    assertEquals(TOPIC_1, topic);
  }

  @Test
  public void getLocalTopic_nonExistingTopic_returnsEmpty() throws Exception {
    expect(clusterManager.getLocalCluster()).andReturn(completedFuture(CLUSTER));
    expect(adminClient.describeTopics(isA(Collection.class), anyObject()))
        .andReturn(describeTopicResult);
    expect(describeTopicResult.allTopicNames())
        .andReturn(KafkaFuture.completedFuture(new HashMap<>()));
    replay(clusterManager, adminClient, describeTopicResult);

    Optional<Topic> topic = topicManager.getLocalTopic(TOPIC_1.getName()).get();

    assertFalse(topic.isPresent());
  }

  // Test for the deprecated createTopic method making sure it work for successful completion
  @Test
  public void createTopic_nonExistingTopic_createsTopic() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(KafkaFuture.completedFuture(TOPIC_1.getPartitions().size()));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(KafkaFuture.completedFuture((int) (TOPIC_1.getReplicationFactor())));
    replay(clusterManager, adminClient, createTopicsResult);

    topicManager
        .createTopic(
            CLUSTER_ID,
            TOPIC_1.getName(),
            Optional.of(TOPIC_1.getPartitions().size()),
            Optional.of(TOPIC_1.getReplicationFactor()),
            /* replicasAssignments= */ emptyMap(),
            singletonMap("cleanup.policy", Optional.of("compact")))
        .get();

    verify(adminClient);
  }

  @Test
  public void createTopic2_nonExistingTopic_createsTopic() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(KafkaFuture.completedFuture(TOPIC_1.getPartitions().size()));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(KafkaFuture.completedFuture((int) (TOPIC_1.getReplicationFactor())));
    replay(clusterManager, adminClient, createTopicsResult);

    Topic createTopicResponse =
        topicManager
            .createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                Optional.of(TOPIC_1.getPartitions().size()),
                Optional.of(TOPIC_1.getReplicationFactor()),
                /* replicasAssignments= */ emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false)
            .get();

    verify(adminClient);
    assertEquals(
        Topic.create(
            CLUSTER_ID,
            TOPIC_1_EMPTY_REPLICAS.getName(),
            TOPIC_1_EMPTY_REPLICAS.getPartitions(),
            TOPIC_1_EMPTY_REPLICAS.getReplicationFactor(),
            false),
        createTopicResponse);
  }

  @Test
  public void createTopic2_nonExistingTopic_defaultPartitionsCount_createsTopic() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(KafkaFuture.completedFuture(TOPIC_1.getPartitions().size()));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(KafkaFuture.completedFuture((int) (TOPIC_1.getReplicationFactor())));
    replay(clusterManager, adminClient, createTopicsResult);

    Topic createTopicResponse =
        topicManager
            .createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                /* partitionsCount= */ Optional.empty(),
                Optional.of(TOPIC_1.getReplicationFactor()),
                /* replicasAssignments= */ emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false)
            .get();

    verify(adminClient);
    assertEquals(
        Topic.create(
            CLUSTER_ID,
            TOPIC_1_EMPTY_REPLICAS.getName(),
            TOPIC_1_EMPTY_REPLICAS.getPartitions(),
            TOPIC_1_EMPTY_REPLICAS.getReplicationFactor(),
            false),
        createTopicResponse);
  }

  @Test
  public void createTopic2_nonExistingTopic_defaultReplicationFactor_createsTopic()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(KafkaFuture.completedFuture(TOPIC_1.getPartitions().size()));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(KafkaFuture.completedFuture((int) (TOPIC_1.getReplicationFactor())));
    replay(clusterManager, adminClient, createTopicsResult);

    Topic createTopicResponse =
        topicManager
            .createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                Optional.of(TOPIC_1.getPartitions().size()),
                /* replicationFactor= */ Optional.empty(),
                /* replicasAssignments= */ emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false)
            .get();

    verify(adminClient);
    assertEquals(
        Topic.create(
            CLUSTER_ID,
            TOPIC_1_EMPTY_REPLICAS.getName(),
            TOPIC_1_EMPTY_REPLICAS.getPartitions(),
            TOPIC_1_EMPTY_REPLICAS.getReplicationFactor(),
            false),
        createTopicResponse);
  }

  @Test
  public void createTopic2_nonExistingTopic_customReplicasAssignments_createsTopic()
      throws Exception {
    List<Integer> allReplicas = new ArrayList<>();
    for (int replicaId = 1; replicaId <= TOPIC_1.getReplicationFactor(); replicaId++) {
      allReplicas.add(replicaId);
    }
    ImmutableMap.Builder<Integer, List<Integer>> builder = new Builder<>();
    for (int partitionId = 0; partitionId < TOPIC_1.getPartitions().size(); partitionId++) {
      List<Integer> replicas = new ArrayList<>(allReplicas);
      replicas.remove(partitionId % replicas.size());
      builder.put(partitionId, replicas);
    }
    Map<Integer, List<Integer>> replicasAssignments = builder.build();

    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(KafkaFuture.completedFuture(TOPIC_1.getPartitions().size()));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(KafkaFuture.completedFuture((int) (TOPIC_1.getReplicationFactor())));
    replay(clusterManager, adminClient, createTopicsResult);

    Topic createTopicResponse =
        topicManager
            .createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                /* partitionsCount= */ Optional.empty(),
                /* replicationFactor= */ Optional.empty(),
                replicasAssignments,
                singletonMap("cleanup.policy", Optional.of("compact")),
                false)
            .get();

    verify(adminClient);
    assertEquals(
        Topic.create(
            CLUSTER_ID,
            TOPIC_1_EMPTY_REPLICAS.getName(),
            TOPIC_1_EMPTY_REPLICAS.getPartitions(),
            TOPIC_1_EMPTY_REPLICAS.getReplicationFactor(),
            false),
        createTopicResponse);
  }

  // Test for the deprecated createTopic method making sure it works for exceptional completion
  @Test
  public void createTopic_existingTopic_throwsTopicExists() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(failedFuture(new TopicExistsException("")));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(failedFuture(new TopicExistsException("")));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(failedFuture(new TopicExistsException("")));
    replay(clusterManager, adminClient, createTopicsResult);

    try {
      topicManager
          .createTopic(
              CLUSTER_ID,
              TOPIC_1.getName(),
              Optional.of(TOPIC_1.getPartitions().size()),
              Optional.of(TOPIC_1.getReplicationFactor()),
              /* replicasAssignments= */ emptyMap(),
              singletonMap("cleanup.policy", Optional.of("compact")))
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(TopicExistsException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void createTopic2_existingTopic_throwsTopicExists() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(failedFuture(new TopicExistsException("")));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(failedFuture(new TopicExistsException("")));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(failedFuture(new TopicExistsException("")));
    replay(clusterManager, adminClient, createTopicsResult);

    try {
      topicManager
          .createTopic2(
              CLUSTER_ID,
              TOPIC_1.getName(),
              Optional.of(TOPIC_1.getPartitions().size()),
              Optional.of(TOPIC_1.getReplicationFactor()),
              /* replicasAssignments= */ emptyMap(),
              singletonMap("cleanup.policy", Optional.of("compact")),
              false)
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(TopicExistsException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void createTopic2_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      topicManager
          .createTopic2(
              CLUSTER_ID,
              TOPIC_1.getName(),
              Optional.of(TOPIC_1.getPartitions().size()),
              Optional.of(TOPIC_1.getReplicationFactor()),
              /* replicasAssignments= */ emptyMap(),
              singletonMap("cleanup.policy", Optional.of("compact")),
              false)
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  // Test for the deprecated createTopic method making sure it work for successful completion
  @Test
  public void createTopic_nonExistingTopic_noAuthDescribeConfigs_createsTopic() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    replay(clusterManager, adminClient, createTopicsResult);

    topicManager
        .createTopic(
            CLUSTER_ID,
            TOPIC_1.getName(),
            Optional.of(TOPIC_1.getPartitions().size()),
            Optional.of(TOPIC_1.getReplicationFactor()),
            /* replicasAssignments= */ emptyMap(),
            singletonMap("cleanup.policy", Optional.of("compact")))
        .get();

    verify(adminClient);
  }

  @Test
  public void createTopic2_nonExistingTopic_noAuthDescribeConfigs_createsTopic() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    replay(clusterManager, adminClient, createTopicsResult);

    Topic createTopicResponse =
        topicManager
            .createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                Optional.of(TOPIC_1.getPartitions().size()),
                Optional.of(TOPIC_1.getReplicationFactor()),
                /* replicasAssignments= */ emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false)
            .get();

    verify(adminClient);
    assertEquals(
        Topic.create(
            CLUSTER_ID,
            TOPIC_1_EMPTY_REPLICAS.getName(),
            TOPIC_1_EMPTY_REPLICAS.getPartitions(),
            TOPIC_1_EMPTY_REPLICAS.getReplicationFactor(),
            false),
        createTopicResponse);
  }

  @Test
  public void
      createTopic2_nonExistingTopic_noAuthDescribeConfigs_defaultPartitionsCount_createsTopic()
          throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    replay(clusterManager, adminClient, createTopicsResult);

    Topic createTopicResponse =
        topicManager
            .createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                /* partitionsCount= */ Optional.empty(),
                Optional.of(TOPIC_1.getReplicationFactor()),
                /* replicasAssignments= */ emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false)
            .get();

    verify(adminClient);
    assertEquals(
        Topic.create(
            CLUSTER_ID,
            TOPIC_1_EMPTY_REPLICAS.getName(),
            Collections.emptyList(),
            TOPIC_1_EMPTY_REPLICAS.getReplicationFactor(),
            false),
        createTopicResponse);
  }

  @Test
  public void
      createTopic2_nonExistingTopic_noAuthDescribeConfigs_defaultReplicationFactor_createsTopic()
          throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    replay(clusterManager, adminClient, createTopicsResult);

    Topic createTopicResponse =
        topicManager
            .createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                Optional.of(TOPIC_1.getPartitions().size()),
                /* replicationFactor= */ Optional.empty(),
                /* replicasAssignments= */ emptyMap(),
                singletonMap("cleanup.policy", Optional.of("compact")),
                false)
            .get();

    verify(adminClient);
    assertEquals(
        Topic.create(
            CLUSTER_ID,
            TOPIC_1_EMPTY_REPLICAS.getName(),
            TOPIC_1_EMPTY_REPLICAS.getPartitions(),
            (short) 0,
            false),
        createTopicResponse);
  }

  @Test
  public void
      createTopic2_nonExistingTopic_noAuthDescribeConfigs_customReplicasAssignments_createsTopic()
          throws Exception {
    List<Integer> allReplicas = new ArrayList<>();
    for (int replicaId = 1; replicaId <= TOPIC_1.getReplicationFactor(); replicaId++) {
      allReplicas.add(replicaId);
    }
    ImmutableMap.Builder<Integer, List<Integer>> builder = new Builder<>();
    for (int partitionId = 0; partitionId < TOPIC_1.getPartitions().size(); partitionId++) {
      List<Integer> replicas = new ArrayList<>(allReplicas);
      replicas.remove(partitionId % replicas.size());
      builder.put(partitionId, replicas);
    }
    Map<Integer, List<Integer>> replicasAssignments = builder.build();

    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.createTopics(anyObject(), anyObject())).andReturn(createTopicsResult);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    expect(createTopicsResult.numPartitions(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    expect(createTopicsResult.replicationFactor(anyObject()))
        .andReturn(failedFuture(new TopicAuthorizationException("")));
    replay(clusterManager, adminClient, createTopicsResult);

    Topic createTopicResponse =
        topicManager
            .createTopic2(
                CLUSTER_ID,
                TOPIC_1.getName(),
                /* partitionsCount= */ Optional.of(replicasAssignments.size()),
                /* replicationFactor= */ Optional.of((short) (allReplicas.size() - 1)),
                replicasAssignments,
                singletonMap("cleanup.policy", Optional.of("compact")),
                false)
            .get();

    verify(adminClient);
    assertEquals(
        Topic.create(
            CLUSTER_ID,
            TOPIC_1_EMPTY_REPLICAS.getName(),
            TOPIC_1_EMPTY_REPLICAS.getPartitions(),
            (short) (TOPIC_1_EMPTY_REPLICAS.getReplicationFactor() - 1),
            false),
        createTopicResponse);
  }

  @Test
  public void deleteTopic_existingTopic_deletesTopic() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.deleteTopics(singletonList(TOPIC_1.getName())))
        .andReturn(deleteTopicsResult);
    expect(deleteTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null));
    replay(clusterManager, adminClient, deleteTopicsResult);

    topicManager.deleteTopic(CLUSTER_ID, TOPIC_1.getName()).get();

    verify(adminClient);
  }

  @Test
  public void deleteTopic_nonExistingTopic_throwsUnknownTopicOrPartition() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.deleteTopics(singletonList(TOPIC_1.getName())))
        .andReturn(deleteTopicsResult);
    expect(deleteTopicsResult.all())
        .andReturn(failedFuture(new UnknownTopicOrPartitionException("")));
    replay(clusterManager, adminClient, deleteTopicsResult);

    try {
      topicManager.deleteTopic(CLUSTER_ID, TOPIC_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void deleteTopic_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      topicManager.deleteTopic(CLUSTER_ID, TOPIC_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void alterTopicPartitions_validPartitionRequest_returnsTopic() {

    expect(
            adminClient.createPartitions(
                Collections.singletonMap("topicName", anyObject(NewPartitions.class))))
        .andReturn(createPartitionsResult);
    expect(createPartitionsResult.all()).andReturn(KafkaFuture.completedFuture(null));

    replay(adminClient, createPartitionsResult);

    topicManager.updateTopicPartitionsCount("topicName", 1);
    verify(adminClient);
  }

  private static Map<String, TopicDescription> createTopicDescriptionMap(
      TopicDescription... topicDescriptions) {
    HashMap<String, TopicDescription> topicDescriptionMap = new HashMap<>();
    for (TopicDescription topicDescription : topicDescriptions) {
      topicDescriptionMap.put(topicDescription.name(), topicDescription);
    }
    return topicDescriptionMap;
  }
}
