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

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TopicManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final Node NODE_1 = new Node(1, "broker-1", 9091);
  private static final Node NODE_2 = new Node(2, "broker-2", 9092);
  private static final Node NODE_3 = new Node(3, "broker-3", 9093);
  private static final Node NODE_4 = new Node(4, "broker-4", 9094);

  private static final List<Node> NODES = Arrays.asList(NODE_1, NODE_2, NODE_3);

  // mock data for list topics
  private static final String TOPIC_NAME = "topic1";
  private static final TopicListing TOPIC_LISTING_1 = new TopicListing("topic1", true);
  private static final TopicListing TOPIC_LISTING_2 = new TopicListing("topic2", true);
  private static final TopicListing TOPIC_LISTING_3 = new TopicListing("topic3", false);
  private static final Map<String, TopicListing> TOPICS_MAP = new HashMap<>();


  // mock data for get topic
  List<Node> REPLICAS = Arrays.asList(NODE_2, NODE_3, NODE_4);
  TopicPartitionInfo TOPIC_PARTITION_INFO = new TopicPartitionInfo(0, NODE_1, REPLICAS, REPLICAS);
  TopicDescription TOPIC_DESCRIPTION = new TopicDescription("topic1", true,
      Collections.singletonList(TOPIC_PARTITION_INFO),new HashSet<>());
  Map<String, TopicDescription> TOPIC_DESC_MAP = new HashMap<>();

  PartitionReplica PARTITION_REPLICA_1 = new PartitionReplica(2, false, true);
  PartitionReplica PARTITION_REPLICA_2 = new PartitionReplica(3, false, true);
  PartitionReplica PARTITION_REPLICA_3 = new PartitionReplica(4, false, true);

  List<PartitionReplica> partitionReplicas = new ArrayList<>();

  List<Partition> PARTITIONS = new ArrayList<>();

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private Admin adminClient;

  @Mock
  private DescribeClusterResult describeClusterResult;
  @Mock
  private ListTopicsResult listTopicsResult;
  @Mock
  private DescribeTopicsResult describeTopicResult;

  private TopicManagerImpl topicManager;
  private ClusterManagerImpl clusterManager;

  @Before
  public void setUp() {
    clusterManager = new ClusterManagerImpl(adminClient);
    topicManager = new TopicManagerImpl(adminClient, clusterManager);
    populateMockData();
  }

  public void populateMockData() {
    TOPICS_MAP.put(TOPIC_LISTING_1.name(), TOPIC_LISTING_1);
    TOPICS_MAP.put(TOPIC_LISTING_2.name(), TOPIC_LISTING_2);
    TOPICS_MAP.put(TOPIC_LISTING_3.name(), TOPIC_LISTING_3);
    TOPIC_DESC_MAP.put(TOPIC_NAME, TOPIC_DESCRIPTION);
    partitionReplicas.add(PARTITION_REPLICA_1);
    partitionReplicas.add(PARTITION_REPLICA_2);
    partitionReplicas.add(PARTITION_REPLICA_3);
    Partition partition = new Partition(0, 1, partitionReplicas);
    PARTITIONS.add(partition);
  }

  @Test
  public void testListTopics() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(listTopicsResult.listings()).andReturn(KafkaFuture.completedFuture(TOPICS_MAP.values()));
    expect(adminClient.describeTopics(anyObject())).andReturn(describeTopicResult);
    expect(describeTopicResult.all()).andReturn(KafkaFuture.completedFuture(TOPIC_DESC_MAP));
    replay(adminClient, listTopicsResult, describeClusterResult, describeTopicResult);

    List<Topic> actualTopics = topicManager.listTopics(CLUSTER_ID).get();

    List<Topic> expectedTopics = new ArrayList<>();
    expectedTopics.add(new Topic(TOPIC_NAME, new Properties(), PARTITIONS, 3, true, CLUSTER_ID));
    assertEquals(expectedTopics, actualTopics);
  }

  @Test
  public void testListTopics_timeoutException_returnTimeoutException() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(listTopicsResult.listings()).andReturn(failedFuture(new TimeoutException()));
    replay(adminClient, describeClusterResult, listTopicsResult);

    CompletableFuture<List<Topic>> topics = topicManager.listTopics(CLUSTER_ID);

    try {
      topics.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }

  @Test
  public void testListTopic_differentClusterId_returnsEmptyList() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    replay(adminClient, describeClusterResult);

    List<Topic> topics = topicManager.listTopics("2").get();
    assertTrue(topics.isEmpty());
  }

  @Test
  public void testGetTopic_ownClusterId_returnsTopic() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    expect(adminClient.describeTopics(anyObject())).andReturn(describeTopicResult);
    expect(describeTopicResult.all()).andReturn(KafkaFuture.completedFuture(TOPIC_DESC_MAP));
    replay(adminClient, describeTopicResult, describeClusterResult);

    Topic topic = topicManager.getTopic(CLUSTER_ID, TOPIC_NAME).get().get();
    Topic expectedTopic =
        new Topic(TOPIC_NAME,
            new Properties(),
            PARTITIONS,
            3,
            true,
            CLUSTER_ID
            );

    assertEquals(expectedTopic, topic);
  }

  @Test
  public void testGetTopic_differentClusterId_returnsNull() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    expect(adminClient.describeTopics(anyObject())).andReturn(describeTopicResult);
    expect(describeTopicResult.all()).andReturn(KafkaFuture.completedFuture(TOPIC_DESC_MAP));
    replay(adminClient, describeTopicResult, describeClusterResult);

    Optional<Topic> topic = topicManager.getTopic("2", TOPIC_NAME).get();
    assertFalse(topic.isPresent());
  }

  private static <T> KafkaFuture<T> failedFuture(RuntimeException exception) {
    KafkaFuture<T> future = KafkaFuture.completedFuture(null);
    return future.whenComplete((value, error) -> { throw exception; });
  }
}
