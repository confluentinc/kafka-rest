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
import io.confluent.kafkarest.entities.Topic;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
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
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.confluent.kafkarest.TestUtils.failedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TopicManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final Node NODE_1 = new Node(1, "broker-1", 9091);
  private static final Node NODE_2 = new Node(2, "broker-2", 9092);
  private static final Node NODE_3 = new Node(3, "broker-3", 9093);
  private static final List<Node> NODES = Arrays.asList(NODE_1, NODE_2, NODE_3);

  // mock data for list topics
  TopicListing topicListing1 = new TopicListing("topic1", true);
  TopicListing topicListing2 = new TopicListing("topic2", true);
  TopicListing topicListing3 = new TopicListing("topic3", false);
  private static final Map<String, TopicListing> TOPICS_MAP = new HashMap<>();

  List<Partition> partitions = new ArrayList<Partition>(Collections.singleton(
    new Partition(0, 1, null)));

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private Admin adminClient;

  @Mock
  private DescribeClusterResult describeClusterResult;
  @Mock
  private ListTopicsResult listTopicsResult;
  @Mock
  private DescribeTopicsResult describeTopicsResult;

  private TopicManagerImpl topicManager;
  private ClusterManagerImpl clusterManager;

  @Before
  public void setUp() {
    clusterManager = new ClusterManagerImpl(adminClient);
    topicManager = new TopicManagerImpl(adminClient, clusterManager);
    initializeTopicsMap();
  }

  public void initializeTopicsMap() {
    TOPICS_MAP.put(topicListing1.name(), topicListing1);
    TOPICS_MAP.put(topicListing2.name(), topicListing2);
    TOPICS_MAP.put(topicListing3.name(), topicListing3);
  }

  @Test
  public void testListTopics() throws Exception {
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(listTopicsResult.namesToListings()).andReturn(KafkaFuture.completedFuture(TOPICS_MAP));
    expect(listTopicsResult.names()).andReturn(KafkaFuture.completedFuture(TOPICS_MAP.keySet()));
    expect(listTopicsResult.listings()).andReturn(KafkaFuture.completedFuture(TOPICS_MAP.values()));
    replay(adminClient, listTopicsResult);

    List<Topic> actualTopics = topicManager.listTopics(CLUSTER_ID).get();

    List<Topic> expectedTopics = new ArrayList<>();
    expectedTopics.add(new Topic("topic1", new Properties(), partitions, 0, true));
    expectedTopics.add(new Topic("topic2", new Properties(), partitions, 0, true));
    expectedTopics.add(new Topic("topic3", new Properties(), partitions, 0, false));

    assertEquals(expectedTopics, actualTopics);
  }

  @Test
  public void testListTopics_timeoutException_returnTimeoutException() throws Exception {
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(listTopicsResult.namesToListings()).andReturn(failedFuture(new TimeoutException()));
    expect(listTopicsResult.names()).andReturn(failedFuture(new TimeoutException()));
    expect(listTopicsResult.listings()).andReturn(failedFuture(new TimeoutException()));
    replay(adminClient, listTopicsResult);

    CompletableFuture<List<Topic>> topics = topicManager.listTopics(CLUSTER_ID);

    try {
      topics.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }
}
