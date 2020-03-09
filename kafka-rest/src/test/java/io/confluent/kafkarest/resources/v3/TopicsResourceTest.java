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

import static io.confluent.kafkarest.resources.v3.CompletableFutures.failedFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TopicsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Topic TOPIC_1 =
      new Topic(
          CLUSTER_ID,
          "topic-1",
          new Properties(),
          Arrays.asList(
              new Partition(
                  /* partition= */ 0,
                  /* leader= */ 1,
                  Arrays.asList(
                      new PartitionReplica(1, /* leader= */ true, /* inSync= */ true),
                      new PartitionReplica(2, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(3, /* leader= */ false, /* inSync= */ false))),
              new Partition(
                  /* partition= */ 1,
                  /* leader= */ 2,
                  Arrays.asList(
                      new PartitionReplica(1, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(2, /* leader= */ true, /* inSync= */ true),
                      new PartitionReplica(3, /* leader= */ false, /* inSync= */ false))),
              new Partition(
                  /* partition= */2,
                  /* leader= */ 3,
                  Arrays.asList(
                      new PartitionReplica(1, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(2, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(3, /* leader= */ true, /* inSync= */ true)))),
          /* replicationFactor= */ 3,
          /* isInternal= */ true);

  private static final Topic TOPIC_2 =
      new Topic(
          CLUSTER_ID,
          "topic-2",
          new Properties(),
          Arrays.asList(
              new Partition(
                  /* partition= */ 0,
                  /* leader= */ 3,
                  Arrays.asList(
                      new PartitionReplica(1, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(2, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(3, /* leader= */ true, /* inSync= */ true))),
              new Partition(
                  /* partition= */ 1,
                  /* leader= */ 1,
                  Arrays.asList(
                      new PartitionReplica(1, /* leader= */ true, /* inSync= */ true),
                      new PartitionReplica(2, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(3, /* leader= */ false, /* inSync= */ false))),
              new Partition(
                  /* partition= */2,
                  /* leader= */ 2,
                  Arrays.asList(
                      new PartitionReplica(1, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(2, /* leader= */ true, /* inSync= */ true),
                      new PartitionReplica(3, /* leader= */ false, /* inSync= */ false)))),
          /* replicationFactor= */ 3,
          /* isInternal= */ true);

  private static final Topic TOPIC_3 =
      new Topic(
          CLUSTER_ID,
          "topic-3",
          new Properties(),
          Arrays.asList(
              new Partition(
                  /* partition= */ 0,
                  /* leader= */ 2,
                  Arrays.asList(
                      new PartitionReplica(1, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(2, /* leader= */ true, /* inSync= */ true),
                      new PartitionReplica(3, /* leader= */ false, /* inSync= */ false))),
              new Partition(
                  /* partition= */ 1,
                  /* leader= */ 3,
                  Arrays.asList(
                      new PartitionReplica(1, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(2, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(3, /* leader= */ true, /* inSync= */ true))),
              new Partition(
                  /* partition= */2,
                  /* leader= */ 1,
                  Arrays.asList(
                      new PartitionReplica(1, /* leader= */ true, /* inSync= */ true),
                      new PartitionReplica(2, /* leader= */ false, /* inSync= */ false),
                      new PartitionReplica(3, /* leader= */ false, /* inSync= */ false)))),
          /* replicationFactor= */ 3,
          /* isInternal= */ false);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private TopicManager topicManager;

  private TopicsResource topicsResource;

  @Before
  public void setUp() {
    topicsResource = new TopicsResource(topicManager, new FakeUrlFactory());
  }

  @Test
  public void listTopics_existingCluster_returnsTopics() {
    expect(topicManager.listTopics(CLUSTER_ID))
        .andReturn(completedFuture(Arrays.asList(TOPIC_1, TOPIC_2, TOPIC_3)));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, CLUSTER_ID);

    ListTopicsResponse expected =
        new ListTopicsResponse(
            new CollectionLink("/v3/clusters/cluster-1/topics", null),
            Arrays.asList(
                new TopicData(
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-1"),
                    "cluster-1",
                    "topic-1",
                    /* isInternal= */ true,
                    /* replicationFactor= */ 3,
                    new Relationship("/v3/clusters/cluster-1/topics/topic-1/configurations"),
                    new Relationship("/v3/clusters/cluster-1/topics/topic-1/partitions")),
                new TopicData(
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-2"),
                    "cluster-1",
                    "topic-2",
                    /* isInternal= */ true,
                    /* replicationFactor= */ 3,
                    new Relationship("/v3/clusters/cluster-1/topics/topic-2/configurations"),
                    new Relationship("/v3/clusters/cluster-1/topics/topic-2/partitions")),
                new TopicData(
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-3"),
                    "cluster-1",
                    "topic-3",
                    /* isInternal= */ false,
                    /* replicationFactor= */ 3,
                    new Relationship("/v3/clusters/cluster-1/topics/topic-3/configurations"),
                    new Relationship("/v3/clusters/cluster-1/topics/topic-3/partitions"))));

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

    GetTopicResponse expected = new GetTopicResponse(
        new TopicData(
            new ResourceLink("/v3/clusters/cluster-1/topics/topic-1"),
            "cluster-1",
            "topic-1",
            /* isInternal= */ true,
            /* replicationFactor= */ 3,
            new Relationship("/v3/clusters/cluster-1/topics/topic-1/configurations"),
            new Relationship("/v3/clusters/cluster-1/topics/topic-1/partitions")));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getTopics_nonexistingCluster_throwsNotFoundException() {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_1.getName())).
        andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, CLUSTER_ID, TOPIC_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopics_existingCluster_nonexistingTopic_throwsNotFound() {
    expect(topicManager.getTopic(TOPIC_1.getClusterId(), TOPIC_1.getName()))
        .andReturn(completedFuture(Optional.empty()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
