/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.resources.v2;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.TopicConfigManager;
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicConfig;
import io.confluent.kafkarest.entities.v2.GetTopicResponse;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TopicsResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

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

  private static final TopicConfig CONFIG_1 =
      TopicConfig.create(
          CLUSTER_ID,
          TOPIC_1.getName(),
          "config-1",
          "value-1",
          /* isDefault= */ true,
          /* isReadOnly= */ false,
          /* isSensitive= */ false,
          ConfigSource.DEFAULT_CONFIG,
          /* synonyms= */ emptyList());
  private static final TopicConfig CONFIG_2 =
      TopicConfig.create(
          CLUSTER_ID,
          TOPIC_1.getName(),
          "config-2",
          "value-2",
          /* isDefault= */ false,
          /* isReadOnly= */ true,
          /* isSensitive= */ false,
          ConfigSource.DYNAMIC_TOPIC_CONFIG,
          /* synonyms= */ emptyList());
  private static final TopicConfig CONFIG_3 =
      TopicConfig.create(
          CLUSTER_ID,
          TOPIC_1.getName(),
          "config-3",
          null,
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive= */ true,
          ConfigSource.DYNAMIC_TOPIC_CONFIG,
          /* synonyms= */ emptyList());

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private TopicManager topicManager;

  @Mock
  private TopicConfigManager topicConfigManager;

  public TopicsResourceTest() throws RestConfigException {
    super();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    addResource(new TopicsResource(() -> topicManager, () -> topicConfigManager));
    super.setUp();
  }

  @Test
  public void testList() {
    expect(topicManager.listLocalTopics())
        .andReturn(completedFuture(Arrays.asList(TOPIC_1, TOPIC_2, TOPIC_3)));
    replay(topicManager);

    Response response = request("/topics", Versions.KAFKA_V2_JSON).get();
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final List<String> topicsResponse = TestUtils
        .tryReadEntityOrLog(response, new GenericType<List<String>>() {
        });

    assertEquals(
        Arrays.asList(TOPIC_1.getName(), TOPIC_2.getName(), TOPIC_3.getName()), topicsResponse);
  }

  @Test
  public void testGetTopic() {
    expect(topicManager.getLocalTopic(TOPIC_1.getName()))
        .andReturn(completedFuture(Optional.of(TOPIC_1)));
    expect(topicConfigManager.listTopicConfigs(CLUSTER_ID, TOPIC_1.getName()))
        .andReturn(completedFuture(Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)));
    replay(topicManager, topicConfigManager);

    Response response1 = request("/topics/" + TOPIC_1.getName(), Versions.KAFKA_V2_JSON).get();
    assertOKResponse(response1, Versions.KAFKA_V2_JSON);
    final GetTopicResponse topicResponse1 =
        TestUtils.tryReadEntityOrLog(response1, GetTopicResponse.class);

    assertEquals(
        GetTopicResponse.fromTopic(TOPIC_1, Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)),
        topicResponse1);
  }

  @Test
  public void testGetInvalidTopic() {
    expect(topicManager.getLocalTopic("nonexistanttopic"))
        .andReturn(completedFuture(Optional.empty()));
    replay(topicManager);

    Response response = request("/topics/nonexistanttopic", Versions.KAFKA_V2_JSON).get();
    assertErrorResponse(Response.Status.NOT_FOUND, response,
        Errors.TOPIC_NOT_FOUND_ERROR_CODE, Errors.TOPIC_NOT_FOUND_MESSAGE,
        Versions.KAFKA_V2_JSON);
  }
}
