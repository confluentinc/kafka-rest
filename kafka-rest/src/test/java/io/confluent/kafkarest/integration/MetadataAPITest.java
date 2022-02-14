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
package io.confluent.kafkarest.integration;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static io.confluent.kafkarest.TestUtils.tryReadEntityOrLog;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v2.BrokerList;
import io.confluent.kafkarest.entities.v2.GetPartitionResponse;
import io.confluent.kafkarest.entities.v2.GetTopicResponse;
import io.confluent.rest.exceptions.KafkaExceptionMapper;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests metadata access against a real cluster. This isn't exhaustive since the unit tests cover
 * corner cases; rather it verifies the basic functionality works against a real cluster.
 */
public class MetadataAPITest extends ClusterTestHarness {

  private static final String topic1Name = "topic1";
  private static final List<Partition> topic1Partitions =
      singletonList(
          Partition.create(
              /* clusterId= */ "",
              "topic1",
              /* partitionId= */ 0,
              Arrays.asList(
                  PartitionReplica.create(/* clusterId= */ "", "topic1", 0, 0, true, true),
                  PartitionReplica.create(/* clusterId= */ "", "topic1", 0, 1, false, false))));
  private static final Topic topic1 =
      Topic.create("", topic1Name, topic1Partitions, (short) 2, false, emptySet());
  private static final String topic2Name = "topic2";
  private static final List<Partition> topic2Partitions =
      Arrays.asList(
          Partition.create(
              /* clusterId= */ "",
              "topic2",
              /* partitionId= */ 0,
              Arrays.asList(
                  PartitionReplica.create(/* clusterId= */ "", "topic2", 0, 0, true, true),
                  PartitionReplica.create(/* clusterId= */ "", "topic2", 0, 1, false, false))),
          Partition.create(
              /* clusterId= */ "",
              "topic2",
              /* partitionId= */ 1,
              Arrays.asList(
                  PartitionReplica.create(/* clusterId= */ "", "topic2", 1, 0, false, true),
                  PartitionReplica.create(/* clusterId= */ "", "topic2", 1, 1, true, true))));
  private static final Properties topic2Configs;

  static {
    topic2Configs = new Properties();
    topic2Configs.setProperty("cleanup.policy", "delete");
  }

  private static final short numReplicas = 2;

  public MetadataAPITest() {
    super(2, false);
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTopic(topic1Name, topic1Partitions.size(), numReplicas);
    createTopic(topic2Name, topic2Partitions.size(), numReplicas);
  }

  @Test
  public void testBrokers() throws InterruptedException {
    // Listing
    Response response = request("/brokers").get();
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final BrokerList brokers = tryReadEntityOrLog(response, BrokerList.class);
    assertEquals(new BrokerList(Arrays.asList(0, 1)), brokers);
  }

  /* This should work, but due to the lack of timeouts in ZkClient, if ZK is down some calls
   *  will just block forever, see https://issues.apache.org/jira/browse/KAFKA-1907. We should
   *  reenable this once we can apply timeouts to ZK operations.
   */
  /*
    @Test
    public void testZkFailure() throws InterruptedException {
      // Kill ZK so the request will generate an error.
      zookeeper.shutdown();

      // Since this is handled via an ExceptionMapper, testing one endpoint is good enough
      Response response = request("/brokers").get();
      assertErrorResponse(Response.Status.INTERNAL_SERVER_ERROR, response,
                          Errors.ZOOKEEPER_ERROR_ERROR_CODE, Errors.ZOOKEEPER_ERROR_MESSAGE,
                          Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    }
  */

  @Test
  public void testTopicsList() throws InterruptedException {
    // Listing
    testWithRetry(
        () -> {
          Response response = request("/topics").get();
          assertOKResponse(response, Versions.KAFKA_V2_JSON);
          final List<String> topicsResponse =
              tryReadEntityOrLog(response, new GenericType<List<String>>() {});
          assertEquals(Arrays.asList(topic1Name, topic2Name), topicsResponse);
        });

    // Get topic
    Response response1 = request("/topics/{topic}", "topic", topic1Name).get();
    assertOKResponse(response1, Versions.KAFKA_V2_JSON);
    final GetTopicResponse topic1Response = tryReadEntityOrLog(response1, GetTopicResponse.class);
    // Just verify some basic properties because the exact values can vary based on replica
    // assignment, leader election
    assertEquals(topic1.getName(), topic1Response.getName());
    // admin client provides default configs as well and hence not asserting for now
    //    assertEquals(topic1.getConfigs(), topic1Response.getConfigs());
    assertEquals(topic1Partitions.size(), topic1Response.getPartitions().size());
    assertEquals(numReplicas, topic1Response.getPartitions().get(0).getReplicas().size());

    // Get invalid topic
    final Response invalidResponse = request("/topics/{topic}", "topic", "topicdoesntexist").get();
    assertErrorResponse(
        Response.Status.NOT_FOUND,
        invalidResponse,
        KafkaExceptionMapper.KAFKA_UNKNOWN_TOPIC_PARTITION_CODE,
        null,
        Versions.KAFKA_V2_JSON);
  }

  @Test
  public void testPartitionsList() throws InterruptedException {
    // Listing

    testWithRetry(() -> verifyPartitionGet(topic1Name, 2, 1));
    testWithRetry(() -> verifyPartitionGet(topic2Name, 2, 2));

    // Get single partition
    // No need to retry, because we know the topic has been made by this point as the above
    // assertions pass
    Response response = request("/topics/" + topic1Name + "/partitions/0").get();
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final GetPartitionResponse getPartitionResponse =
        tryReadEntityOrLog(response, GetPartitionResponse.class);
    assertEquals((Integer) 0, getPartitionResponse.getPartition());
    assertEquals(numReplicas, getPartitionResponse.getReplicas().size());

    // Get invalid partition
    final Response invalidResponse = request("/topics/topic1/partitions/1000").get();
    assertErrorResponse(
        Response.Status.NOT_FOUND,
        invalidResponse,
        Errors.PARTITION_NOT_FOUND_ERROR_CODE,
        Errors.PARTITION_NOT_FOUND_MESSAGE,
        Versions.KAFKA_V2_JSON);
  }

  private void verifyPartitionGet(String topicName, int numReplicas, int numPartitions) {
    Response response = request("/topics/" + topicName + "/partitions").get();
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    List<GetPartitionResponse> partitionsResponse =
        tryReadEntityOrLog(response, new GenericType<List<GetPartitionResponse>>() {});
    // Just verify some basic properties because the exact values can vary based on replica
    // assignment, leader election
    assertEquals(numPartitions, partitionsResponse.size());
    for (int i = 0; i < numPartitions; i++) {
      assertEquals(numReplicas, partitionsResponse.get(i).getReplicas().size());
    }
  }
}
