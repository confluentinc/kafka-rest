/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafkarest.integration.v3;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.testing.DefaultKafkaRestTestEnvironment;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("IntegrationTest")
public class ProduceActionRateLimitIntegrationTest {

  private static final String TOPIC_NAME = "topic-1";

  @RegisterExtension
  public final DefaultKafkaRestTestEnvironment testEnv = new DefaultKafkaRestTestEnvironment(false);

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    Properties restConfigs = new Properties();
    // Adding custom KafkaRestConfigs for individual test-cases/test-methods below.
    if (testInfo.getDisplayName().contains("CallerIsRateLimited")) {
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_ENABLE_CONFIG, "true");
      restConfigs.put(KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED, "true");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_BACKEND_CONFIG, "resilience4j");
      // The happy-path testing, i.e. rest calls below threshold succeed are already covered by the
      // other existing tests. The 4 tests below, 1 per rate-limit config, set a very low rate-limit
      // of "1", to deterministically make sure limits apply and rest-calls see 429s.
      if (testInfo
          .getDisplayName()
          .contains("test_whenGlobalByteLimitReached_thenCallerIsRateLimited")) {

        restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_GLOBAL_PER_SECOND, "1");
      }
      if (testInfo
          .getDisplayName()
          .contains("test_whenClusterByteLimitReached_thenCallerIsRateLimited")) {

        restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND, "1");
      }
      if (testInfo
          .getDisplayName()
          .contains("test_whenGlobalRequestCountLimitReached_thenCallerIsRateLimited")) {

        restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_GLOBAL_PER_SECOND, "1");
      }
      if (testInfo
          .getDisplayName()
          .contains("test_whenClusterRequestCountLimitReached_thenCallerIsRateLimited")) {

        restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND, "1");
      }
    }
    testEnv.kafkaRest().startApp(restConfigs);

    testEnv.kafkaCluster().createTopic(TOPIC_NAME, 3, (short) 1);
  }

  @AfterEach
  public void tearDown() {
    testEnv.kafkaRest().closeApp();
  }

  private void doByteLimitReachedTest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf(value))
                    .build())
            // 0 value here is meaningless and only set as originalSize is mandatory for AutoValue.
            // Value set here is ignored any-ways, as "true" originalSize is calculated & set,
            // when the JSON request is de-serialized into a ProduceRecord object on the
            // server-side.
            .setOriginalSize(0L)
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ErrorResponse> actual = readErrorResponses(response);
    assertEquals(actual.size(), 1);
    // Check request was rate-limited, so return http error-code is 429.
    // NOTE - Byte rate-limit is set as 1 in setup() making sure 1st request itself fails.
    assertEquals(actual.get(0).getErrorCode(), 429);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  @DisplayName("test_whenGlobalByteLimitReached_thenCallerIsRateLimited")
  public void test_whenGlobalByteLimitReached_thenCallerIsRateLimited(String quorum)
      throws Exception {
    doByteLimitReachedTest();
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  @DisplayName("test_whenClusterByteLimitReached_thenCallerIsRateLimited")
  public void test_whenClusterByteLimitReached_thenCallerIsRateLimited(String quorum)
      throws Exception {
    doByteLimitReachedTest();
  }

  private void doCountLimitTest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf(value))
                    .build())
            .setOriginalSize(0L)
            .build();

    Response response1 =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    Response response2 =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response1.getStatus());

    assertEquals(Status.OK.getStatusCode(), response2.getStatus());
    List<ErrorResponse> actual = readErrorResponses(response2);
    assertEquals(actual.size(), 1);
    // Check request was rate-limited, so return http error-code is 429.
    // NOTE - Count rate-limit is set as 1 in setup() making sure 2nd request fails
    // deterministically.
    assertEquals(actual.get(0).getErrorCode(), 429);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  @DisplayName("test_whenGlobalRequestCountLimitReached_thenCallerIsRateLimited")
  public void test_whenGlobalRequestCountLimitReached_thenCallerIsRateLimited(String quorum)
      throws Exception {
    doCountLimitTest();
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  @DisplayName("test_whenClusterRequestCountLimitReached_thenCallerIsRateLimited")
  public void test_whenClusterRequestCountLimitReached_thenCallerIsRateLimited(String quorum)
      throws Exception {
    doCountLimitTest();
  }

  private static ImmutableList<ErrorResponse> readErrorResponses(Response response) {
    return ImmutableList.copyOf(
        response.readEntity(new GenericType<MappingIterator<ErrorResponse>>() {}));
  }
}
