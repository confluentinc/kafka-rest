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
 * specific language governing permissions and limitations under the License
 */

package io.confluent.kafkarest.integration;

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.node.BinaryNode;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.ratelimit.RateLimitExceededException.ErrorCodes;
import io.confluent.kafkarest.requestlog.CustomLogRequestAttributes;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.server.RequestLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests to validate request-log has necessary custom-log when request is rate-limited.
 */
@Tag("IntegrationTest")
public class CustomLogIntegrationTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(CustomLogIntegrationTest.class);

  private ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);

  private final ConcurrentLinkedQueue<String> logEntries = new ConcurrentLinkedQueue<>();

  private static final String topicName = "topic-1";

  private Properties restConfigs = new Properties();

  public CustomLogIntegrationTest() {
    super(1, false, false);
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.putAll(restConfigs);
  }

  @Override
  /** Override to do nothing, as this class has an overloaded setup() below. */
  public void setUp() {}

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp();

    restConfigs.clear();
    // Adding custom KafkaRestConfigs for individual test-cases/test-methods below.
    // Make the "other" rate-limit large to make sure they doesn't trigger.
    restConfigs.put("dos.filter.delay.ms", -1);
    if (testInfo
        .getDisplayName()
        .contains("test_whenJettyGlobalRateLimitTriggered_ThenRequestLogHasRelevantInfo")) {
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put("dos.filter.max.requests.per.sec", 1);
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 99999);
    } else if (testInfo
        .getDisplayName()
        .contains("test_whenNoRateLimitTriggered_ThenRequestLogHasRelevantInfo")) {
      // Set all rate-limits very high
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put("dos.filter.max.requests.per.sec", 99999);
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 99999);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_ENABLE_CONFIG, "true");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_BACKEND_CONFIG, "resilience4j");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_PERMITS_PER_SEC_CONFIG, 99999);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_PER_CLUSTER_PERMITS_PER_SEC_CONFIG, 99999);
    } else if (testInfo
        .getDisplayName()
        .contains("test_whenCustomLoggingDisabled_ThenRequestLogDoesntHaveCustomInfo")) {
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put(KafkaRestConfig.USE_CUSTOM_REQUEST_LOGGING_CONFIG, "false");
      restConfigs.put("dos.filter.max.requests.per.sec", 1);
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 99999);
    } else if (testInfo
        .getDisplayName()
        .contains("test_whenJettyNonGlobalRateLimitTriggered_ThenRequestLogHasRelevantInfo")) {
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 1);
      restConfigs.put("dos.filter.max.requests.per.sec", 99999);
    } else if (testInfo
        .getDisplayName()
        .contains("test_whenGlobalPermitRateLimitTriggered_ThenRequestLogHasRelevantInfo")) {
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 99999);
      restConfigs.put("dos.filter.max.requests.per.sec", 99999);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_ENABLE_CONFIG, "true");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_BACKEND_CONFIG, "resilience4j");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_PERMITS_PER_SEC_CONFIG, 1);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_PER_CLUSTER_PERMITS_PER_SEC_CONFIG, 99999);
    } else if (testInfo
        .getDisplayName()
        .contains("test_whenPerClusterPermitRateLimitTriggered_ThenRequestLogHasRelevantInfo")) {
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 99999);
      restConfigs.put("dos.filter.max.requests.per.sec", 99999);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_ENABLE_CONFIG, "true");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_BACKEND_CONFIG, "resilience4j");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_PERMITS_PER_SEC_CONFIG, 99999);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_PER_CLUSTER_PERMITS_PER_SEC_CONFIG, 1);
    } else if (testInfo
        .getDisplayName()
        .contains(
            "test_whenGlobalProduceRequestsRateLimitTriggered_ThenRequestLogHasRelevantInfo")) {
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 99999);
      restConfigs.put("dos.filter.max.requests.per.sec", 99999);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_ENABLE_CONFIG, "true");
      restConfigs.put(KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED, "true");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_BACKEND_CONFIG, "resilience4j");
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_GLOBAL_PER_SECOND, 1);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND, 99999);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_GLOBAL_PER_SECOND, 99999);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND, 99999);
    } else if (testInfo
        .getDisplayName()
        .contains(
            "test_whenPerTenantProduceRequestsRateLimitTriggered_ThenRequestLogHasRelevantInfo")) {
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 99999);
      restConfigs.put("dos.filter.max.requests.per.sec", 99999);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_ENABLE_CONFIG, "true");
      restConfigs.put(KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED, "true");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_BACKEND_CONFIG, "resilience4j");
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_GLOBAL_PER_SECOND, 99999);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND, 1);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_GLOBAL_PER_SECOND, 99999);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND, 99999);
    } else if (testInfo
        .getDisplayName()
        .contains("test_whenGlobalProduceBytesRateLimitTriggered_ThenRequestLogHasRelevantInfo")) {
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 99999);
      restConfigs.put("dos.filter.max.requests.per.sec", 99999);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_ENABLE_CONFIG, "true");
      restConfigs.put(KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED, "true");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_BACKEND_CONFIG, "resilience4j");
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_GLOBAL_PER_SECOND, 99999);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND, 99999);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_GLOBAL_PER_SECOND, 1);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND, 99999);
    } else if (testInfo
        .getDisplayName()
        .contains(
            "test_whenPerTenantProduceBytesRateLimitTriggered_ThenRequestLogHasRelevantInfo")) {
      restConfigs.put("dos.filter.enabled", "true");
      restConfigs.put("dos.filter.max.requests.per.connection.per.sec", 99999);
      restConfigs.put("dos.filter.max.requests.per.sec", 99999);
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_ENABLE_CONFIG, "true");
      restConfigs.put(KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED, "true");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_BACKEND_CONFIG, "resilience4j");
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_GLOBAL_PER_SECOND, 99999);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND, 99999);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_GLOBAL_PER_SECOND, 99999);
      restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND, 1);
    } else {
      // Produce rate limit configs
      throw new Exception("Invalid test, doesn't have rest-configs defined in setup.");
    }

    // Setup KafkaRest with custom-request-logging.
    // And also setup dosfilter-listeners to populate request-attributes for logging
    // on Jetty dos-filters being triggered.
    logEntries.clear();
    CustomLogIntegrationTest.TestRequestLogWriter logWriter =
        new CustomLogIntegrationTest.TestRequestLogWriter();
    startRest(logWriter, "%s");

    createTopic(topicName, 1, (short) 1);
  }

  @AfterEach
  public void tearDown() throws Exception {
    stopRest();
  }

  @Test
  @DisplayName("test_whenCustomLoggingDisabled_ThenRequestLogDoesntHaveCustomInfo")
  public void test_whenCustomLoggingDisabled_ThenRequestLogDoesntHaveCustomInfo() {
    int totalRequests = 100;
    // Since rate-limit is 1.
    int requestsWithStatusOk = 1;
    String url = "/v3/clusters/" + getClusterId() + "/topics/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    // Since custom logging disabled, this will check rate-limited request don't have an extra
    // error-code at the end.
    verifyLog(
        totalRequests,
        ErrorCodes.DOS_FILTER_MAX_REQUEST_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        false,
        true);
  }

  /*
   * All the test below are run with custom-logging enabled in kafka-rest.
   */

  @Test
  @DisplayName("test_whenNoRateLimitTriggered_ThenRequestLogHasRelevantInfo")
  public void test_whenNoRateLimitTriggered_ThenRequestLogHasRelevantInfo() {
    int totalRequests = 100;
    int requestsWithStatusOk = 100;
    String url = "/v3/clusters/" + getClusterId() + "/topics/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    // Check all requests are logged with status 200, and there is no extra error-code at the end.
    verifyLog(
        totalRequests,
        ErrorCodes.DOS_FILTER_MAX_REQUEST_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        false,
        false);
  }

  @Test
  @DisplayName("test_whenJettyGlobalRateLimitTriggered_ThenRequestLogHasRelevantInfo")
  public void test_whenJettyGlobalRateLimitTriggered_ThenRequestLogHasRelevantInfo() {
    int totalRequests = 100;
    // Since rate-limit is 1.
    int requestsWithStatusOk = 1;
    String url = "/v3/clusters/" + getClusterId() + "/topics/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    verifyLog(
        totalRequests,
        ErrorCodes.DOS_FILTER_MAX_REQUEST_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        false,
        false);
  }

  @Test
  @DisplayName("test_whenJettyNonGlobalRateLimitTriggered_ThenRequestLogHasRelevantInfo")
  public void test_whenJettyNonGlobalRateLimitTriggered_ThenRequestLogHasRelevantInfo() {
    int totalRequests = 100;
    // Since rate-limit is 1.
    int requestsWithStatusOk = 1;
    String clusterId = getClusterId();
    String url = "/v3/clusters/" + getClusterId() + "/topics/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    verifyLog(
        totalRequests,
        ErrorCodes.DOS_FILTER_MAX_REQUEST_PER_CONNECTION_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        false,
        false);
  }

  @Test
  @DisplayName("test_whenGlobalPermitRateLimitTriggered_ThenRequestLogHasRelevantInfo")
  public void test_whenGlobalPermitRateLimitTriggered_ThenRequestLogHasRelevantInfo() {
    // This is global-request-rate-limiter at the Jersey layer.
    int totalRequests = 100;
    // Since rate-limit is 1.
    int requestsWithStatusOk = 1;
    String url = "/v3/clusters/" + getClusterId() + "/topics/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    verifyLog(
        totalRequests,
        ErrorCodes.PERMITS_MAX_GLOBAL_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        false,
        false);
  }

  @Test
  @DisplayName("test_whenPerClusterPermitRateLimitTriggered_ThenRequestLogHasRelevantInfo")
  public void test_whenPerClusterPermitRateLimitTriggered_ThenRequestLogHasRelevantInfo() {
    // This is the per-tenant request-rate-limiter at the Jersey Layer
    int totalRequests = 100;
    // Since rate-limit is 1.
    int requestsWithStatusOk = 1;
    String url = "/v3/clusters/" + getClusterId() + "/topics/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    verifyLog(
        totalRequests,
        ErrorCodes.PERMITS_MAX_PER_CLUSTER_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        false,
        false);
  }

  @Test
  @DisplayName("test_whenGlobalProduceRequestsRateLimitTriggered_ThenRequestLogHasRelevantInfo")
  public void test_whenGlobalProduceRequestsRateLimitTriggered_ThenRequestLogHasRelevantInfo() {
    int totalRequests = 100;
    // Since rate-limit is 1.
    int requestsWithStatusOk = 1;
    String url = "/v3/clusters/" + getClusterId() + "/topics/" + topicName + "/records/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    verifyLog(
        totalRequests,
        ErrorCodes.PRODUCE_MAX_REQUESTS_GLOBAL_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        true,
        false);
  }

  @Test
  @DisplayName("test_whenPerTenantProduceRequestsRateLimitTriggered_ThenRequestLogHasRelevantInfo")
  public void test_whenPerTenantProduceRequestsRateLimitTriggered_ThenRequestLogHasRelevantInfo() {
    int totalRequests = 100;
    // Since rate-limit is 1.
    int requestsWithStatusOk = 1;
    String url = "/v3/clusters/" + getClusterId() + "/topics/" + topicName + "/records/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    verifyLog(
        totalRequests,
        ErrorCodes.PRODUCE_MAX_REQUESTS_PER_TENANT_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        true,
        false);
  }

  @Test
  @DisplayName("test_whenGlobalProduceBytesRateLimitTriggered_ThenRequestLogHasRelevantInfo")
  public void test_whenGlobalProduceBytesRateLimitTriggered_ThenRequestLogHasRelevantInfo() {
    int totalRequests = 100;
    // Since rate-limit is 1, so event first request will have enough data to get rate-limited
    int requestsWithStatusOk = 0;
    String url = "/v3/clusters/" + getClusterId() + "/topics/" + topicName + "/records/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    verifyLog(
        totalRequests,
        ErrorCodes.PRODUCE_MAX_BYTES_GLOBAL_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        true,
        false);
  }

  @Test
  @DisplayName("test_whenPerTenantProduceBytesRateLimitTriggered_ThenRequestLogHasRelevantInfo")
  public void test_whenPerTenantProduceBytesRateLimitTriggered_ThenRequestLogHasRelevantInfo() {
    int totalRequests = 100;
    // Since rate-limit is 1, so event first request will have enough data to get rate-limited
    int requestsWithStatusOk = 0;
    String url = "/v3/clusters/" + getClusterId() + "/topics/" + topicName + "/records/";
    hammerAtConstantRate(url, Duration.ofMillis(1), 100);
    verifyLog(
        totalRequests,
        ErrorCodes.PRODUCE_MAX_BYTES_PER_TENANT_LIMIT_EXCEEDED,
        totalRequests - requestsWithStatusOk,
        true,
        false);
  }

  class TestRequestLogWriter implements RequestLog.Writer {

    AtomicInteger entryCounter = new AtomicInteger();

    @Override
    public void write(String requestEntry) {
      try {
        boolean added = logEntries.add(requestEntry);
        assertTrue(added, "Failed to add entry to log, unexpected.");
        log.info("Log #{}:{}", entryCounter.getAndIncrement(), requestEntry);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private void hammerAtConstantRate(String path, Duration requestInterval, int totalRequests) {
    checkArgument(!requestInterval.isNegative(), "rate must be non-negative");
    List<Response> responses =
        IntStream.range(0, totalRequests)
            .mapToObj(
                i -> {
                  if (path.contains("/records")) {
                    // Produce API call
                    ByteString key = ByteString.copyFromUtf8("foo");
                    ByteString value = ByteString.copyFromUtf8("bar");
                    ProduceRequest request =
                        ProduceRequest.builder()
                            .setKey(
                                ProduceRequestData.builder()
                                    .setFormat(EmbeddedFormat.BINARY)
                                    .setData(BinaryNode.valueOf(key.toByteArray()))
                                    .build())
                            .setValue(
                                ProduceRequestData.builder()
                                    .setFormat(EmbeddedFormat.BINARY)
                                    .setData(BinaryNode.valueOf(value.toByteArray()))
                                    .build())
                            .setOriginalSize(0L)
                            .build();
                    return executor.schedule(
                        () ->
                            request(path)
                                .accept(MediaType.APPLICATION_JSON)
                                .post(Entity.entity(request, MediaType.APPLICATION_JSON)),
                        /* delay= */ i * requestInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
                  } else {
                    return executor.schedule(
                        () -> request(path).accept(MediaType.APPLICATION_JSON).get(),
                        /* delay= */ i * requestInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
                  }
                })
            .collect(Collectors.toList()).stream()
            .map(
                future -> {
                  try {
                    return future.get();
                  } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    for (Response response : responses) {
      assertTrue(
          !response.getHeaders().containsKey(CustomLogRequestAttributes.REST_ERROR_CODE),
          "Unexpected header in headers " + response.getHeaders());
      int status = response.getStatus();
      if (status != 200 && status != 429) {
        fail(
            String.format(
                "Expected HTTP 200 or HTTP 429, but got HTTP %d instead: %s",
                status, response.readEntity(String.class)));
      }
    }
  }

  private void verifyLog(
      int expectedNumOfEntries,
      int errorCodeInLog,
      int expectedRateLimitLogs,
      boolean isProduce,
      boolean isCustomLoggingDisabled) {
    // Sleep so all request-logs are logged before reading and validating them here.
    try {
      TimeUnit.SECONDS.sleep(2);
    } catch (InterruptedException e) {
      assertTrue(false, "Unexpectedly failed to sleep.");
    }
    String okStatusLogEntry = "200 -";
    String rateLimitedLogEntry = "429 " + errorCodeInLog;
    if (isProduce) {
      // When produce-api gets rate-limited by produce-rate-limiters, then http-response-code still
      // is 200. Though status-code at record receipt would be 429.
      rateLimitedLogEntry = "200 " + errorCodeInLog;
    }
    int rateLimitedRequests = 0;
    int totalRequests = 0;
    while (true) {
      String entry = logEntries.poll();
      if (entry == null) {
        break;
      }
      assertTrue(
          okStatusLogEntry.equals(entry) || rateLimitedLogEntry.equals(entry),
          "Log entry is <"
              + entry
              + "> Vs it should be either of <"
              + okStatusLogEntry
              + ">, or <"
              + rateLimitedLogEntry
              + ">");
      if (rateLimitedLogEntry.equals(entry)) {
        rateLimitedRequests++;
      }
      totalRequests++;
    }
    if (isCustomLoggingDisabled) {
      // Custom logging is disabled, Jetty's CustomRequestLog.java would be used, which is
      // configured to log to stdout, so expect 0 entries in the queue logEntries.
      assertEquals(0, totalRequests);
      return;
    }
    // This test will run on CI machines(like on Jenkins), which are shared & busy. Even
    // though all requests are expected to be done with-in 1 second, on such machines, they can
    // happen across 1 second. So less # of requests are rate-limited, keep 5% margin for that.
    int minExpectedRateLimitLogs = (int) (0.95 * expectedRateLimitLogs);
    assertTrue(
        rateLimitedRequests >= minExpectedRateLimitLogs,
        "Expected # of rate-limited requests to be >= " + minExpectedRateLimitLogs);
    assertEquals(expectedNumOfEntries, totalRequests);
  }
}
