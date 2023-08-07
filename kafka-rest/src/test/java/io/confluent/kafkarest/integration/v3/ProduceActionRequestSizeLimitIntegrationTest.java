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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.testing.DefaultKafkaRestTestEnvironment;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.OutputStreamContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("IntegrationTest")
public class ProduceActionRequestSizeLimitIntegrationTest {
  private static final Logger log =
      LoggerFactory.getLogger(ProduceActionRequestSizeLimitIntegrationTest.class);
  private static final Random RNG = new Random();
  private static final String TEST_MESSAGE_TEMPLATE =
      "{ " + "\"value\" : { " + "\"type\" : \"JSON\", " + "\"data\" : \"%s\" " + "}}";
  private static final String TEST_TOPIC_NAME = "test-topic-1";
  private static final ObjectMapper TEST_RESPONSE_OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final int TEST_DATA_SIZE = 1024 * 1024;
  private static final int LIMIT_SIZE = 1024 * 1024 + 8 * 1024;

  private HttpClient httpClient;

  @RegisterExtension
  public final DefaultKafkaRestTestEnvironment testEnv = new DefaultKafkaRestTestEnvironment(false);

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    Properties properties = new Properties();
    // Adding custom KafkaRestConfigs for individual test-cases/test-methods below.
    if (!testInfo.getDisplayName().contains("ProduceRequestSizeNoLimit")) {
      properties.put(
          KafkaRestConfig.PRODUCE_REQUEST_SIZE_LIMIT_MAX_BYTES_CONFIG, String.valueOf(LIMIT_SIZE));
    }
    testEnv.kafkaRest().startApp(properties);
    testEnv.kafkaCluster().createTopic(TEST_TOPIC_NAME, 1, (short) 1);

    SslContextFactory.Client sslContextFactory = new SslContextFactory.Client();
    sslContextFactory.setSslContext(testEnv.certificates().getSslContext("kafka-rest"));
    httpClient = new HttpClient(sslContextFactory);
    httpClient.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    httpClient.stop();
    testEnv.kafkaRest().closeApp();
  }

  @Test
  @DisplayName("testStreaming_ProduceRequestSizeNoLimit")
  public void testStreaming_ProduceRequestSizeNoLimit() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    URI uri =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TEST_TOPIC_NAME + "/records")
            .getUri();

    final OutputStreamContentProvider contentProvider = new OutputStreamContentProvider();
    InputStreamResponseListener responseListener = new InputStreamResponseListener();

    httpClient
        .POST(uri)
        .header(HttpHeader.TRANSFER_ENCODING, "chunked")
        .content(contentProvider, MediaType.APPLICATION_JSON)
        .send(responseListener); // async request

    httpClient
        .getExecutor()
        .execute(
            () -> {
              // send data in a separate thread
              try (OutputStream outputStream = contentProvider.getOutputStream()) {
                for (int i = 0; i < 5; i++) {
                  outputStream.write(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8));
                }
              } catch (IOException e) {
                log.error("Error writing to output stream", e);
              }
            });

    List<TestProduceResponse> produceResponses = new ArrayList<>();
    // waiting for response
    Response response = responseListener.get(1, TimeUnit.MINUTES);
    if (response.getStatus() == HttpStatus.OK_200) {
      // Obtain the input stream on the response content
      try (InputStream input = responseListener.getInputStream()) {
        // Read the response content as stream
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
          produceResponses.add(
              TEST_RESPONSE_OBJECT_MAPPER.readValue(line, TestProduceResponse.class));
        }
      }
    }

    // 5 requests, therefore, we have 5 responses
    assertEquals(5, produceResponses.size());
    for (int i = 0; i < produceResponses.size(); i++) {
      // all successful
      assertEquals(HttpStatus.OK_200, produceResponses.get(i).errorCode);
      assertEquals(i, produceResponses.get(i).offset);
    }
  }

  @Test
  @DisplayName("testStreaming_ProduceRequestSizeWithLimit_withinLimit")
  public void testStreaming_ProduceRequestSizeWithLimit_withinLimit() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    URI uri =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TEST_TOPIC_NAME + "/records")
            .getUri();

    final OutputStreamContentProvider contentProvider = new OutputStreamContentProvider();
    InputStreamResponseListener responseListener = new InputStreamResponseListener();

    httpClient
        .POST(uri)
        .header(HttpHeader.TRANSFER_ENCODING, "chunked")
        .content(contentProvider, MediaType.APPLICATION_JSON)
        .send(responseListener); // async request

    httpClient
        .getExecutor()
        .execute(
            () -> {
              // send data in a separate thread
              try (OutputStream outputStream = contentProvider.getOutputStream()) {
                // all the messages are within limit
                for (int i = 0; i < 10; i++) {
                  outputStream.write(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8));
                }
              } catch (IOException e) {
                log.error("Error writing to output stream", e);
              }
            });

    List<TestProduceResponse> produceResponses = new ArrayList<>();
    // waiting for response
    Response response = responseListener.get(1, TimeUnit.MINUTES);
    if (response.getStatus() == HttpStatus.OK_200) {
      // Obtain the input stream on the response content
      try (InputStream input = responseListener.getInputStream()) {
        // Read the response content as stream
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
          produceResponses.add(
              TEST_RESPONSE_OBJECT_MAPPER.readValue(line, TestProduceResponse.class));
        }
      }
    }

    // 10 requests so we should get 10 responses
    assertEquals(10, produceResponses.size());
    for (int i = 0; i < produceResponses.size(); i++) {
      // all successful
      assertEquals(HttpStatus.OK_200, produceResponses.get(i).errorCode);
      assertEquals(i, produceResponses.get(i).offset);
    }
  }

  @Test
  @DisplayName("testStreaming_ProduceRequestSizeWithLimit_violateLimitFirstMessage")
  public void testStreaming_ProduceRequestSizeWithLimit_violateLimitFirstMessage()
      throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    URI uri =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TEST_TOPIC_NAME + "/records")
            .getUri();

    final OutputStreamContentProvider contentProvider = new OutputStreamContentProvider();
    InputStreamResponseListener responseListener = new InputStreamResponseListener();

    httpClient
        .POST(uri)
        .header(HttpHeader.TRANSFER_ENCODING, "chunked")
        .content(contentProvider, MediaType.APPLICATION_JSON)
        .send(responseListener); // async request

    httpClient
        .getExecutor()
        .execute(
            () -> {
              // send data in a separate thread
              try (OutputStream outputStream = contentProvider.getOutputStream()) {
                // this is over limit size
                outputStream.write(
                    generateData(TEST_DATA_SIZE + 16 * 1024).getBytes(StandardCharsets.UTF_8));
                // those requests are not sent
                for (int i = 0; i < 10; i++) {
                  outputStream.write(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8));
                }
              } catch (IOException e) {
                log.error("Error writing to output stream", e);
              }
            });

    List<TestProduceResponse> produceResponses = new ArrayList<>();
    // waiting for response
    Response response = responseListener.get(1, TimeUnit.MINUTES);
    if (response.getStatus() == HttpStatus.OK_200) {
      // Obtain the input stream on the response content
      try (InputStream input = responseListener.getInputStream()) {
        // Read the response content as stream
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
          produceResponses.add(
              TEST_RESPONSE_OBJECT_MAPPER.readValue(line, TestProduceResponse.class));
        }
      }
    }

    // as the first produce request is over the limit, we should have only one response
    assertEquals(1, produceResponses.size());
    TestProduceResponse produceResponse = produceResponses.get(0);
    assertEquals(HttpStatus.BAD_REQUEST_400, produceResponse.errorCode);
    assertThat(
        produceResponse.message,
        containsString("Produce request size is larger than allowed threshold"));
  }

  @Test
  @DisplayName("testStreaming_ProduceRequestSizeWithLimit_violateLimitSecondMessage")
  public void testStreaming_ProduceRequestSizeWithLimit_violateLimitSecondMessage()
      throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    URI uri =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TEST_TOPIC_NAME + "/records")
            .getUri();

    final OutputStreamContentProvider contentProvider = new OutputStreamContentProvider();
    InputStreamResponseListener responseListener = new InputStreamResponseListener();

    httpClient
        .POST(uri)
        .header(HttpHeader.TRANSFER_ENCODING, "chunked")
        .content(contentProvider, MediaType.APPLICATION_JSON)
        .send(responseListener); // async request

    httpClient
        .getExecutor()
        .execute(
            () -> {
              // send data in a separate thread
              try (OutputStream outputStream = contentProvider.getOutputStream()) {
                outputStream.write(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8));
                // this is over limit size
                outputStream.write(
                    generateData(TEST_DATA_SIZE + 16 * 1024).getBytes(StandardCharsets.UTF_8));
                // those requests are not sent
                for (int i = 0; i < 10; i++) {
                  outputStream.write(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8));
                }
              } catch (IOException e) {
                log.error("Error writing to output stream", e);
              }
            });

    List<TestProduceResponse> produceResponses = new ArrayList<>();
    // waiting for response
    Response response = responseListener.get(1, TimeUnit.MINUTES);
    if (response.getStatus() == HttpStatus.OK_200) {
      // Obtain the input stream on the response content
      try (InputStream input = responseListener.getInputStream()) {
        // Read the response content as stream
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
          produceResponses.add(
              TEST_RESPONSE_OBJECT_MAPPER.readValue(line, TestProduceResponse.class));
        }
      }
    }

    // as the second produce request is over the limit, we should have only two responses
    assertEquals(2, produceResponses.size());
    TestProduceResponse response1 = produceResponses.get(0);
    assertEquals(HttpStatus.OK_200, response1.errorCode);
    assertEquals(0, response1.offset);

    TestProduceResponse response2 = produceResponses.get(1);
    assertEquals(HttpStatus.BAD_REQUEST_400, response2.errorCode);
    assertThat(
        response2.message, containsString("Produce request size is larger than allowed threshold"));
  }

  private static String generateData(int size) {
    String value = TestUtils.generateAlphanumericString(RNG, size);
    return String.format(TEST_MESSAGE_TEMPLATE, value);
  }

  /** A class that encapsulates either ErrorResponse or ProduceResponse */
  private static class TestProduceResponse {
    @JsonProperty("offset")
    private long offset = -1L;

    @JsonProperty("error_code")
    private int errorCode = -1;

    @JsonProperty("message")
    private String message = "";

    public void setOffset(long offset) {
      this.offset = offset;
    }

    public void setErrorCode(int errorCode) {
      this.errorCode = errorCode;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }
}
