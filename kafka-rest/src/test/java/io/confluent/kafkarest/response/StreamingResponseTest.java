/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.response;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.response.StreamingResponse.ResultOrError;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.easymock.EasyMock;
import org.eclipse.jetty.http.HttpStatus;
import org.glassfish.jersey.server.ChunkedOutput;
import org.junit.jupiter.api.Test;

public class StreamingResponseTest {

  private static final Duration DURATION = Duration.ofMillis(5000);
  private static final Duration GRACE_DURATION = Duration.ofMillis(1);
  private static final int DEPTH = 100;

  @Test
  public void testGracePeriodExceededExceptionThrown() throws IOException {
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(value))
                    .build())
            .setOriginalSize(0L)
            .build();

    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);
    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue()).andReturn(request);
    expect(requests.hasNext()).andReturn(false);
    requests.close();
    replay(requests);

    ChunkedOutputFactory mockedChunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);

    ProduceResponse produceResponse =
        ProduceResponse.builder()
            .setClusterId("clusterId")
            .setTopicName("topicName")
            .setPartitionId(1)
            .setOffset(1L)
            .setErrorCode(HttpStatus.OK_200)
            .build();

    ResultOrError resultOrError = ResultOrError.result(produceResponse);

    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    mockedChunkedOutput.write(resultOrError);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.close();
    replay(mockedChunkedOutputFactory);
    replay(mockedChunkedOutput);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(
            mockedChunkedOutputFactory, DURATION, GRACE_DURATION, DEPTH, DEPTH);
    StreamingResponse<ProduceRequest> streamingResponse =
        streamingResponseFactory.from(new JsonStream<>(() -> requests));

    CompletableFuture<ProduceResponse> produceResponseFuture = new CompletableFuture<>();

    produceResponseFuture.complete(produceResponse);

    FakeAsyncResponse response = new FakeAsyncResponse();
    streamingResponse.compose(result -> produceResponseFuture).resume(response);

    EasyMock.verify(mockedChunkedOutput);
    EasyMock.verify(mockedChunkedOutputFactory);
    EasyMock.verify(requests);
  }

  @Test
  public void testWriteToChunkedOutput() throws IOException {
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(value))
                    .build())
            .setOriginalSize(0L)
            .build();

    MappingIterator<ProduceRequest> requestsMappingIterator = mock(MappingIterator.class);
    expect(requestsMappingIterator.hasNext()).andReturn(true);
    expect(requestsMappingIterator.nextValue()).andReturn(request);
    expect(requestsMappingIterator.hasNext()).andReturn(false);
    requestsMappingIterator.close();
    replay(requestsMappingIterator);

    ChunkedOutputFactory mockedChunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);

    ProduceResponse produceResponse =
        ProduceResponse.builder()
            .setClusterId("clusterId")
            .setTopicName("topicName")
            .setPartitionId(1)
            .setOffset(1L)
            .setErrorCode(HttpStatus.OK_200)
            .build();
    ResultOrError resultOrError = ResultOrError.result(produceResponse);

    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    mockedChunkedOutput.write(resultOrError);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.close();
    replay(mockedChunkedOutput, mockedChunkedOutputFactory);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(
            mockedChunkedOutputFactory, DURATION, GRACE_DURATION, DEPTH, DEPTH);

    StreamingResponse<ProduceRequest> streamingResponse =
        streamingResponseFactory.from(new JsonStream<>(() -> requestsMappingIterator));

    CompletableFuture<ProduceResponse> produceResponseFuture = new CompletableFuture<>();
    produceResponseFuture.complete(produceResponse);

    FakeAsyncResponse response = new FakeAsyncResponse();
    streamingResponse.compose(result -> produceResponseFuture).resume(response);

    EasyMock.verify(mockedChunkedOutput);
    EasyMock.verify(mockedChunkedOutputFactory);
    EasyMock.verify(requestsMappingIterator);
  }

  @Test
  public void testHasNextMappingException() throws IOException {

    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);
    expect(requests.hasNext())
        .andThrow(
            new RuntimeJsonMappingException(
                "Error thrown by mapping iterator describing problem."));
    requests.close();
    replay(requests);

    ChunkedOutputFactory mockedChunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);

    ResultOrError resultOrError =
        ResultOrError.error(
            ErrorResponse.create(
                400,
                "Bad Request: Error processing JSON: "
                    + "Error thrown by mapping iterator describing problem."));

    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    mockedChunkedOutput.write(resultOrError);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.close();
    replay(mockedChunkedOutputFactory);
    replay(mockedChunkedOutput);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(
            mockedChunkedOutputFactory, DURATION, GRACE_DURATION, DEPTH, DEPTH);
    StreamingResponse<ProduceRequest> streamingResponse =
        streamingResponseFactory.from(new JsonStream<>(() -> requests));

    FakeAsyncResponse response = new FakeAsyncResponse();

    streamingResponse.compose(result -> new CompletableFuture<>()).resume(response);

    EasyMock.verify(mockedChunkedOutput);
    EasyMock.verify(mockedChunkedOutputFactory);
    EasyMock.verify(requests);
  }

  @Test
  public void testHasNextRuntimeException() throws IOException {
    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);
    expect(requests.hasNext())
        .andThrow(
            new RuntimeException("IO error thrown by mapping iterator describing" + " problem."));
    requests.close();
    replay(requests);

    ChunkedOutputFactory mockedChunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);

    ResultOrError resultOrError =
        ResultOrError.error(
            ErrorResponse.create(
                400,
                "Bad Request: Error processing message: "
                    + "IO error thrown by mapping iterator describing problem."));

    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    mockedChunkedOutput.write(resultOrError);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.close();
    replay(mockedChunkedOutputFactory);
    replay(mockedChunkedOutput);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(
            mockedChunkedOutputFactory, DURATION, GRACE_DURATION, DEPTH, DEPTH);
    StreamingResponse<ProduceRequest> streamingResponse =
        streamingResponseFactory.from(new JsonStream<>(() -> requests));

    FakeAsyncResponse response = new FakeAsyncResponse();

    streamingResponse.compose(result -> new CompletableFuture<>()).resume(response);

    EasyMock.verify(mockedChunkedOutput);
    EasyMock.verify(mockedChunkedOutputFactory);
    EasyMock.verify(requests);
  }

  @Test
  public void testWriteToChunkedOutputAfterTimeout() throws IOException {
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(value))
                    .build())
            .setOriginalSize(0L)
            .build();

    MappingIterator<ProduceRequest> requestsMappingIterator = mock(MappingIterator.class);

    long timeout = 10;
    Clock clock = mock(Clock.class);

    ProduceResponse produceResponse =
        ProduceResponse.builder()
            .setClusterId("clusterId")
            .setTopicName("topicName")
            .setPartitionId(1)
            .setOffset(1L)
            .setErrorCode(HttpStatus.OK_200)
            .build();
    ResultOrError sucessResult = ResultOrError.result(produceResponse);

    ResultOrError error =
        ResultOrError.error(
            ErrorResponse.create(
                408,
                "Streaming connection open for longer than allowed: "
                    + "Connection will be closed."));

    ChunkedOutputFactory mockedChunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);

    expect(clock.instant())
        .andReturn(Instant.ofEpochMilli(0)); // stream start - input stream response

    // first message
    expect(clock.instant()).andReturn(Instant.ofEpochMilli(0));
    expect(requestsMappingIterator.hasNext()).andReturn(true);
    expect(requestsMappingIterator.nextValue()).andReturn(request);
    expect(clock.instant())
        .andReturn(Instant.ofEpochMilli(1)); // first comparison duration.  within timeout
    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(sucessResult);

    // second message
    expect(requestsMappingIterator.hasNext()).andReturn(true);
    expect(requestsMappingIterator.nextValue()).andReturn(request);
    expect(clock.instant())
        .andReturn(Instant.ofEpochMilli(timeout + 5)); // second message beyond timeout
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(error);

    // no third message
    expect(requestsMappingIterator.hasNext()).andReturn(false);

    requestsMappingIterator.close(); // call from thread executor
    mockedChunkedOutput.close(); // call from thread executor

    requestsMappingIterator.close(); // call from finally
    mockedChunkedOutput.close(); // call from finally

    replay(mockedChunkedOutput, mockedChunkedOutputFactory, requestsMappingIterator, clock);

    final StreamingResponse streamingResponse =
        StreamingResponse.fromWithClock(
            new JsonStream<>(() -> requestsMappingIterator),
            mockedChunkedOutputFactory,
            Duration.ofMillis(timeout),
            Duration.ofMillis(50),
            DEPTH,
            DEPTH,
            clock);

    CompletableFuture<ProduceResponse> produceResponseFuture = new CompletableFuture<>();
    produceResponseFuture.complete(produceResponse);

    FakeAsyncResponse response = new FakeAsyncResponse();
    streamingResponse.compose(result -> produceResponseFuture).resume(response);

    try {
      EasyMock.verify(mockedChunkedOutput);
      EasyMock.verify(mockedChunkedOutputFactory);
      EasyMock.verify(requestsMappingIterator);
      EasyMock.verify(clock);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSlowWritesToKafka429() throws IOException {

    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(value))
                    .build())
            .setOriginalSize(0L)
            .build();

    MappingIterator<ProduceRequest> requestsMappingIterator = mock(MappingIterator.class);

    long timeout = 10;
    Clock clock = mock(Clock.class);

    CompletableFuture<ProduceResponse> produceResponseFuture = new CompletableFuture<>();

    ProduceResponse produceResponse =
        ProduceResponse.builder()
            .setClusterId("clusterId")
            .setTopicName("topicName")
            .setPartitionId(1)
            .setOffset(1L)
            .setErrorCode(HttpStatus.OK_200)
            .build();
    ResultOrError sucessResult = ResultOrError.result(produceResponse);

    ResultOrError error =
        ResultOrError.error(
            ErrorResponse.create(
                429,
                "Backlog of messages waiting to be sent to Kafka is too large: Not "
                    + "sending to Kafka."));

    ChunkedOutputFactory mockedChunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);

    expect(clock.instant()).andReturn(Instant.ofEpochMilli(0));

    // first message
    expect(requestsMappingIterator.hasNext()).andReturn(true);
    expect(clock.instant()).andReturn(Instant.ofEpochMilli(0));
    expect(requestsMappingIterator.nextValue()).andReturn(request);
    expect(clock.instant()).andReturn(Instant.ofEpochMilli(1));
    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(sucessResult);

    // second message
    expect(requestsMappingIterator.hasNext()).andReturn(true);
    expect(clock.instant()).andReturn(Instant.ofEpochMilli(2));
    expect(requestsMappingIterator.nextValue())
        .andAnswer(
            () -> {
              produceResponseFuture.complete(produceResponse);
              return request;
            });
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(error);

    expect(requestsMappingIterator.hasNext()).andReturn(false);

    requestsMappingIterator.close(); // closes from the finally
    mockedChunkedOutput.close();

    replay(mockedChunkedOutput, mockedChunkedOutputFactory, requestsMappingIterator, clock);

    StreamingResponse<ProduceRequest> streamingResponse =
        StreamingResponse.fromWithClock(
            new JsonStream<>(() -> requestsMappingIterator),
            mockedChunkedOutputFactory,
            Duration.ofMillis(timeout),
            Duration.ofMillis(50),
            1,
            2,
            clock);

    FakeAsyncResponse response = new FakeAsyncResponse();
    streamingResponse.compose(result -> produceResponseFuture).resume(response);

    EasyMock.verify(mockedChunkedOutput);
    EasyMock.verify(mockedChunkedOutputFactory);
    EasyMock.verify(requestsMappingIterator);
    EasyMock.verify(clock);
  }

  @Test
  public void testSlowWritesToKafkaDisconnects() throws IOException {

    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(value))
                    .build())
            .setOriginalSize(0L)
            .build();

    MappingIterator<ProduceRequest> requestsMappingIterator = mock(MappingIterator.class);

    long timeout = 10;
    Clock clock = mock(Clock.class);

    CompletableFuture<ProduceResponse> produceResponseFuture = new CompletableFuture<>();

    ProduceResponse produceResponse =
        ProduceResponse.builder()
            .setClusterId("clusterId")
            .setTopicName("topicName")
            .setPartitionId(1)
            .setOffset(1L)
            .setErrorCode(HttpStatus.OK_200)
            .build();
    ResultOrError sucessResult = ResultOrError.result(produceResponse);

    ResultOrError error =
        ResultOrError.error(
            ErrorResponse.create(
                429,
                "Backlog of messages waiting to be sent to Kafka is too large: Not "
                    + "sending to Kafka."));

    ChunkedOutputFactory mockedChunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);

    expect(clock.instant()).andReturn(Instant.ofEpochMilli(0));

    // first message
    expect(requestsMappingIterator.hasNext()).andReturn(true);
    expect(clock.instant()).andReturn(Instant.ofEpochMilli(0));
    expect(requestsMappingIterator.nextValue()).andReturn(request);
    expect(clock.instant()).andReturn(Instant.ofEpochMilli(1));
    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(sucessResult);

    // second message
    expect(requestsMappingIterator.hasNext()).andReturn(true);
    expect(clock.instant()).andReturn(Instant.ofEpochMilli(2));
    expect(requestsMappingIterator.nextValue())
        .andAnswer(
            () -> {
              return request;
            });
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(error);

    // third message
    expect(requestsMappingIterator.hasNext()).andReturn(true);
    expect(clock.instant()).andReturn(Instant.ofEpochMilli(2));
    expect(requestsMappingIterator.nextValue())
        .andAnswer(
            () -> {
              produceResponseFuture.complete(produceResponse);
              return request;
            });
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(error);

    expect(requestsMappingIterator.hasNext()).andReturn(false);

    requestsMappingIterator.close(); // closes from thread executor
    mockedChunkedOutput.close();
    requestsMappingIterator.close(); // closes from finally
    mockedChunkedOutput.close();

    replay(mockedChunkedOutput, mockedChunkedOutputFactory, requestsMappingIterator, clock);

    StreamingResponse<ProduceRequest> streamingResponse =
        StreamingResponse.fromWithClock(
            new JsonStream<>(() -> requestsMappingIterator),
            mockedChunkedOutputFactory,
            Duration.ofMillis(timeout),
            Duration.ofMillis(50),
            1,
            2,
            clock);

    FakeAsyncResponse response = new FakeAsyncResponse();
    streamingResponse.compose(result -> produceResponseFuture).resume(response);

    EasyMock.verify(mockedChunkedOutput);
    EasyMock.verify(mockedChunkedOutputFactory);
    EasyMock.verify(requestsMappingIterator);
    EasyMock.verify(clock);
  }
}
