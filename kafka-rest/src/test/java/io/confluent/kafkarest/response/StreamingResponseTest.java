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
import java.util.concurrent.CompletableFuture;
import org.easymock.EasyMock;
import org.glassfish.jersey.server.ChunkedOutput;
import org.junit.jupiter.api.Test;

public class StreamingResponseTest {

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
            .build();

    ResultOrError resultOrError = ResultOrError.result(produceResponse);

    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    mockedChunkedOutput.write(resultOrError);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.close();
    replay(mockedChunkedOutputFactory);
    replay(mockedChunkedOutput);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(mockedChunkedOutputFactory);
    StreamingResponse<ProduceRequest> streamingResponse = streamingResponseFactory.from(requests);

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
            .build();
    ResultOrError resultOrError = ResultOrError.result(produceResponse);

    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    mockedChunkedOutput.write(resultOrError);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.close();
    replay(mockedChunkedOutput, mockedChunkedOutputFactory);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(mockedChunkedOutputFactory);

    StreamingResponse<ProduceRequest> streamingResponse =
        streamingResponseFactory.from(requestsMappingIterator);

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
                "Bad Request: Error processing JSON: Error thrown by mapping iterator describing problem."));

    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    mockedChunkedOutput.write(resultOrError);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.close();
    replay(mockedChunkedOutputFactory);
    replay(mockedChunkedOutput);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(mockedChunkedOutputFactory);
    StreamingResponse<ProduceRequest> streamingResponse = streamingResponseFactory.from(requests);

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
        .andThrow(new RuntimeException("IO error thrown by mapping iterator describing problem."));
    requests.close();
    replay(requests);

    ChunkedOutputFactory mockedChunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);

    ResultOrError resultOrError =
        ResultOrError.error(
            ErrorResponse.create(
                400,
                "Bad Request: Error processing message: IO error thrown by mapping iterator describing problem."));

    expect(mockedChunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput);
    mockedChunkedOutput.write(resultOrError);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.close();
    replay(mockedChunkedOutputFactory);
    replay(mockedChunkedOutput);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(mockedChunkedOutputFactory);
    StreamingResponse<ProduceRequest> streamingResponse = streamingResponseFactory.from(requests);

    FakeAsyncResponse response = new FakeAsyncResponse();

    streamingResponse.compose(result -> new CompletableFuture<>()).resume(response);

    EasyMock.verify(mockedChunkedOutput);
    EasyMock.verify(mockedChunkedOutputFactory);
    EasyMock.verify(requests);
  }
}
