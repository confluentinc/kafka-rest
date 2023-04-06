package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED;
import static io.confluent.kafkarest.KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.util.concurrent.MoreExecutors;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.ratelimit.RateLimitExceededException;
import io.confluent.kafkarest.ratelimit.RequestRateLimiter;
import io.confluent.kafkarest.response.ChunkedOutputFactory;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.StreamingResponse.ResultOrError;
import io.confluent.kafkarest.response.StreamingResponseFactory;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.inject.Provider;
import org.easymock.EasyMock;
import org.glassfish.jersey.server.ChunkedOutput;
import org.junit.jupiter.api.Test;

public class ProduceActionTest {

  @Test
  public void produceNoSchemaRegistryDefined() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS = 1;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "100");
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(999999999));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, "3600000");
    properties.put(SCHEMA_REGISTRY_URL_CONFIG, "");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS);

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider);

    ProduceAction produceAction =
        getProduceAction(
            properties,
            chunkedOutputFactory,
            1,
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            true);
    MappingIterator<ProduceRequest> requests = getProduceRequestsMappingIteratorWithSchemaNeeded();

    // expected results

    ErrorResponse err =
        ErrorResponse.create(
            422,
            "Error: 42206 : Payload error. Schema Registry must be configured when using schemas.");
    ResultOrError resultOrErrorFail = ResultOrError.error(err);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorFail);
    mockedChunkedOutput.close();

    replay(mockedChunkedOutput, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse, "clusterId", "topicName", requests);

    // check results
    EasyMock.verify(requests);
    EasyMock.verify(mockedChunkedOutput);
  }

  @Test
  public void streamingRequests() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1 = 1;
    final int TOTAL_NUMBER_OF_STREAMING_CALLS = 4;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(10000));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(999999999));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1);

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider);

    ProduceAction produceAction1 =
        getProduceAction(
            properties,
            chunkedOutputFactory,
            TOTAL_NUMBER_OF_STREAMING_CALLS,
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider);
    MappingIterator<ProduceRequest> requests = getStreamingProduceRequestsMappingIterator(4);

    // expected results
    ProduceResponse produceResponse1 = getProduceResponse(0);
    ResultOrError resultOrErrorOKProd1 = ResultOrError.result(produceResponse1);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd1);

    ProduceResponse produceResponse2 = getProduceResponse(1);
    ResultOrError resultOrErrorOKProd2 = ResultOrError.result(produceResponse2);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd2);

    ProduceResponse produceResponse3 = getProduceResponse(2);
    ResultOrError resultOrErrorOKProd3 = ResultOrError.result(produceResponse3);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd3);

    ProduceResponse produceResponse4 = getProduceResponse(3);
    ResultOrError resultOrErrorOKProd4 = ResultOrError.result(produceResponse4);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd4);
    mockedChunkedOutput.close();

    replay(mockedChunkedOutput, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse1 = new FakeAsyncResponse();
    produceAction1.produce(fakeAsyncResponse1, "clusterId", "topicName", requests);

    // check results
    EasyMock.verify(requests);
    EasyMock.verify(mockedChunkedOutput);
  }

  @Test
  public void produceWithByteLimit() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS = 2;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "100");
    properties.put(
        PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(30)); // first record is 25 bytes long
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, "3600000");
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS);

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());

    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    EasyMock.expectLastCall().andThrow(new RateLimitExceededException());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider);

    ProduceAction produceAction =
        getProduceAction(
            properties,
            chunkedOutputFactory,
            1,
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider);
    MappingIterator<ProduceRequest> requests =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS);

    // expected results
    ProduceResponse produceResponse = getProduceResponse(0);
    ResultOrError resultOrErrorOK = ResultOrError.result(produceResponse);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOK); // successful first produce
    mockedChunkedOutput.close();

    ErrorResponse err =
        ErrorResponse.create(
            429,
            "Request rate limit exceeded: The rate limit of requests per second has been exceeded.");
    ResultOrError resultOrErrorFail = ResultOrError.error(err);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorFail); // failing second produce
    mockedChunkedOutput.close(); // error close

    replay(mockedChunkedOutput, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse2, "clusterId", "topicName", requests);

    // check results
    // check results
    verify(
        requests,
        mockedChunkedOutput,
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal);
  }

  @Test
  public void produceWithCountLimit() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS = 2;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "100");
    properties.put(
        PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(30)); // first record is 25 bytes long
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, "3600000");
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS);

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());

    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());
    rateLimiterForCount.rateLimit(anyInt());
    EasyMock.expectLastCall().andThrow(new RateLimitExceededException());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider);

    ProduceAction produceAction =
        getProduceAction(
            properties,
            chunkedOutputFactory,
            1,
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider);
    MappingIterator<ProduceRequest> requests =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS);

    // expected results
    ProduceResponse produceResponse = getProduceResponse(0);
    ResultOrError resultOrErrorOK = ResultOrError.result(produceResponse);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOK); // successful first produce
    mockedChunkedOutput.close();

    ErrorResponse err =
        ErrorResponse.create(
            429,
            "Request rate limit exceeded: The rate limit of requests per second has been exceeded.");
    ResultOrError resultOrErrorFail = ResultOrError.error(err);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorFail); // failing second produce
    mockedChunkedOutput.close(); // error close

    replay(mockedChunkedOutput, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse2, "clusterId", "topicName", requests);

    // check results
    verify(
        requests,
        mockedChunkedOutput,
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal);
  }

  @Test
  public void produceNoLimit() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS = 2;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "100");
    properties.put(
        PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(30)); // first record is 25 bytes long
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, "3600000");
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "falsse");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS);

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider);

    ProduceAction produceAction =
        getProduceAction(
            properties,
            chunkedOutputFactory,
            2,
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider);
    MappingIterator<ProduceRequest> requests =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS);

    // expected results
    ProduceResponse produceResponse = getProduceResponse(0);
    ResultOrError resultOrErrorOK1 = ResultOrError.result(produceResponse);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOK1); // successful first produce
    mockedChunkedOutput.close();

    ProduceResponse produceResponse2 = getProduceResponse(1);
    ResultOrError resultOrErrorOK2 = ResultOrError.result(produceResponse2);

    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOK2); // successful second produce
    mockedChunkedOutput.close();

    replay(mockedChunkedOutput, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse2, "clusterId", "topicName", requests);

    // check results
    verify(
        requests,
        mockedChunkedOutput,
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal);
  }

  @Test
  public void testHasNextOnNullData() throws Exception {
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "100");
    properties.put(
        PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(30)); // first record is 25 bytes long
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, "3600000");
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());

    ProduceAction produceAction =
        getProduceAction(
            properties,
            chunkedOutputFactory,
            1,
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider);
    MappingIterator<ProduceRequest> requests = null;

    FakeAsyncResponse fakeAsyncResponse = new FakeAsyncResponse();

    RestConstraintViolationException e =
        assertThrows(
            RestConstraintViolationException.class,
            () -> produceAction.produce(fakeAsyncResponse, "clusterId", "topicName", requests));
    assertEquals("Payload error. Null input provided. Data is required.", e.getMessage());
    assertEquals(42206, e.getErrorCode());
  }

  private static Provider<RecordSerializer> getRecordSerializerProvider(boolean error) {
    Provider<RecordSerializer> recordSerializerProvider = mock(Provider.class);
    RecordSerializer recordSerializer = mock(RecordSerializer.class);
    expect(recordSerializerProvider.get()).andReturn(recordSerializer).anyTimes();

    if (!error) {
      expect(
              recordSerializer.serialize(
                  anyObject(), anyObject(), anyObject(), anyObject(), anyBoolean()))
          .andReturn(Optional.empty())
          .anyTimes();
    } else {
      expect(
              recordSerializer.serialize(
                  anyObject(), anyObject(), anyObject(), anyObject(), anyBoolean()))
          .andThrow(
              Errors.messageSerializationException(
                  "Schema Registry not defined, no Schema Registry client available to deserialize message."))
          .anyTimes();
    }
    replay(recordSerializerProvider, recordSerializer);

    EasyMock.verify(recordSerializer);
    return recordSerializerProvider;
  }

  private static ProduceController getProduceControllerMock(Provider produceControllerProvider) {
    ProduceController produceController = mock(ProduceController.class);
    expect(produceControllerProvider.get()).andReturn(produceController).anyTimes();
    return produceController;
  }

  private static MappingIterator<ProduceRequest> getProduceRequestsMappingIterator(int times)
      throws IOException {
    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);
    ProduceRequest request = ProduceRequest.builder().setOriginalSize(25L).build();
    for (int i = 0; i < times; i++) {
      expect(requests.hasNext()).andReturn(true).times(1);
      expect(requests.nextValue()).andReturn(request).times(1);
      expect(requests.hasNext()).andReturn(false).times(1);
      requests.close();
    }
    replay(requests);
    return requests;
  }

  private static MappingIterator<ProduceRequest> getProduceRequestsMappingIteratorWithSchemaNeeded()
      throws IOException {
    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);
    ProduceRequestData key =
        ProduceRequestData.builder()
            .setFormat(EmbeddedFormat.AVRO)
            .setData(TextNode.valueOf("bob"))
            .setRawSchema("bob")
            .build();

    ProduceRequest request = ProduceRequest.builder().setKey(key).setOriginalSize(25L).build();

    expect(requests.hasNext()).andReturn(true).times(1);
    expect(requests.nextValue()).andReturn(request).times(1);
    expect(requests.hasNext()).andReturn(false).times(1);
    requests.close();

    replay(requests);
    return requests;
  }

  private static MappingIterator<ProduceRequest> getStreamingProduceRequestsMappingIterator(
      int times) throws IOException {
    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);

    for (int i = 0; i < times; i++) {
      ProduceRequest request = ProduceRequest.builder().setOriginalSize(25L).build();
      expect(requests.hasNext()).andReturn(true).times(1);
      expect(requests.nextValue()).andReturn(request).times(1);
    }
    expect(requests.hasNext()).andReturn(false).times(1);
    requests.close();
    replay(requests);
    return requests;
  }

  private static MappingIterator<ProduceRequest>
      getStreamingProduceRequestsMappingIteratorCombinations() throws IOException {
    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);

    ProduceRequest request = ProduceRequest.builder().setOriginalSize(25L).build();
    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue()).andReturn(request);

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue()).andReturn(request);

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue()).andReturn(request);

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue()).andReturn(request);

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue()).andReturn(request);

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue()).andReturn(request);

    expect(requests.hasNext()).andReturn(false).times(1);
    requests.close();
    replay(requests);

    return requests;
  }

  private static ChunkedOutput<ResultOrError> getChunkedOutput(
      ChunkedOutputFactory chunkedOutputFactory, int times) {
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);
    expect(chunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput).times(times);

    return mockedChunkedOutput;
  }

  private static ProduceResponse getProduceResponse(int offset) {
    return getProduceResponse(offset, Optional.empty());
  }

  private static ProduceResponse getProduceResponse(int offset, int partitionId) {
    return getProduceResponse(offset, Optional.empty(), partitionId);
  }

  private static ProduceResponse getProduceResponse(int offset, Optional<Duration> waitFor) {
    return getProduceResponse(offset, waitFor, 0);
  }

  private static ProduceResponse getProduceResponse(
      int offset, Optional<Duration> waitFor, int partitionId) {
    return ProduceResponse.builder()
        .setClusterId("clusterId")
        .setTopicName("topicName")
        .setPartitionId(partitionId)
        .setOffset(offset)
        .setTimestamp(Instant.ofEpochMilli(0))
        .build();
  }

  private static CompletableFuture<ProduceResult> getProduceResultMock(int offset, int producerId) {
    ProduceResult produceResult = mock(ProduceResult.class);
    setExpectsForProduceResult(produceResult, offset, producerId);

    return CompletableFuture.completedFuture(produceResult);
  }

  private static void setExpectsForProduceResult(
      ProduceResult produceResult, long offset, int producerId) {
    expect(produceResult.getPartitionId()).andReturn(producerId).anyTimes();
    expect(produceResult.getOffset()).andReturn(offset).anyTimes();

    expect(produceResult.getTimestamp()).andReturn(Optional.of(Instant.ofEpochMilli(0))).anyTimes();
    expect(produceResult.getSerializedKeySize()).andReturn(1).anyTimes();
    expect(produceResult.getSerializedValueSize()).andReturn(1).anyTimes();
    expect(produceResult.getCompletionTimestamp()).andReturn(Instant.now()).anyTimes();
    replay(produceResult);
  }

  private static void setupExpectsMockCallsForProduce(
      ProduceController produceController, int times, int producerId) {
    for (int i = 0; i < times; i++) {
      CompletableFuture<ProduceResult> response = getProduceResultMock(i, producerId);
      expect(
              (produceController.produce(
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject())))
          .andReturn(response);
    }
  }

  private static ProduceAction getProduceAction(
      Properties properties,
      ChunkedOutputFactory chunkedOutputFactory,
      int times,
      Provider<RequestRateLimiter> countLimiter,
      Provider<RequestRateLimiter> bytesLimiter,
      Provider<RequestRateLimiter> countLimiterGlobal,
      Provider<RequestRateLimiter> bytesLimiterGlobal) {
    return getProduceAction(
        properties,
        chunkedOutputFactory,
        times,
        countLimiter,
        bytesLimiter,
        countLimiterGlobal,
        bytesLimiterGlobal,
        false);
  }

  private static ProduceAction getProduceAction(
      Properties properties,
      ChunkedOutputFactory chunkedOutputFactory,
      int times,
      Provider<RequestRateLimiter> countLimiter,
      Provider<RequestRateLimiter> bytesLimiter,
      Provider<RequestRateLimiter> countLimiterGlobal,
      Provider<RequestRateLimiter> bytesLimiterGlobal,
      boolean errorSchemaRegistry) {
    return getProduceAction(
        new ProduceRateLimiters(
            countLimiter,
            bytesLimiter,
            countLimiterGlobal,
            bytesLimiterGlobal,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS)))),
        chunkedOutputFactory,
        times,
        0,
        errorSchemaRegistry);
  }

  private static ProduceAction getProduceAction(
      ProduceRateLimiters produceRateLimiters,
      ChunkedOutputFactory chunkedOutputFactory,
      int times,
      int producerId,
      boolean errorSchemaRegistry) {
    Provider<SchemaManager> schemaManagerProvider = mock(Provider.class);
    SchemaManager schemaManagerMock = mock(SchemaManager.class);
    expect(schemaManagerProvider.get()).andReturn(schemaManagerMock);
    expect(
            schemaManagerMock.getSchema(
                "topicName",
                Optional.of(EmbeddedFormat.AVRO),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("bob"),
                true))
        .andThrow(
            Errors.invalidPayloadException(
                "Schema Registry must be configured when using schemas."));
    replay(schemaManagerProvider, schemaManagerMock);
    Provider<RecordSerializer> recordSerializerProvider =
        getRecordSerializerProvider(errorSchemaRegistry);
    Provider<ProduceController> produceControllerProvider = mock(Provider.class);
    ProduceController produceController = getProduceControllerMock(produceControllerProvider);
    setupExpectsMockCallsForProduce(produceController, times, producerId);

    replay(produceControllerProvider, produceController);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(chunkedOutputFactory);

    // get the current thread so that the call counts can be seen by easy mock
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();

    ProduceAction produceAction =
        new ProduceAction(
            schemaManagerProvider,
            recordSerializerProvider,
            produceControllerProvider,
            () -> new ProducerMetrics(new KafkaRestConfig(), Time.SYSTEM),
            streamingResponseFactory,
            produceRateLimiters,
            executorService);
    produceRateLimiters.clear();
    return produceAction;
  }
}
