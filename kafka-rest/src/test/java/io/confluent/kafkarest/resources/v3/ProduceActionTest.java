package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_GRACE_PERIOD_MS;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

import com.fasterxml.jackson.databind.MappingIterator;
import com.google.common.util.concurrent.MoreExecutors;
import io.confluent.kafkarest.ProducerMetrics;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.response.ChunkedOutputFactory;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.StreamingResponse.ResultOrError;
import io.confluent.kafkarest.response.StreamingResponseFactory;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.inject.Provider;
import org.easymock.EasyMock;
import org.glassfish.jersey.server.ChunkedOutput;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProduceActionTest {

  @Test
  public void produceWithZeroGracePeriod() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS = 2;
    Properties properties = new Properties();
    properties.put(PRODUCE_GRACE_PERIOD_MS, "0");
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "1");
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(999999999));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, "3600000");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS);
    Clock clock = mock(Clock.class);
    ProduceAction produceAction = getProduceAction(properties, chunkedOutputFactory, clock, 1);
    MappingIterator<ProduceRequest> requests =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS);

    // expected results
    ProduceResponse produceResponse = getProduceResponse(0);
    ResultOrError resultOrErrorOK = ResultOrError.result(produceResponse);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOK); // successful first produce
    mockedChunkedOutput.close();

    ErrorResponse err =
        ErrorResponse.create(429, "Rate limit exceeded : Connection will be closed.");
    ResultOrError resultOrErrorFail = ResultOrError.error(err);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorFail); // failing second produce
    mockedChunkedOutput.close(); // error close
    mockedChunkedOutput.close(); // second close always happens on tidy up

    expect(clock.millis()).andReturn(0L);
    expect(clock.millis()).andReturn(1L);

    replay(mockedChunkedOutput, chunkedOutputFactory, clock);

    // run test
    FakeAsyncResponse fakeAsyncResponse = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse2, "clusterId", "topicName", requests);

    // check results
    EasyMock.verify(requests);
    EasyMock.verify(mockedChunkedOutput);
  }

  @Test
  public void twoProducers() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD0 = 2;
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1 = 3;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(999999999));
    properties.put(PRODUCE_GRACE_PERIOD_MS, Integer.toString(10));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    // setup
    ChunkedOutputFactory chunkedOutputFactory0 = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput0 =
        getChunkedOutput(chunkedOutputFactory0, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD0);
    ChunkedOutputFactory chunkedOutputFactory1 = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput1 =
        getChunkedOutput(chunkedOutputFactory1, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1);
    Clock clock = mock(Clock.class);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            Duration.ofMillis(Integer.parseInt(properties.getProperty(PRODUCE_GRACE_PERIOD_MS))),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_REQUESTS_PER_SECOND)),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_BYTES_PER_SECOND)),
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))),
            clock);

    ProduceAction produceAction0 =
        getProduceAction(
            produceRateLimiters, chunkedOutputFactory0, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD0, 0);
    ProduceAction produceAction1 =
        getProduceAction(
            produceRateLimiters, chunkedOutputFactory1, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1, 1);
    MappingIterator<ProduceRequest> requests0 =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD0);
    MappingIterator<ProduceRequest> requests1 =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1);

    // expected results
    ProduceResponse produceResponse1 = getProduceResponse(0, 0);
    ResultOrError resultOrErrorOKProd1 = ResultOrError.result(produceResponse1);
    expect(mockedChunkedOutput0.isClosed()).andReturn(false);
    mockedChunkedOutput0.write(resultOrErrorOKProd1);
    mockedChunkedOutput0.close();

    ProduceResponse produceResponse2 =
        getProduceResponse(0, Optional.of(Duration.ofMillis(1000)), 1);
    ResultOrError resultOrErrorOKProd2 = ResultOrError.result(produceResponse2);
    expect(mockedChunkedOutput1.isClosed()).andReturn(false);
    mockedChunkedOutput1.write(resultOrErrorOKProd2);
    mockedChunkedOutput1.close();

    ProduceResponse produceResponse3 = getProduceResponse(1, 1);
    ResultOrError resultOrErrorOKProd3 = ResultOrError.result(produceResponse3);
    expect(mockedChunkedOutput1.isClosed()).andReturn(false);
    mockedChunkedOutput1.write(resultOrErrorOKProd3);
    mockedChunkedOutput1.close();

    ProduceResponse produceResponse4 =
        getProduceResponse(1, Optional.of(Duration.ofMillis(1000)), 0);
    ResultOrError resultOrErrorOKProd4 = ResultOrError.result(produceResponse4);
    expect(mockedChunkedOutput0.isClosed()).andReturn(false);
    mockedChunkedOutput0.write(resultOrErrorOKProd4);
    mockedChunkedOutput0.close();

    ErrorResponse err =
        ErrorResponse.create(429, "Rate limit exceeded : Connection will be closed.");
    ResultOrError resultOrErrorOKProd5 = ResultOrError.error(err);
    expect(mockedChunkedOutput1.isClosed()).andReturn(false);
    mockedChunkedOutput1.write(resultOrErrorOKProd5);
    mockedChunkedOutput1.close();
    mockedChunkedOutput1.close();

    expect(clock.millis()).andReturn(0L);
    expect(clock.millis()).andReturn(1L);
    expect(clock.millis()).andReturn(1011L);
    expect(clock.millis()).andReturn(1012L);
    expect(clock.millis()).andReturn(1023L);

    replay(
        mockedChunkedOutput0,
        chunkedOutputFactory0,
        mockedChunkedOutput1,
        chunkedOutputFactory1,
        clock);

    // run test
    FakeAsyncResponse fakeAsyncResponse1 = new FakeAsyncResponse();
    produceAction0.produce(fakeAsyncResponse1, "clusterId", "topicName", requests0);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction1.produce(fakeAsyncResponse2, "clusterId", "topicName", requests1);
    FakeAsyncResponse fakeAsyncResponse3 = new FakeAsyncResponse();
    produceAction1.produce(fakeAsyncResponse3, "clusterId", "topicName", requests1);
    FakeAsyncResponse fakeAsyncResponse4 = new FakeAsyncResponse();
    produceAction0.produce(fakeAsyncResponse4, "clusterId", "topicName", requests0);
    FakeAsyncResponse fakeAsyncResponse5 = new FakeAsyncResponse();
    produceAction1.produce(fakeAsyncResponse5, "clusterId", "topicName", requests1);

    // check results
    EasyMock.verify(requests0);
    EasyMock.verify(requests1);
    EasyMock.verify(mockedChunkedOutput0, mockedChunkedOutput1);
  }

  @Test
  public void produceCombinationsHittingRateAndGraceLimit() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD = 7;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(999999999));
    properties.put(PRODUCE_GRACE_PERIOD_MS, Integer.toString(10));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);
    Clock clock = mock(Clock.class);
    ProduceAction produceAction =
        getProduceAction(
            properties, chunkedOutputFactory, clock, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);
    MappingIterator<ProduceRequest> requests =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);

    // expected results
    ProduceResponse produceResponse1 = getProduceResponse(0);
    ResultOrError resultOrErrorOKProd1 = ResultOrError.result(produceResponse1);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd1);
    mockedChunkedOutput.close();

    ProduceResponse produceResponse2 = getProduceResponse(1, Optional.of(Duration.ofMillis(1000)));
    ResultOrError resultOrErrorOKProd2 = ResultOrError.result(produceResponse2);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd2);
    mockedChunkedOutput.close();

    ProduceResponse produceResponse3 = getProduceResponse(2);
    ResultOrError resultOrErrorOKProd3 = ResultOrError.result(produceResponse3);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd3);
    mockedChunkedOutput.close();

    ProduceResponse produceResponse4 = getProduceResponse(3, Optional.of(Duration.ofMillis(1000)));
    ResultOrError resultOrErrorOKProd4 = ResultOrError.result(produceResponse4);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd4);
    mockedChunkedOutput.close();

    ProduceResponse produceResponse5 = getProduceResponse(4, Optional.of(Duration.ofMillis(2000)));
    ResultOrError resultOrErrorOKProd5 = ResultOrError.result(produceResponse5);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd5);
    mockedChunkedOutput.close();

    ErrorResponse err =
        ErrorResponse.create(429, "Rate limit exceeded : Connection will be closed.");
    ResultOrError resultOrErrorProd6 = ResultOrError.error(err);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorProd6);
    mockedChunkedOutput.close();
    mockedChunkedOutput.close();

    ProduceResponse produceResponse7 = getProduceResponse(5);
    ResultOrError resultOrErrorOKProd7 = ResultOrError.result(produceResponse7);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd7);
    mockedChunkedOutput.close();

    replay(mockedChunkedOutput, chunkedOutputFactory);

    expect(clock.millis()).andReturn(0L);
    expect(clock.millis()).andReturn(1L);
    expect(clock.millis()).andReturn(1011L);
    expect(clock.millis()).andReturn(1012L);
    expect(clock.millis()).andReturn(1013L);
    expect(clock.millis()).andReturn(1024L);
    expect(clock.millis()).andReturn(2025L);
    replay(clock);

    // run test
    FakeAsyncResponse fakeAsyncResponse1 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse1, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse2, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse3 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse3, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse4 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse4, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse5 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse5, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse6 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse6, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse7 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse7, "clusterId", "topicName", requests);

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
    properties.put(PRODUCE_GRACE_PERIOD_MS, Integer.toString(30000));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1);
    Clock clock = mock(Clock.class);

    ProduceAction produceAction1 =
        getProduceAction(properties, chunkedOutputFactory, clock, TOTAL_NUMBER_OF_STREAMING_CALLS);
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

    expect((clock.millis())).andReturn(0L);
    expect((clock.millis())).andReturn(1L);
    expect((clock.millis())).andReturn(2L);
    expect((clock.millis())).andReturn(3L);
    expect((clock.millis())).andReturn(4L);

    replay(mockedChunkedOutput, chunkedOutputFactory, clock);

    // run test
    FakeAsyncResponse fakeAsyncResponse1 = new FakeAsyncResponse();
    produceAction1.produce(fakeAsyncResponse1, "clusterId", "topicName", requests);

    // check results
    EasyMock.verify(requests);
    EasyMock.verify(mockedChunkedOutput);
  }

  @Test
  public void produceStreamingCombinationsHittingRateAndGraceLimit() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD = 5;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(999999999));
    properties.put(PRODUCE_GRACE_PERIOD_MS, Integer.toString(10));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);
    Clock clock = mock(Clock.class);
    ProduceAction produceAction =
        getProduceAction(
            properties, chunkedOutputFactory, clock, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);

    MappingIterator<ProduceRequest> requests =
        getStreamingProduceRequestsMappingIteratorCombinations(clock);

    // expected results
    ProduceResponse produceResponse1 = getProduceResponse(0);
    ResultOrError resultOrErrorOKProd1 = ResultOrError.result(produceResponse1);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd1);

    ProduceResponse produceResponse2 = getProduceResponse(1, Optional.of(Duration.ofMillis(1000)));
    ResultOrError resultOrErrorOKProd2 = ResultOrError.result(produceResponse2);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd2);

    ProduceResponse produceResponse3 = getProduceResponse(2);
    ResultOrError resultOrErrorOKProd3 = ResultOrError.result(produceResponse3);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd3);

    ProduceResponse produceResponse4 = getProduceResponse(3, Optional.of(Duration.ofMillis(1000)));
    ResultOrError resultOrErrorOKProd4 = ResultOrError.result(produceResponse4);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd4);

    ProduceResponse produceResponse5 = getProduceResponse(4, Optional.of(Duration.ofMillis(2000)));
    ResultOrError resultOrErrorOKProd5 = ResultOrError.result(produceResponse5);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOKProd5);

    ErrorResponse err =
        ErrorResponse.create(429, "Rate limit exceeded : Connection will be closed.");
    ResultOrError resultOrErrorProd6 = ResultOrError.error(err);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorProd6);
    mockedChunkedOutput.close();
    mockedChunkedOutput.close();

    replay(mockedChunkedOutput, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse1 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse1, "clusterId", "topicName", requests);

    // check results
    EasyMock.verify(requests);
    EasyMock.verify(mockedChunkedOutput);
  }

  @Test
  public void produceWithByteLimit() throws Exception {
    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS = 2;
    Properties properties = new Properties();
    properties.put(PRODUCE_GRACE_PERIOD_MS, "0");
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "100");
    properties.put(
        PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(30)); // first record is 25 bytes long
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, "3600000");
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS);
    Clock clock = mock(Clock.class);
    ProduceAction produceAction = getProduceAction(properties, chunkedOutputFactory, clock, 1);
    MappingIterator<ProduceRequest> requests =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS);

    // expected results
    ProduceResponse produceResponse = getProduceResponse(0);
    ResultOrError resultOrErrorOK = ResultOrError.result(produceResponse);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorOK); // successful first produce
    mockedChunkedOutput.close();

    ErrorResponse err =
        ErrorResponse.create(429, "Rate limit exceeded : Connection will be closed.");
    ResultOrError resultOrErrorFail = ResultOrError.error(err);
    expect(mockedChunkedOutput.isClosed()).andReturn(false);
    mockedChunkedOutput.write(resultOrErrorFail); // failing second produce
    mockedChunkedOutput.close(); // error close
    mockedChunkedOutput.close(); // second close always happens on tidy up

    expect(clock.millis()).andReturn(0L);
    expect(clock.millis()).andReturn(1L);

    replay(mockedChunkedOutput, chunkedOutputFactory, clock);

    // run test
    FakeAsyncResponse fakeAsyncResponse = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse, "clusterId", "topicName", requests);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse2, "clusterId", "topicName", requests);

    // check results
    EasyMock.verify(requests);
    EasyMock.verify(mockedChunkedOutput);
  }

  private static Provider<RecordSerializer> getRecordSerializerProvider() {
    Provider<RecordSerializer> recordSerializerProvider = mock(Provider.class);
    RecordSerializer recordSerializer = mock(RecordSerializer.class);
    expect(recordSerializerProvider.get()).andReturn(recordSerializer).anyTimes();
    expect(
            recordSerializer.serialize(
                anyObject(), anyObject(), anyObject(), anyObject(), anyBoolean()))
        .andReturn(Optional.empty())
        .anyTimes();
    replay(recordSerializerProvider, recordSerializer);
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
      getStreamingProduceRequestsMappingIteratorCombinations(Clock clock) throws IOException {
    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);

    expect(clock.millis()).andReturn(0L);
    expect(clock.millis()).andReturn(1L);
    expect(clock.millis()).andReturn(1002L);
    expect(clock.millis()).andReturn(1003L);
    expect(clock.millis()).andReturn(1004L);
    expect(clock.millis()).andReturn(1015L);
    replay(clock);

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
        .setWaitForMs(
            waitFor.isPresent() ? Optional.of(waitFor.get().toMillis()) : Optional.empty())
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
      Properties properties, ChunkedOutputFactory chunkedOutputFactory, Clock clock, int times) {
    return getProduceAction(
        new ProduceRateLimiters(
            Duration.ofMillis(Integer.parseInt(properties.getProperty(PRODUCE_GRACE_PERIOD_MS))),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_REQUESTS_PER_SECOND)),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_BYTES_PER_SECOND)),
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))),
            clock),
        chunkedOutputFactory,
        times,
        0);
  }

  private static ProduceAction getProduceAction(
      ProduceRateLimiters produceRateLimiters,
      ChunkedOutputFactory chunkedOutputFactory,
      int times,
      int producerId) {
    Provider<SchemaManager> schemaManagerProvider = mock(Provider.class);
    Provider<ProducerMetrics> producerMetricsProvider = mock(Provider.class);
    getProducerMetricsProvider(producerMetricsProvider);
    Provider<RecordSerializer> recordSerializerProvider = getRecordSerializerProvider();
    Provider<ProduceController> produceControllerProvider = mock(Provider.class);
    ProduceController produceController = getProduceControllerMock(produceControllerProvider);
    setupExpectsMockCallsForProduce(produceController, times, producerId);

    replay(producerMetricsProvider, produceControllerProvider, produceController);

    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(chunkedOutputFactory);

    // get the current thread so that the call counts can be seen by easy mock
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();

    ProduceAction produceAction =
        new ProduceAction(
            schemaManagerProvider,
            recordSerializerProvider,
            produceControllerProvider,
            producerMetricsProvider,
            streamingResponseFactory,
            produceRateLimiters,
            executorService);
    produceRateLimiters.clear();
    return produceAction;
  }

  private static void getProducerMetricsProvider(
      Provider<ProducerMetrics> producerMetricsProvider) {
    ProducerMetrics producerMetrics = mock(ProducerMetrics.class);
    expect(producerMetricsProvider.get()).andReturn(producerMetrics).anyTimes();
    ProducerMetrics.ProduceMetricMBean metricMBean = mock(ProducerMetrics.ProduceMetricMBean.class);
    expect(producerMetrics.mbean(anyObject(), anyObject())).andReturn(metricMBean).anyTimes();
    replay(producerMetrics);
  }
}
