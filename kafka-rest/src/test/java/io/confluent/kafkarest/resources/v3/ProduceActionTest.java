package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_GRACE_PERIOD;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

import com.fasterxml.jackson.databind.MappingIterator;
import io.confluent.kafkarest.ProducerMetrics;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.mock.MockTime;
import io.confluent.kafkarest.resources.RateLimiter;
import io.confluent.kafkarest.response.ChunkedOutputFactory;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.StreamingResponse.ResultOrError;
import io.confluent.kafkarest.response.StreamingResponseFactory;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import javax.inject.Provider;
import org.easymock.EasyMock;
import org.glassfish.jersey.server.ChunkedOutput;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProduceActionTest {

  /**
   * Provider<SchemaManager> schemaManager, Provider<RecordSerializer> recordSerializer,
   * Provider<ProduceController> produceController, KafkaRestConfig config
   */
  @Test
  public void produceWithZeroGracePeriod() throws Exception {

    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS = 2;
    Properties properties = new Properties();
    properties.put(PRODUCE_GRACE_PERIOD, "0");
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "1");
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS);
    Time time = new MockTime();
    ProduceAction produceAction = getProduceAction(properties, chunkedOutputFactory, time, 1);
    MappingIterator<ProduceRequest> requests =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS);

    // expected results
    ProduceResponse produceResponse = getProduceResponse(0);
    ResultOrError resultOrErrorOK = ResultOrError.result(produceResponse);
    mockedChunkedOutput.write(resultOrErrorOK); // successful first produce
    mockedChunkedOutput.close();

    ErrorResponse err =
        ErrorResponse.create(
            429,
            "Rate limit of 1 exceeded within the grace period of 0: Connection will be closed.");
    ResultOrError resultOrErrorFail = ResultOrError.error(err);
    mockedChunkedOutput.write(resultOrErrorFail); // failing second produce
    mockedChunkedOutput.close(); // error close
    mockedChunkedOutput.close(); // second close always happens on tidy up

    replay(mockedChunkedOutput, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse, "clusterId", "topicName", requests);

    time.sleep(1);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse2, "clusterId", "topicName", requests);

    // check results
    EasyMock.verify(mockedChunkedOutput);
  }

  @Test
  public void twoProducers() throws Exception {

    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD0 = 2;
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1 = 3;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_GRACE_PERIOD, Integer.toString(10));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");

    // setup
    ChunkedOutputFactory chunkedOutputFactory0 = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput0 =
        getChunkedOutput(chunkedOutputFactory0, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD0);
    ChunkedOutputFactory chunkedOutputFactory1 = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput1 =
        getChunkedOutput(chunkedOutputFactory1, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1);
    Time time = new MockTime();

    RateLimiter rateLimiter =
        new RateLimiter(
            Integer.parseInt(properties.getProperty(PRODUCE_GRACE_PERIOD)),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_REQUESTS_PER_SECOND)),
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            time);

    ProduceAction produceAction0 =
        getProduceAction(
            rateLimiter, chunkedOutputFactory0, time, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD0, 0);
    ProduceAction produceAction1 =
        getProduceAction(
            rateLimiter, chunkedOutputFactory1, time, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1, 1);
    MappingIterator<ProduceRequest> requests0 =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD0);
    MappingIterator<ProduceRequest> requests1 =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1);

    // expected results
    ProduceResponse produceResponse1 = getProduceResponse(0, 0);
    ResultOrError resultOrErrorOKProd1 = ResultOrError.result(produceResponse1);
    mockedChunkedOutput0.write(resultOrErrorOKProd1);
    mockedChunkedOutput0.close();

    ProduceResponse produceResponse2 =
        getProduceResponse(0, Optional.of(Duration.ofMillis(1000)), 1);
    ResultOrError resultOrErrorOKProd2 = ResultOrError.result(produceResponse2);
    mockedChunkedOutput1.write(resultOrErrorOKProd2);
    mockedChunkedOutput1.close();

    ProduceResponse produceResponse3 = getProduceResponse(1, 1);
    ResultOrError resultOrErrorOKProd3 = ResultOrError.result(produceResponse3);
    mockedChunkedOutput1.write(resultOrErrorOKProd3);
    mockedChunkedOutput1.close();

    ProduceResponse produceResponse4 =
        getProduceResponse(1, Optional.of(Duration.ofMillis(1000)), 0);
    ResultOrError resultOrErrorOKProd4 = ResultOrError.result(produceResponse4);
    mockedChunkedOutput0.write(resultOrErrorOKProd4);
    mockedChunkedOutput0.close();

    ErrorResponse err =
        ErrorResponse.create(
            429,
            "Rate limit of 1 exceeded within the grace period of 10: Connection will be closed.");
    ResultOrError resultOrErrorOKProd5 = ResultOrError.error(err);
    mockedChunkedOutput1.write(resultOrErrorOKProd5);
    mockedChunkedOutput1.close();
    mockedChunkedOutput1.close();

    replay(
        mockedChunkedOutput0, chunkedOutputFactory0, mockedChunkedOutput1, chunkedOutputFactory1);

    // run test
    FakeAsyncResponse fakeAsyncResponse1 = new FakeAsyncResponse();
    produceAction0.produce(fakeAsyncResponse1, "clusterId", "topicName", requests0);

    time.sleep(1);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction1.produce(fakeAsyncResponse2, "clusterId", "topicName", requests1);

    time.sleep(1010);
    FakeAsyncResponse fakeAsyncResponse3 = new FakeAsyncResponse();
    produceAction1.produce(fakeAsyncResponse3, "clusterId", "topicName", requests1);

    time.sleep(1);
    FakeAsyncResponse fakeAsyncResponse4 = new FakeAsyncResponse();
    produceAction0.produce(fakeAsyncResponse4, "clusterId", "topicName", requests0);

    time.sleep(11);
    FakeAsyncResponse fakeAsyncResponse5 = new FakeAsyncResponse();
    produceAction1.produce(fakeAsyncResponse5, "clusterId", "topicName", requests1);

    // check results
    EasyMock.verify(mockedChunkedOutput0, mockedChunkedOutput1);
  }

  @Test
  public void produceCombinationsHittingRateAndGraceLimit() throws Exception {

    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD = 7;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_GRACE_PERIOD, Integer.toString(10));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);
    Time time = new MockTime();
    ProduceAction produceAction =
        getProduceAction(
            properties, chunkedOutputFactory, time, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);
    MappingIterator<ProduceRequest> requests =
        getProduceRequestsMappingIterator(TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);

    // expected results
    ProduceResponse produceResponse1 = getProduceResponse(0);
    ResultOrError resultOrErrorOKProd1 = ResultOrError.result(produceResponse1);
    mockedChunkedOutput.write(resultOrErrorOKProd1);
    mockedChunkedOutput.close();

    ProduceResponse produceResponse2 = getProduceResponse(1, Optional.of(Duration.ofMillis(1000)));
    ResultOrError resultOrErrorOKProd2 = ResultOrError.result(produceResponse2);
    mockedChunkedOutput.write(resultOrErrorOKProd2);
    mockedChunkedOutput.close();

    ProduceResponse produceResponse3 = getProduceResponse(2);
    ResultOrError resultOrErrorOKProd3 = ResultOrError.result(produceResponse3);
    mockedChunkedOutput.write(resultOrErrorOKProd3);
    mockedChunkedOutput.close();

    ProduceResponse produceResponse4 = getProduceResponse(3, Optional.of(Duration.ofMillis(1000)));
    ResultOrError resultOrErrorOKProd4 = ResultOrError.result(produceResponse4);
    mockedChunkedOutput.write(resultOrErrorOKProd4);
    mockedChunkedOutput.close();

    ProduceResponse produceResponse5 = getProduceResponse(4, Optional.of(Duration.ofMillis(2000)));
    ResultOrError resultOrErrorOKProd5 = ResultOrError.result(produceResponse5);
    mockedChunkedOutput.write(resultOrErrorOKProd5);
    mockedChunkedOutput.close();

    ErrorResponse err =
        ErrorResponse.create(
            429,
            "Rate limit of 1 exceeded within the grace period of 10: Connection will be closed.");
    ResultOrError resultOrErrorProd6 = ResultOrError.error(err);
    mockedChunkedOutput.write(resultOrErrorProd6);
    mockedChunkedOutput.close();
    mockedChunkedOutput.close();

    ProduceResponse produceResponse7 = getProduceResponse(5);
    ResultOrError resultOrErrorOKProd7 = ResultOrError.result(produceResponse7);
    mockedChunkedOutput.write(resultOrErrorOKProd7);
    mockedChunkedOutput.close();

    replay(mockedChunkedOutput, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse1 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse1, "clusterId", "topicName", requests);

    time.sleep(1);
    FakeAsyncResponse fakeAsyncResponse2 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse2, "clusterId", "topicName", requests);

    time.sleep(1001);
    FakeAsyncResponse fakeAsyncResponse3 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse3, "clusterId", "topicName", requests);

    time.sleep(1);
    FakeAsyncResponse fakeAsyncResponse4 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse4, "clusterId", "topicName", requests);

    time.sleep(1);
    FakeAsyncResponse fakeAsyncResponse5 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse5, "clusterId", "topicName", requests);

    time.sleep(11);
    FakeAsyncResponse fakeAsyncResponse6 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse6, "clusterId", "topicName", requests);

    time.sleep(1001);
    FakeAsyncResponse fakeAsyncResponse7 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse7, "clusterId", "topicName", requests);

    // check results
    EasyMock.verify(mockedChunkedOutput);
  }

  @Test
  public void streamingRequests() throws Exception {

    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1 = 1;
    final int TOTAL_NUMBER_OF_STREAMING_CALLS = 4;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(10000));
    properties.put(PRODUCE_GRACE_PERIOD, Integer.toString(30000));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput1 =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD1);
    Time time = new MockTime();

    ProduceAction produceAction1 =
        getProduceAction(properties, chunkedOutputFactory, time, TOTAL_NUMBER_OF_STREAMING_CALLS);
    MappingIterator<ProduceRequest> requests1 = getStreamingProduceRequestsMappingIterator(4);

    // expected results
    ProduceResponse produceResponse1 = getProduceResponse(0);
    ResultOrError resultOrErrorOKProd1 = ResultOrError.result(produceResponse1);
    mockedChunkedOutput1.write(resultOrErrorOKProd1);

    ProduceResponse produceResponse2 = getProduceResponse(1);
    ResultOrError resultOrErrorOKProd2 = ResultOrError.result(produceResponse2);
    mockedChunkedOutput1.write(resultOrErrorOKProd2);

    ProduceResponse produceResponse3 = getProduceResponse(2);
    ResultOrError resultOrErrorOKProd3 = ResultOrError.result(produceResponse3);
    mockedChunkedOutput1.write(resultOrErrorOKProd3);

    ProduceResponse produceResponse4 = getProduceResponse(3);
    ResultOrError resultOrErrorOKProd4 = ResultOrError.result(produceResponse4);
    mockedChunkedOutput1.write(resultOrErrorOKProd4);
    mockedChunkedOutput1.close();

    replay(mockedChunkedOutput1, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse1 = new FakeAsyncResponse();
    produceAction1.produce(fakeAsyncResponse1, "clusterId", "topicName", requests1);

    // check results
    EasyMock.verify(mockedChunkedOutput1);
  }

  @Test
  public void produceStreamingCombinationsHittingRateAndGraceLimit() throws Exception {

    // config
    final int TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD = 5;
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_GRACE_PERIOD, Integer.toString(10));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");

    // setup
    ChunkedOutputFactory chunkedOutputFactory = mock(ChunkedOutputFactory.class);
    ChunkedOutput<ResultOrError> mockedChunkedOutput =
        getChunkedOutput(chunkedOutputFactory, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);
    Time time = new MockTime();
    ProduceAction produceAction =
        getProduceAction(
            properties, chunkedOutputFactory, time, TOTAL_NUMBER_OF_PRODUCE_CALLS_PROD);

    MappingIterator<ProduceRequest> requests =
        getStreamingProduceRequestsMappingIteratorCombinations(time);

    // expected results
    ProduceResponse produceResponse1 = getProduceResponse(0);
    ResultOrError resultOrErrorOKProd1 = ResultOrError.result(produceResponse1);
    mockedChunkedOutput.write(resultOrErrorOKProd1);

    ProduceResponse produceResponse2 = getProduceResponse(1, Optional.of(Duration.ofMillis(1000)));
    ResultOrError resultOrErrorOKProd2 = ResultOrError.result(produceResponse2);
    mockedChunkedOutput.write(resultOrErrorOKProd2);

    ProduceResponse produceResponse3 = getProduceResponse(2);
    ResultOrError resultOrErrorOKProd3 = ResultOrError.result(produceResponse3);
    mockedChunkedOutput.write(resultOrErrorOKProd3);

    ProduceResponse produceResponse4 = getProduceResponse(3, Optional.of(Duration.ofMillis(1000)));
    ResultOrError resultOrErrorOKProd4 = ResultOrError.result(produceResponse4);
    mockedChunkedOutput.write(resultOrErrorOKProd4);

    ProduceResponse produceResponse5 = getProduceResponse(4, Optional.of(Duration.ofMillis(2000)));
    ResultOrError resultOrErrorOKProd5 = ResultOrError.result(produceResponse5);
    mockedChunkedOutput.write(resultOrErrorOKProd5);

    ErrorResponse err =
        ErrorResponse.create(
            429,
            "Rate limit of 1 exceeded within the grace period of 10: Connection will be closed.");
    ResultOrError resultOrErrorProd6 = ResultOrError.error(err);
    mockedChunkedOutput.write(resultOrErrorProd6);
    mockedChunkedOutput.close();
    mockedChunkedOutput.close();

    replay(mockedChunkedOutput, chunkedOutputFactory);

    // run test
    FakeAsyncResponse fakeAsyncResponse1 = new FakeAsyncResponse();
    produceAction.produce(fakeAsyncResponse1, "clusterId", "topicName", requests);

    // check results
    EasyMock.verify(mockedChunkedOutput);
  }

  Provider<RecordSerializer> getRecordSerializerProvider() {
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

  ProduceController getProduceControllerMock(Provider produceControllerProvider) {
    ProduceController produceController = mock(ProduceController.class);
    expect(produceControllerProvider.get()).andReturn(produceController).anyTimes();
    return produceController;
  }

  MappingIterator<ProduceRequest> getProduceRequestsMappingIterator(int times) throws IOException {
    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);
    ProduceRequest request = ProduceRequest.builder().build();
    for (int i = 0; i < times; i++) {
      expect(requests.hasNext()).andReturn(true).times(1);
      expect(requests.nextValue()).andReturn(request).times(1);
      expect(requests.hasNext()).andReturn(false).times(1);
    }
    replay(requests);
    return requests;
  }

  MappingIterator<ProduceRequest> getStreamingProduceRequestsMappingIterator(int times)
      throws IOException {
    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);

    for (int i = 0; i < times; i++) {
      ProduceRequest request = ProduceRequest.builder().build();
      expect(requests.hasNext()).andReturn(true).times(1);
      expect(requests.nextValue()).andReturn(request).times(1);
    }
    expect(requests.hasNext()).andReturn(false).times(1000);
    replay(requests);
    return requests;
  }

  MappingIterator<ProduceRequest> getStreamingProduceRequestsMappingIteratorCombinations(Time time)
      throws IOException {

    MappingIterator<ProduceRequest> requests = mock(MappingIterator.class);

    ProduceRequest request = ProduceRequest.builder().build();
    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue()).andReturn(request);

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue())
        .andAnswer(
            () -> {
              time.sleep(1);
              return request;
            });

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue())
        .andAnswer(
            () -> {
              time.sleep(1001);
              return request;
            });

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue())
        .andAnswer(
            () -> {
              time.sleep(1);
              return request;
            });

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue())
        .andAnswer(
            () -> {
              time.sleep(1);
              return request;
            });

    expect(requests.hasNext()).andReturn(true);
    expect(requests.nextValue())
        .andAnswer(
            () -> {
              time.sleep(11);
              return request;
            });

    expect(requests.hasNext()).andReturn(false).times(1000);
    replay(requests);
    return requests;
  }

  ChunkedOutput<ResultOrError> getChunkedOutput(
      ChunkedOutputFactory chunkedOutputFactory, int times) {
    ChunkedOutput<ResultOrError> mockedChunkedOutput = mock(ChunkedOutput.class);
    expect(chunkedOutputFactory.getChunkedOutput()).andReturn(mockedChunkedOutput).times(times);

    return mockedChunkedOutput;
  }

  ProduceResponse getProduceResponse(int offset) {
    return getProduceResponse(offset, Optional.empty());
  }

  ProduceResponse getProduceResponse(int offset, int partitionId) {
    return getProduceResponse(offset, Optional.empty(), partitionId);
  }

  ProduceResponse getProduceResponse(int offset, Optional<Duration> resumeAfter) {
    return getProduceResponse(offset, resumeAfter, 0);
  }

  ProduceResponse getProduceResponse(
      int offset, Optional<Duration> resumeAfterMs, int partitionId) {

    return ProduceResponse.builder()
        .setClusterId("clusterId")
        .setTopicName("topicName")
        .setPartitionId(partitionId)
        .setOffset(offset)
        .setTimestamp(Instant.ofEpochMilli(0))
        .setResumeAfterMs(
            resumeAfterMs.isPresent()
                ? Optional.of(resumeAfterMs.get().toMillis())
                : Optional.empty())
        .build();
  }

  CompletableFuture<ProduceResult> getProduceResultMock(int offset, int producerId) {
    ProduceResult produceResult = mock(ProduceResult.class);
    setExpectsForProduceResult(produceResult, offset, producerId);

    return CompletableFuture.completedFuture(produceResult);
  }

  void setExpectsForProduceResult(ProduceResult produceResult, long offset, int producerId) {
    expect(produceResult.getPartitionId()).andReturn(producerId).anyTimes();
    expect(produceResult.getOffset()).andReturn(offset).anyTimes();
    expect(produceResult.getTimestamp()).andReturn(Optional.of(Instant.ofEpochMilli(0))).anyTimes();
    expect(produceResult.getSerializedKeySize()).andReturn(1).anyTimes();
    expect(produceResult.getSerializedValueSize()).andReturn(1).anyTimes();
    expect(produceResult.getCompletionTimestamp()).andReturn(Instant.now()).anyTimes();
    replay(produceResult);
  }

  void setupExpectsMockCallsForProduce(
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

  ProduceAction getProduceAction(
      Properties properties, ChunkedOutputFactory chunkedOutputFactory, Time time, int times) {
    return getProduceAction(
        new RateLimiter(
            Integer.parseInt(properties.getProperty(PRODUCE_GRACE_PERIOD)),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_REQUESTS_PER_SECOND)),
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            time),
        chunkedOutputFactory,
        time,
        times,
        0);
  }

  ProduceAction getProduceAction(
      RateLimiter rateLimiter,
      ChunkedOutputFactory chunkedOutputFactory,
      Time time,
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

    ProduceAction produceAction =
        new ProduceAction(
            schemaManagerProvider,
            recordSerializerProvider,
            produceControllerProvider,
            producerMetricsProvider,
            chunkedOutputFactory,
            streamingResponseFactory,
            rateLimiter);
    rateLimiter.resetGracePeriodStart();
    rateLimiter.clear();

    return produceAction;
  }

  void getProducerMetricsProvider(Provider<ProducerMetrics> producerMetricsProvider) {
    ProducerMetrics producerMetrics = mock(ProducerMetrics.class);
    expect(producerMetricsProvider.get()).andReturn(producerMetrics).anyTimes();
    ProducerMetrics.ProduceMetricMBean metricMBean = mock(ProducerMetrics.ProduceMetricMBean.class);
    expect(producerMetrics.mbean(anyObject(), anyObject())).andReturn(metricMBean).anyTimes();
    replay(producerMetrics);
  }
}
