package io.confluent.kafkarest.resources;

import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafkarest.ratelimit.RateLimitExceededException;
import io.confluent.kafkarest.ratelimit.RequestRateLimiter;
import io.confluent.kafkarest.resources.v3.ProduceRateLimiters;
import java.time.Duration;
import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Provider;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class ProduceRateLimitersTest {

  @Test
  @Inject
  public void rateLimitingDisabledNoWaitTimeGiven() {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "false");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    replay(countLimitProvider, bytesLimitProvider, rateLimiterForCount, rateLimiterForBytes);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));
    produceRateLimiters.rateLimit("clusterId", 10L);

    verify(countLimitProvider, bytesLimitProvider, rateLimiterForCount, rateLimiterForBytes);
  }

  @Test
  @Inject
  public void waitTimesReturnedForMultipleClusters() {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    RequestRateLimiter rateLimiterForCount1 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes1 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount2 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes2 = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount1);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes1);
    rateLimiterForCount1.rateLimit(anyInt());
    rateLimiterForBytes1.rateLimit(anyInt());

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount2);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes2);
    rateLimiterForCount2.rateLimit(anyInt());
    rateLimiterForBytes2.rateLimit(anyInt());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount1,
        rateLimiterForBytes1,
        rateLimiterForCount2,
        rateLimiterForBytes2);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    produceRateLimiters.rateLimit("clusterId", 10L);

    produceRateLimiters.rateLimit("clusterId2", 10L);

    verify(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount1,
        rateLimiterForBytes1,
        rateLimiterForCount2,
        rateLimiterForBytes2);
  }

  @Test
  @Inject
  public void rateLimitedOnCountExceptionThrown() {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    rateLimiterForCount.rateLimit(anyInt());
    EasyMock.expectLastCall().andThrow(new RateLimitExceededException());

    replay(countLimitProvider, bytesLimitProvider, rateLimiterForCount, rateLimiterForBytes);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    produceRateLimiters.rateLimit("clusterId", 10L);

    RateLimitExceededException e =
        assertThrows(
            RateLimitExceededException.class,
            () -> produceRateLimiters.rateLimit("clusterId", 10L));

    assertEquals("The rate limit of requests per second has been exceeded.", e.getMessage());

    verify(countLimitProvider, bytesLimitProvider, rateLimiterForCount, rateLimiterForBytes);
  }

  @Test
  @Inject
  public void rateLimitedOnBytesExceptionThrown() {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    EasyMock.expectLastCall().andThrow(new RateLimitExceededException());

    replay(countLimitProvider, bytesLimitProvider, rateLimiterForCount, rateLimiterForBytes);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    produceRateLimiters.rateLimit("clusterId", 10L);

    RateLimitExceededException e =
        assertThrows(
            RateLimitExceededException.class,
            () -> produceRateLimiters.rateLimit("clusterId", 10L));

    assertEquals("The rate limit of requests per second has been exceeded.", e.getMessage());

    verify(countLimitProvider, bytesLimitProvider, rateLimiterForCount, rateLimiterForBytes);
  }

  @Test
  public void cacheExpiresforeRateLimit() throws InterruptedException {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(20));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());

    // these are called after the delay that will reset the cache
    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);

    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());

    replay(countLimitProvider, bytesLimitProvider, rateLimiterForCount, rateLimiterForBytes);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    produceRateLimiters.rateLimit("clusterId", 10L);

    Thread.sleep(50);
    produceRateLimiters.rateLimit("clusterId", 10L);

    verify(countLimitProvider, bytesLimitProvider, rateLimiterForCount, rateLimiterForBytes);
  }
}
