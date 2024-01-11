package io.confluent.kafkarest.resources;

import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_GRACE_PERIOD_MS;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import io.confluent.kafkarest.resources.v3.ProduceRateLimiters;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProduceRateLimitersTest {

  @Test
  public void rateLimitingDisabledNoWaitTimeGiven() {
    Clock clock = mock(Clock.class);

    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(100));
    properties.put(PRODUCE_GRACE_PERIOD_MS, Integer.toString(10));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "false");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            Duration.ofMillis(Integer.parseInt(properties.getProperty(PRODUCE_GRACE_PERIOD_MS))),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_REQUESTS_PER_SECOND)),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_BYTES_PER_SECOND)),
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))),
            clock);
    Optional<Duration> waitTime = produceRateLimiters.calculateGracePeriodExceeded("clusterId", 10);

    assertFalse(waitTime.isPresent());
  }

  @Test
  public void newRateLimiterReturnsNoWait() {
    Clock clock = mock(Clock.class);

    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(100));
    properties.put(PRODUCE_GRACE_PERIOD_MS, Integer.toString(10));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            Duration.ofMillis(Integer.parseInt(properties.getProperty(PRODUCE_GRACE_PERIOD_MS))),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_REQUESTS_PER_SECOND)),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_BYTES_PER_SECOND)),
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))),
            clock);
    Optional<Duration> waitTime = produceRateLimiters.calculateGracePeriodExceeded("clusterId", 10);

    assertFalse(waitTime.isPresent());
  }

  @Test
  public void waitTimesReturnedForMultipleClusters() {
    Clock clock = mock(Clock.class);
    expect(clock.millis()).andReturn(0L);
    expect(clock.millis()).andReturn(1L);
    expect(clock.millis()).andReturn(2L);
    replay(clock);

    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(100));
    properties.put(PRODUCE_GRACE_PERIOD_MS, Integer.toString(10));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            Duration.ofMillis(Integer.parseInt(properties.getProperty(PRODUCE_GRACE_PERIOD_MS))),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_REQUESTS_PER_SECOND)),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_BYTES_PER_SECOND)),
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))),
            clock);

    Optional<Duration> waitTime = produceRateLimiters.calculateGracePeriodExceeded("clusterId", 10);
    assertFalse(waitTime.isPresent());
    waitTime = produceRateLimiters.calculateGracePeriodExceeded("clusterId", 10);
    assertTrue(waitTime.isPresent());
    assertEquals(waitTime.get().toMillis(), 1000);
    Optional<Duration> waitTime2 =
        produceRateLimiters.calculateGracePeriodExceeded("clusterId2", 10);
    assertFalse(waitTime2.isPresent());
    assertTrue(waitTime.isPresent());
    assertEquals(waitTime.get().toMillis(), 1000);
  }

  @Test
  public void gracePeriodExceptionThrown() {
    Clock clock = mock(Clock.class);
    expect(clock.millis()).andReturn(0L);
    expect(clock.millis()).andReturn(1L);
    replay(clock);

    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(100));
    properties.put(PRODUCE_GRACE_PERIOD_MS, Integer.toString(0));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            Duration.ofMillis(Integer.parseInt(properties.getProperty(PRODUCE_GRACE_PERIOD_MS))),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_REQUESTS_PER_SECOND)),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_BYTES_PER_SECOND)),
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))),
            clock);

    Optional<Duration> waitTime = produceRateLimiters.calculateGracePeriodExceeded("clusterId", 10);
    assertFalse(waitTime.isPresent());
    try {
      produceRateLimiters.calculateGracePeriodExceeded("clusterId", 10);
      fail("RateLimitGracePeriodExceededException should be thrown");
    } catch (RateLimitGracePeriodExceededException e) {
      assertEquals("Connection will be closed.", e.getMessage());
    }
  }

  @Test
  public void cacheExpiresforeRateLimit() throws InterruptedException {
    Clock clock = mock(Clock.class);
    expect(clock.millis()).andReturn(0L);
    expect(clock.millis()).andReturn(1L);
    expect(clock.millis()).andReturn(700L);
    replay(clock);

    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(1));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(100));
    properties.put(PRODUCE_GRACE_PERIOD_MS, Integer.toString(10000));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(200));

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            Duration.ofMillis(Integer.parseInt(properties.getProperty(PRODUCE_GRACE_PERIOD_MS))),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_REQUESTS_PER_SECOND)),
            Integer.parseInt(properties.getProperty(PRODUCE_MAX_BYTES_PER_SECOND)),
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))),
            clock);

    Optional<Duration> waitTime = produceRateLimiters.calculateGracePeriodExceeded("clusterId", 10);
    assertFalse(waitTime.isPresent());
    waitTime = produceRateLimiters.calculateGracePeriodExceeded("clusterId", 10);
    assertTrue(waitTime.isPresent());
    assertEquals(waitTime.get().toMillis(), 1000);
    Thread.sleep(400);
    waitTime = produceRateLimiters.calculateGracePeriodExceeded("clusterId", 10);
    assertFalse(waitTime.isPresent());
  }
}
