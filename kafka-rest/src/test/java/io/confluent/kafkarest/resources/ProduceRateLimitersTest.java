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

package io.confluent.kafkarest.resources;

import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafkarest.ratelimit.RateLimitExceededException;
import io.confluent.kafkarest.ratelimit.RequestRateLimiter;
import io.confluent.kafkarest.resources.v3.ProduceRateLimiters;
import java.time.Duration;
import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;

public class ProduceRateLimitersTest {

  @Test
  @Inject
  public void test_thatRateLimitingCanBeDisabled() {
    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "false");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));
    assertDoesNotThrow(() -> produceRateLimiters.rateLimit("clusterId", 10L, mockRequest));

    verify(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        mockRequest);
  }

  @Test
  @Inject
  public void test_whenBelowThreshold_CallsArentRateLimited() {
    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount1 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes1 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount2 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes2 = mock(RequestRateLimiter.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount1);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes1);
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    rateLimiterForCount1.rateLimit(anyInt());
    rateLimiterForBytes1.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount2);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes2);
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    rateLimiterForCount2.rateLimit(anyInt());
    rateLimiterForBytes2.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount1,
        rateLimiterForBytes1,
        rateLimiterForCount2,
        rateLimiterForBytes2,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    produceRateLimiters.rateLimit("clusterId", 10L, mockRequest);

    produceRateLimiters.rateLimit("clusterId2", 10L, mockRequest);

    verify(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount1,
        rateLimiterForBytes1,
        rateLimiterForCount2,
        rateLimiterForBytes2,
        countLimiterGlobal,
        bytesLimiterGlobal,
        mockRequest);
  }

  @Test
  @Inject
  public void test_whenLocalCountRateLimiterBreached_thenExceptionThrown() {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    mockRequest.setAttribute(anyString(), anyInt());
    expectLastCall();

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
    expectLastCall().andThrow(new RateLimitExceededException());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    produceRateLimiters.rateLimit("clusterId", 10L, mockRequest);

    RateLimitExceededException e =
        assertThrows(
            RateLimitExceededException.class,
            () -> produceRateLimiters.rateLimit("clusterId", 10L, mockRequest));

    assertEquals("The rate limit of requests per second has been exceeded.", e.getMessage());

    verify(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        mockRequest);
  }

  @Test
  @Inject
  public void test_whenLocalBytesRateLimiterBreached_thenExceptionThrown() {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    mockRequest.setAttribute(anyString(), anyInt());
    expectLastCall();

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
    expectLastCall().andThrow(new RateLimitExceededException());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    produceRateLimiters.rateLimit("clusterId", 10L, mockRequest);

    RateLimitExceededException e =
        assertThrows(
            RateLimitExceededException.class,
            () -> produceRateLimiters.rateLimit("clusterId", 10L, mockRequest));

    assertEquals("The rate limit of requests per second has been exceeded.", e.getMessage());

    verify(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        mockRequest);
  }

  @Test
  public void test_thatCacheForRateLimitersExpires() throws InterruptedException {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(20));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());

    // these are called after the delay that will reset the cache
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
        bytesLimiterGlobalProvider,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    produceRateLimiters.rateLimit("clusterId", 10L, mockRequest);

    Thread.sleep(50);
    // Cache should have expired, and count reset back to 10L.
    produceRateLimiters.rateLimit("clusterId", 10L, mockRequest);

    verify(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        mockRequest);
  }

  @Test
  @Inject
  public void test_whenGlobalCountLimitHit_thenExceptionThrown() {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount1 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes1 = mock(RequestRateLimiter.class);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    mockRequest.setAttribute(anyString(), anyInt());
    expectLastCall();

    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);

    countLimiterGlobal.rateLimit(anyInt());
    expectLastCall().andThrow(new RateLimitExceededException());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount1,
        rateLimiterForBytes1,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    RateLimitExceededException e =
        assertThrows(
            RateLimitExceededException.class,
            () -> produceRateLimiters.rateLimit("clusterId1", 10L, mockRequest));

    verify(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount1,
        rateLimiterForBytes1,
        countLimiterGlobal,
        bytesLimiterGlobal,
        mockRequest);
  }

  @Test
  @Inject
  public void test_whenGlobalBytesLimitHit_thenExceptionThrown() {

    Properties properties = new Properties();
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount1 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes1 = mock(RequestRateLimiter.class);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    mockRequest.setAttribute(anyString(), anyInt());
    expectLastCall();

    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal);
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal);
    countLimiterGlobal.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    expectLastCall().andThrow(new RateLimitExceededException());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount1,
        rateLimiterForBytes1,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    RateLimitExceededException e =
        assertThrows(
            RateLimitExceededException.class,
            () -> produceRateLimiters.rateLimit("clusterId1", 10L, mockRequest));

    verify(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount1,
        rateLimiterForBytes1,
        countLimiterGlobal,
        bytesLimiterGlobal,
        mockRequest);
  }
}
