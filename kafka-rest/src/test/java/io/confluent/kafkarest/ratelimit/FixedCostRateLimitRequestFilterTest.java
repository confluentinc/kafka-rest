/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.ratelimit;

import static io.confluent.kafkarest.ratelimit.RateLimitExceededException.ErrorCodes.PERMITS_MAX_GLOBAL_LIMIT_EXCEEDED;
import static io.confluent.kafkarest.ratelimit.RateLimitExceededException.ErrorCodes.PERMITS_MAX_PER_CLUSTER_LIMIT_EXCEEDED;
import static io.confluent.kafkarest.requestlog.CustomLogRequestAttributes.REST_ERROR_CODE;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.UriInfo;
import org.glassfish.jersey.internal.util.collection.ImmutableMultivaluedMap;
import org.junit.jupiter.api.Test;

class FixedCostRateLimitRequestFilterTest {

  private static final String CLUSTER_ID = "lkc-mock";

  @Test
  void invalidCost__throwIllegalArgumentException() {
    // prepare
    RequestRateLimiter genericRateLimiter = mock(RequestRateLimiter.class);
    LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache = mock(LoadingCache.class);

    // act and check
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new FixedCostRateLimitRequestFilter(genericRateLimiter, 0, perClusterRateLimiterCache),
        "Cost must be positive");
  }

  @Test
  void filter_requestNotContainClusterId__ok() {
    // prepare
    RequestRateLimiter genericRateLimiter = mock(RequestRateLimiter.class);
    LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache = mock(LoadingCache.class);
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo mockUriInfo = mock(UriInfo.class);

    expect(mockUriInfo.getPathParameters(anyBoolean())).andReturn(ImmutableMultivaluedMap.empty());
    expect(requestContext.getUriInfo()).andReturn(mockUriInfo);
    genericRateLimiter.rateLimit(anyInt());
    replay(requestContext, mockUriInfo, genericRateLimiter, perClusterRateLimiterCache);

    FixedCostRateLimitRequestFilter fixedCostRateLimitRequestFilter =
        new FixedCostRateLimitRequestFilter(genericRateLimiter, 1, perClusterRateLimiterCache);

    // act
    fixedCostRateLimitRequestFilter.filter(requestContext);

    // check
    verify(genericRateLimiter, perClusterRateLimiterCache, requestContext, mockUriInfo);
  }

  @Test
  void filter_requestNotContainClusterId__exceeded() {
    // prepare
    RequestRateLimiter genericRateLimiter = mock(RequestRateLimiter.class);
    LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache = mock(LoadingCache.class);
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo mockUriInfo = mock(UriInfo.class);

    expect(mockUriInfo.getPathParameters(anyBoolean())).andReturn(ImmutableMultivaluedMap.empty());
    expect(requestContext.getUriInfo()).andReturn(mockUriInfo);
    genericRateLimiter.rateLimit(anyInt());
    expectLastCall().andThrow(new RateLimitExceededException());
    requestContext.setProperty(REST_ERROR_CODE, PERMITS_MAX_GLOBAL_LIMIT_EXCEEDED);
    expectLastCall();

    replay(requestContext, mockUriInfo, genericRateLimiter, perClusterRateLimiterCache);

    FixedCostRateLimitRequestFilter fixedCostRateLimitRequestFilter =
        new FixedCostRateLimitRequestFilter(genericRateLimiter, 1, perClusterRateLimiterCache);

    // act
    assertThrows(
        RateLimitExceededException.class,
        () -> fixedCostRateLimitRequestFilter.filter(requestContext));

    // check
    verify(genericRateLimiter, perClusterRateLimiterCache, requestContext, mockUriInfo);
  }

  @Test
  void filter_requestContainsClusterId__throwUncheckedExecutionException() {
    // prepare
    RequestRateLimiter genericRateLimiter = mock(RequestRateLimiter.class);
    LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache = mock(LoadingCache.class);
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo mockUriInfo = mock(UriInfo.class);

    expect(mockUriInfo.getPathParameters(anyBoolean()))
        .andReturn(new MultivaluedHashMap<>(ImmutableMap.of("clusterId", CLUSTER_ID)));
    expect(requestContext.getUriInfo()).andReturn(mockUriInfo);
    expect(perClusterRateLimiterCache.getUnchecked(CLUSTER_ID))
        .andThrow(new UncheckedExecutionException("Something went wrong", new Exception()));

    replay(requestContext, mockUriInfo, genericRateLimiter, perClusterRateLimiterCache);

    FixedCostRateLimitRequestFilter fixedCostRateLimitRequestFilter =
        new FixedCostRateLimitRequestFilter(genericRateLimiter, 1, perClusterRateLimiterCache);
    // act
    assertThrows(
        UncheckedExecutionException.class,
        () -> fixedCostRateLimitRequestFilter.filter(requestContext));

    // check
    verify(genericRateLimiter, perClusterRateLimiterCache, requestContext, mockUriInfo);
  }

  @Test
  void filter_requestContainsClusterId__ok() {
    // prepare
    RequestRateLimiter genericRateLimiter = mock(RequestRateLimiter.class);
    RequestRateLimiter cachedRateLimiter = mock(RequestRateLimiter.class);
    LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache = mock(LoadingCache.class);
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo mockUriInfo = mock(UriInfo.class);

    expect(mockUriInfo.getPathParameters(anyBoolean()))
        .andReturn(new MultivaluedHashMap<>(ImmutableMap.of("clusterId", CLUSTER_ID)));
    expect(requestContext.getUriInfo()).andReturn(mockUriInfo);
    expect(perClusterRateLimiterCache.getUnchecked(CLUSTER_ID)).andReturn(cachedRateLimiter);
    cachedRateLimiter.rateLimit(anyInt());
    genericRateLimiter.rateLimit(anyInt());

    replay(
        requestContext,
        mockUriInfo,
        genericRateLimiter,
        cachedRateLimiter,
        perClusterRateLimiterCache);

    FixedCostRateLimitRequestFilter fixedCostRateLimitRequestFilter =
        new FixedCostRateLimitRequestFilter(genericRateLimiter, 1, perClusterRateLimiterCache);
    // act
    fixedCostRateLimitRequestFilter.filter(requestContext);

    // check
    verify(
        genericRateLimiter,
        cachedRateLimiter,
        perClusterRateLimiterCache,
        requestContext,
        mockUriInfo);
  }

  @Test
  void filter_requestContainClusterId__exceededPerClusterRateLimiter() {
    // prepare
    RequestRateLimiter genericRateLimiter = mock(RequestRateLimiter.class);
    RequestRateLimiter cachedRateLimiter = mock(RequestRateLimiter.class);
    LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache = mock(LoadingCache.class);
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo mockUriInfo = mock(UriInfo.class);

    expect(mockUriInfo.getPathParameters(anyBoolean()))
        .andReturn(new MultivaluedHashMap<>(ImmutableMap.of("clusterId", CLUSTER_ID)));
    expect(requestContext.getUriInfo()).andReturn(mockUriInfo);
    expect(perClusterRateLimiterCache.getUnchecked(CLUSTER_ID)).andReturn(cachedRateLimiter);
    cachedRateLimiter.rateLimit(anyInt());
    expectLastCall().andThrow(new RateLimitExceededException());
    requestContext.setProperty(REST_ERROR_CODE, PERMITS_MAX_PER_CLUSTER_LIMIT_EXCEEDED);
    expectLastCall();

    replay(
        requestContext,
        mockUriInfo,
        genericRateLimiter,
        cachedRateLimiter,
        perClusterRateLimiterCache);

    FixedCostRateLimitRequestFilter fixedCostRateLimitRequestFilter =
        new FixedCostRateLimitRequestFilter(genericRateLimiter, 1, perClusterRateLimiterCache);
    // act
    assertThrows(
        RateLimitExceededException.class,
        () -> fixedCostRateLimitRequestFilter.filter(requestContext));

    // check
    verify(
        genericRateLimiter,
        cachedRateLimiter,
        perClusterRateLimiterCache,
        requestContext,
        mockUriInfo);
  }

  @Test
  void filter_requestContainClusterId__exceededOnGenericRateLimiter() {
    // prepare
    RequestRateLimiter genericRateLimiter = mock(RequestRateLimiter.class);
    RequestRateLimiter cachedRateLimiter = mock(RequestRateLimiter.class);
    LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache = mock(LoadingCache.class);
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo mockUriInfo = mock(UriInfo.class);

    expect(mockUriInfo.getPathParameters(anyBoolean()))
        .andReturn(new MultivaluedHashMap<>(ImmutableMap.of("clusterId", CLUSTER_ID)));
    expect(requestContext.getUriInfo()).andReturn(mockUriInfo);
    expect(perClusterRateLimiterCache.getUnchecked(CLUSTER_ID)).andReturn(cachedRateLimiter);
    cachedRateLimiter.rateLimit(anyInt());
    // cluster rate limit pass but generic rate limit fail
    genericRateLimiter.rateLimit(anyInt());
    expectLastCall().andThrow(new RateLimitExceededException());
    requestContext.setProperty(REST_ERROR_CODE, PERMITS_MAX_GLOBAL_LIMIT_EXCEEDED);
    expectLastCall();

    replay(
        requestContext,
        mockUriInfo,
        genericRateLimiter,
        cachedRateLimiter,
        perClusterRateLimiterCache);

    FixedCostRateLimitRequestFilter fixedCostRateLimitRequestFilter =
        new FixedCostRateLimitRequestFilter(genericRateLimiter, 1, perClusterRateLimiterCache);
    // act
    assertThrows(
        RateLimitExceededException.class,
        () -> fixedCostRateLimitRequestFilter.filter(requestContext));

    // check
    verify(
        genericRateLimiter,
        cachedRateLimiter,
        perClusterRateLimiterCache,
        requestContext,
        mockUriInfo);
  }
}
