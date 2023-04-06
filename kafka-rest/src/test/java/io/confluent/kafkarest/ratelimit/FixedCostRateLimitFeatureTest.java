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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

// TODO This continues being way too flaky.
// Until we fix it (KREST-3850), we should ignore it, as it might be hiding even worse errors.
@Disabled
public class FixedCostRateLimitFeatureTest extends AbstractRateLimitTest {

  RateLimitBackend getBackend() {
    return RateLimitBackend.RESILIENCE4J;
  }

  Duration getRate() {
    return Duration.ofMillis(1);
  }

  int getWarmupRequests() {
    return 750;
  }

  int getTotalRequests() {
    return 1500;
  }

  int getSlack() {
    return 0;
  }

  @Override
  final List<Class<?>> getResources() {
    return ImmutableList.of(FoobarResource.class, FozbazResource.class);
  }

  @Override
  final Properties getProperties() {
    Properties properties = new Properties();
    properties.put("rate.limit.enable", "true");
    properties.put("rate.limit.backend", getBackend().toString());
    // generic (global) rate limit permits
    properties.put("rate.limit.permits.per.sec", "500");
    // per cluster rate limit permits
    properties.put("rate.limit.per.cluster.permits.per.sec", "400");
    properties.put("rate.limit.default.cost", "1");
    properties.put("rate.limit.costs", "foobar.*=2,foobar.foo=4,fozbaz.baz=0");
    return properties;
  }

  // tests on endpoint without clusterId in path parameters
  @Test
  public void rateLimitGenericWithClassCost() {
    int oks = hammerAtConstantRate("foobar/bar");
    // each foobar/bar call cost 2, so we can only get up to 500/2 = 250 requests in
    assertInRange(250 - getSlack(), 250 + getSlack(), oks);
  }

  @Test
  public void rateLimitGenericWithMethodCost() {
    int oks = hammerAtConstantRate("foobar/foo");
    // each foobar/foo call cost 4, so we can only get up to 500/4 = 125 requests in
    assertInRange(125 - getSlack(), 125 + getSlack(), oks);
  }

  // tests on endpoint with clusterId in path parameters
  @Test
  public void rateLimitPerClusterWithDefaultCost() {
    int oks = hammerAtConstantRate("fozbaz/lkc-mock/foz");
    // default cost is 1, so we can only get up to 400 (per cluster permits) requests in
    assertInRange(400 - getSlack(), 400 + getSlack(), oks);
  }

  @Test
  public void rateLimitPerClusterWithZeroCost() {
    int oks = hammerAtConstantRate("fozbaz/lkc-mock/baz");
    // cost is 0, so no rate limit is applied, all requests are in
    assertEquals(getTotalRequests() - getWarmupRequests(), oks);
  }

  private int hammerAtConstantRate(String path) {
    return hammerAtConstantRate(path, getRate(), getWarmupRequests(), getTotalRequests());
  }

  private static void assertInRange(int lower, int upper, int actual) {
    assertTrue(
        actual >= lower && actual <= upper,
        String.format("%d not in [%d, %d]", actual, lower, upper));
  }

  @Path("foobar")
  @ResourceName("foobar.*")
  public static final class FoobarResource {

    @GET
    @Path("foo")
    @ResourceName("foobar.foo")
    public String foo() {
      return "foo";
    }

    @GET
    @Path("bar")
    @ResourceName("foobar.bar")
    public String bar() {
      return "bar";
    }
  }

  @Path("fozbaz/{clusterId}")
  @ResourceName("fozbaz.*")
  public static final class FozbazResource {

    @GET
    @Path("foz")
    @ResourceName("fozbaz.foz")
    public String foo(@PathParam("clusterId") String clusterId) {
      return "foz";
    }

    @GET
    @Path("baz")
    @ResourceName("fozbaz.baz")
    public String baz(@PathParam("clusterId") String clusterId) {
      return "baz";
    }
  }
}
