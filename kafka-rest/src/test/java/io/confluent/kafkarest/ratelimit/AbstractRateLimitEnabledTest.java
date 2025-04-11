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
import org.junit.jupiter.api.Test;

abstract class AbstractRateLimitEnabledTest extends AbstractRateLimitTest {

  abstract RateLimitBackend getBackend();

  abstract Duration getRate();

  abstract int getWarmupRequests();

  abstract int getTotalRequests();

  abstract int getSlack();

  @Override
  final List<Class<?>> getResources() {
    return ImmutableList.of(FoobarResource.class, FozbazResource.class);
  }

  @Override
  final Properties getProperties() {
    Properties properties = new Properties();
    properties.put("rate.limit.enable", "true");
    properties.put("rate.limit.backend", getBackend().toString());
    properties.put("rate.limit.permits.per.sec", "500");
    properties.put("rate.limit.default.cost", "1");
    properties.put("rate.limit.costs", "foobar.*=2,foobar.foo=4,fozbaz.baz=0");
    return properties;
  }

  @Test
  public void rateLimitWithDefaultCost() {
    int oks = hammerAtConstantRate("fozbaz/foz");
    assertInRange(500 - getSlack(), 500 + getSlack(), oks);
  }

  @Test
  public void rateLimitWithZeroCost() {
    int oks = hammerAtConstantRate("fozbaz/baz");
    assertEquals(getTotalRequests() - getWarmupRequests(), oks);
  }

  @Test
  public void rateLimitWithClassCost() {
    int oks = hammerAtConstantRate("foobar/bar");
    assertInRange(250 - getSlack(), 250 + getSlack(), oks);
  }

  @Test
  public void rateLimitWithMethodCost() {
    int oks = hammerAtConstantRate("foobar/foo");
    assertInRange(125 - getSlack(), 125 + getSlack(), oks);
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

  @Path("fozbaz")
  @ResourceName("fozbaz.*")
  public static final class FozbazResource {

    @GET
    @Path("foz")
    @ResourceName("fozbaz.foz")
    public String foo() {
      return "foz";
    }

    @GET
    @Path("baz")
    @ResourceName("fozbaz.baz")
    public String baz() {
      return "baz";
    }
  }
}
