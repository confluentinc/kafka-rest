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

import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.junit.jupiter.api.Test;

public final class DoNotRateLimitTest extends AbstractRateLimitTest {

  @Override
  final List<Class<?>> getResources() {
    return ImmutableList.of(FoobarResource.class, FozbazResource.class);
  }

  @Override
  final Properties getProperties() {
    Properties properties = new Properties();
    properties.put("rate.limit.enable", "true");
    properties.put("rate.limit.backend", "guava");
    properties.put("rate.limit.permits.per.sec", "500");
    properties.put("rate.limit.default.cost", "1");
    properties.put("rate.limit.costs", "");
    return properties;
  }

  @Test
  public void doNotRateLimitOnClass() {
    int oks = hammerAtConstantRate("foobar/foo", Duration.ofMillis(1), 0, 1000);
    assertEquals(1000, oks);
  }

  @Test
  public void doNotRateLimitOnMethod() {
    int oks = hammerAtConstantRate("fozbaz/foz", Duration.ofMillis(1), 0, 1000);
    assertEquals(1000, oks);
  }

  @DoNotRateLimit
  @Path("foobar")
  @ResourceName("foobar.*")
  public static final class FoobarResource {

    @GET
    @Path("foo")
    @ResourceName("foobar.foo")
    public String foo() {
      return "foo";
    }
  }

  @Path("fozbaz")
  @ResourceName("fozbaz.*")
  public static final class FozbazResource {

    @DoNotRateLimit
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
