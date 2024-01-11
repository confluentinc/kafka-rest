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

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.junit.jupiter.api.Test;

public final class RateLimitDisabledTest extends AbstractRateLimitTest {

  @Override
  List<Class<?>> getResources() {
    return singletonList(FooResource.class);
  }

  @Override
  Properties getProperties() {
    Properties properties = new Properties();
    properties.put("rate.limit.enable", "false");
    return properties;
  }

  @Test
  public void rateLimitDisabled_doesNotRateLimit() {
    long oks = hammerAtConstantRate("foobar", Duration.ofMillis(1), 0, 1000);
    assertEquals(1000, oks);
  }

  @Path("foobar")
  public static final class FooResource {

    @GET
    public String bar() {
      return "foobar";
    }
  }
}
