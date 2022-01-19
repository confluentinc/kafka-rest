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

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.config.ConfigModule;
import io.confluent.kafkarest.exceptions.ExceptionsModule;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

abstract class AbstractRateLimitTest extends JerseyTest {
  private ScheduledExecutorService executor;

  abstract List<Class<?>> getResources();

  abstract Properties getProperties();

  @Override
  protected final Application configure() {
    ResourceConfig resourceConfig = new ResourceConfig(getResources().toArray(new Class<?>[0]));
    resourceConfig.register(new ConfigModule(new KafkaRestConfig(getProperties())));
    resourceConfig.register(RateLimitFeature.class);
    resourceConfig.register(ExceptionsModule.class);
    resourceConfig.register(JacksonMessageBodyProvider.class);
    return resourceConfig;
  }

  @BeforeEach
  @Override
  public final void setUp() throws Exception {
    super.setUp();
    executor = Executors.newScheduledThreadPool(4);
  }

  @AfterEach
  @Override
  public final void tearDown() throws Exception {
    super.tearDown();
    executor.shutdownNow();
  }

  final int hammerAtConstantRate(
      String path, Duration rate, int warmupRequests, int totalRequests) {
    checkArgument(!rate.isNegative(), "rate must be non-negative");
    checkArgument(warmupRequests <= totalRequests, "warmupRequests must be at most totalRequests");

    List<Response> responses =
        IntStream.range(0, totalRequests)
            .mapToObj(
                i ->
                    executor.schedule(
                        () -> target(path).request(MediaType.APPLICATION_JSON_TYPE).get(),
                        /* delay= */ i * rate.toMillis(),
                        TimeUnit.MILLISECONDS))
            .collect(Collectors.toList()).stream()
            .map(
                future -> {
                  try {
                    return future.get();
                  } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    for (int i = warmupRequests; i < responses.size(); i++) {
      Response response = responses.get(i);
      int status = response.getStatus();
      if (status != 200 && status != 429) {
        fail(
            String.format(
                "Expected HTTP 200 or HTTP 429, but got HTTP %d instead: %s",
                status, response.readEntity(String.class)));
      }
    }

    return (int)
        responses.subList(warmupRequests, responses.size()).stream()
            .filter(response -> response.getStatus() == Status.OK.getStatusCode())
            .count();
  }

  @Override
  protected final DeploymentContext configureDeployment() {
    return super.configureDeployment();
  }

  @Override
  protected final TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return super.getTestContainerFactory();
  }

  @Override
  protected final Optional<SSLContext> getSslContext() {
    return super.getSslContext();
  }

  @Override
  protected final Optional<SSLParameters> getSslParameters() {
    return super.getSslParameters();
  }

  @Override
  protected final Client getClient() {
    return super.getClient();
  }

  @Override
  protected final Client setClient(Client client) {
    return super.setClient(client);
  }

  @Override
  protected final void configureClient(ClientConfig config) {
    super.configureClient(config);
  }

  @Override
  protected final URI getBaseUri() {
    return super.getBaseUri();
  }

  @Override
  protected final int getAsyncTimeoutMultiplier() {
    return super.getAsyncTimeoutMultiplier();
  }
}
