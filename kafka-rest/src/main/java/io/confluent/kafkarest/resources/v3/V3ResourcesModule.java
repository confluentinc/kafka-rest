/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.resources.v3;

import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.kafkarest.config.ConfigModule.ProduceResponseThreadPoolSizeConfig;
import io.confluent.kafkarest.config.ConfigModule.StreamingResponseMaxIdleTimeConfig;
import io.confluent.kafkarest.response.ChunkedOutputFactory;
import io.confluent.kafkarest.response.StreamingResponseFactory;
import io.confluent.kafkarest.response.StreamingResponseIdleTimeLimiter;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Qualifier;
import javax.inject.Singleton;
import org.glassfish.hk2.api.AnnotationLiteral;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

public final class V3ResourcesModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindAsContract(ProduceRateLimiters.class).in(Singleton.class);
    bindAsContract(ChunkedOutputFactory.class);
    bindAsContract(StreamingResponseFactory.class);
    bindFactory(StreamingResponseIdleTimeLimiterFactory.class)
        .to(StreamingResponseIdleTimeLimiter.class)
        .in(Singleton.class);

    bind(Clock.systemUTC()).to(Clock.class);

    bindFactory(ProduceResponseExecutorServiceFactory.class)
        .qualifiedBy(new ProduceResponseThreadPoolImpl())
        .to(ExecutorService.class)
        .in(Singleton.class);
    bindFactory(StreamingResponseIdleTimeExecutorFactory.class)
        .qualifiedBy(new StreamingResponseIdleTimeExecutorImpl())
        .to(ExecutorService.class)
        .in(Singleton.class);
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface ProduceResponseThreadPool {}

  private static final class ProduceResponseThreadPoolImpl
      extends AnnotationLiteral<ProduceResponseThreadPool> implements ProduceResponseThreadPool {}

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface StreamingResponseIdleTimeExecutor {}

  private static final class StreamingResponseIdleTimeExecutorImpl
      extends AnnotationLiteral<StreamingResponseIdleTimeExecutor>
      implements StreamingResponseIdleTimeExecutor {}

  private static final class ProduceResponseExecutorServiceFactory
      implements Factory<ExecutorService> {

    private final int produceResponseThreadPoolSize;

    @Inject
    ProduceResponseExecutorServiceFactory(
        @ProduceResponseThreadPoolSizeConfig Integer produceExecutorThreadPoolSize) {
      this.produceResponseThreadPoolSize = produceExecutorThreadPoolSize;
    }

    @Override
    public ExecutorService provide() {
      ThreadFactory namedThreadFactory =
          new ThreadFactoryBuilder().setNameFormat("kafka-rest-produce-response-thread-%d").build();
      return Executors.newFixedThreadPool(produceResponseThreadPoolSize, namedThreadFactory);
    }

    @Override
    public void dispose(ExecutorService executorService) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
      }
    }
  }

  private static final class StreamingResponseIdleTimeExecutorFactory
      implements Factory<ExecutorService> {

    @Override
    public ExecutorService provide() {
      return Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("kafka-rest-streaming-response-idle-time-thread-%d")
              .build());
    }

    @Override
    public void dispose(ExecutorService executorService) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
      }
    }
  }

  private static final class StreamingResponseIdleTimeLimiterFactory
      implements Factory<StreamingResponseIdleTimeLimiter> {
    private final ExecutorService executor;
    private final Duration maxIdleTime;

    @Inject
    StreamingResponseIdleTimeLimiterFactory(
        @StreamingResponseIdleTimeExecutor ExecutorService executor,
        @StreamingResponseMaxIdleTimeConfig Duration maxIdleTime) {
      this.executor = requireNonNull(executor);
      this.maxIdleTime = requireNonNull(maxIdleTime);
    }

    @Override
    public StreamingResponseIdleTimeLimiter provide() {
      return new StreamingResponseIdleTimeLimiter(SimpleTimeLimiter.create(executor), maxIdleTime);
    }

    @Override
    public void dispose(StreamingResponseIdleTimeLimiter idleTimeLimiter) {}
  }
}
