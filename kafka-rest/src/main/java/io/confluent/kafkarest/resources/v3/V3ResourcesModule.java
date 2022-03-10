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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.config.ConfigModule.ProduceResponseThreadPoolSizeConfig;
import io.confluent.kafkarest.response.ChunkedOutputFactory;
import io.confluent.kafkarest.response.StreamingResponseFactory;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Provider;
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
    bind(Clock.systemUTC()).to(Clock.class);
    bindFactory(ProduceResponseExecutorServiceFactory.class)
        .qualifiedBy(new ProduceResponseThreadPoolImpl())
        .to(ExecutorService.class)
        .in(Singleton.class);
    bindFactory(ProducerMetricsFactory.class).to(ProducerMetrics.class).in(Singleton.class);
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface ProduceResponseThreadPool {}

  private static final class ProduceResponseThreadPoolImpl
      extends AnnotationLiteral<ProduceResponseThreadPool> implements ProduceResponseThreadPool {}

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
          new ThreadFactoryBuilder().setNameFormat("Produce-response-thread-%d").build();
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

  private static final class ProducerMetricsFactory implements Factory<ProducerMetrics> {
    private final Provider<KafkaRestConfig> config;

    @Inject
    ProducerMetricsFactory(Provider<KafkaRestConfig> config) {
      this.config = requireNonNull(config);
    }

    @Override
    public ProducerMetrics provide() {
      return new ProducerMetrics(config.get(), Time.SYSTEM);
    }

    @Override
    public void dispose(ProducerMetrics producerMetrics) {}
  }
}
