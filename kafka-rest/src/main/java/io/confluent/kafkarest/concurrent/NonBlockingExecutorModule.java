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

package io.confluent.kafkarest.concurrent;

import io.confluent.kafkarest.config.ConfigModule.NonBlockingExecutorCorePoolSizeConfig;
import io.confluent.kafkarest.config.ConfigModule.NonBlockingExecutorKeepAliveConfig;
import io.confluent.kafkarest.config.ConfigModule.NonBlockingExecutorMaxPoolSizeConfig;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.glassfish.hk2.api.AnnotationLiteral;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

public class NonBlockingExecutorModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindFactory(ExecutorServiceFactory.class)
        .in(Singleton.class)
        .qualifiedBy(new NonBlockingExecutorImpl())
        .to(ExecutorService.class);
  }

  private static final class ExecutorServiceFactory implements Factory<ExecutorService> {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final Duration keepAlive;

    @Inject
    private ExecutorServiceFactory(
        @NonBlockingExecutorCorePoolSizeConfig int corePoolSize,
        @NonBlockingExecutorMaxPoolSizeConfig int maxPoolSize,
        @NonBlockingExecutorKeepAliveConfig Duration keepAlive) {
      this.corePoolSize = corePoolSize;
      this.maxPoolSize = maxPoolSize;
      this.keepAlive = Objects.requireNonNull(keepAlive);
    }

    @Override
    public ExecutorService provide() {
      return new ThreadPoolExecutor(
          /* corePoolSize= */ corePoolSize,
          /* maximumPoolSize= */ maxPoolSize,
          /* keepAliveTime= */ keepAlive.toMillis(),
          TimeUnit.MILLISECONDS,
          new LinkedBlockingDeque<>());
    }

    @Override
    public void dispose(ExecutorService executorService) {
      executorService.shutdown();
    }
  }

  private static final class NonBlockingExecutorImpl
      extends AnnotationLiteral<NonBlockingExecutor> implements NonBlockingExecutor {
  }
}
