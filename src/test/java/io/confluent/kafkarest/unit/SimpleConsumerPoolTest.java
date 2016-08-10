/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.*;
import io.confluent.kafkarest.mock.MockKafkaConsumer;
import io.confluent.kafkarest.mock.MockTime;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.RestServerErrorException;
import org.apache.kafka.clients.consumer.Consumer;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class SimpleConsumerPoolTest {

  private final int AWAIT_TERMINATION_TIMEOUT = 2000;
  private final int POOL_CALLER_SLEEP_TIME = 50;

  private SimpleConsumerFactory simpleConsumerFactory;
  private Time mockTime;

  public SimpleConsumerPoolTest() throws RestConfigException {
    simpleConsumerFactory = EasyMock.createMock(SimpleConsumerFactory.class);
  }

  @Before
  public void setUp() throws Exception {
    mockTime = new MockTime();
    EasyMock.reset(simpleConsumerFactory);

    EasyMock.expect(simpleConsumerFactory.createConsumer()).andStubAnswer(new IAnswer<SimpleConsumerFactory.ConsumerProvider>() {

      private AtomicInteger clientIdCounter = new AtomicInteger(0);

      @Override
      public SimpleConsumerFactory.ConsumerProvider answer() throws Throwable {
        final SimpleConsumerFactory.ConsumerProvider consumerProvider = EasyMock
          .createMockBuilder(SimpleConsumerFactory.ConsumerProvider.class)
          .addMockedMethod("clientId")
          .addMockedMethod("consumer")
          .createMock();
        Consumer<byte[], byte[]> mockConsumer = new MockKafkaConsumer(null, null, mockTime);
        EasyMock.expect(consumerProvider.clientId()).andReturn("clientid-"+clientIdCounter.getAndIncrement()).anyTimes();
        EasyMock.expect(consumerProvider.consumer()).andReturn(mockConsumer).anyTimes();

        EasyMock.replay(consumerProvider);
        return consumerProvider;
      }
    });

    EasyMock.replay(simpleConsumerFactory);
  }

  @Test
  public void testPoolWhenOneSingleThreadedCaller() throws Exception {

    final int maxPoolSize = 3;
    final int poolTimeout = 1000;
    final SimpleConsumerPool pool =
        new SimpleConsumerPool(maxPoolSize, poolTimeout, new SystemTime(), simpleConsumerFactory);

    for (int i = 0; i < 10; i++) {
      TPConsumerState fetcher = pool.get("topic", 0);
      fetcher.close();
    }

    assertTrue(pool.size() == 1);
  }

  private class PoolCaller implements Runnable {

    private SimpleConsumerPool pool;

    public PoolCaller(SimpleConsumerPool pool) {
      this.pool = pool;
    }

    @Override
    public void run() {
      TPConsumerState fetcher = pool.get("topic", 0);
      try {
        // Waiting to simulate data fetching from kafka
        Thread.sleep(POOL_CALLER_SLEEP_TIME);
        fetcher.close();
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void testPoolWhenMultiThreadedCaller() throws Exception {

    final int maxPoolSize = 3;
    final int poolTimeout = 1000;
    final SimpleConsumerPool consumersPool =
        new SimpleConsumerPool(maxPoolSize, poolTimeout, new SystemTime(), simpleConsumerFactory);

    final ExecutorService executorService = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      executorService.execute(new PoolCaller(consumersPool));
    }
    executorService.shutdown();

    final boolean allThreadTerminated =
        executorService.awaitTermination(AWAIT_TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS);
    assertTrue(allThreadTerminated);
    assertEquals(maxPoolSize, consumersPool.size());
  }

  @Test
  public void testUnlimitedPoolWhenMultiThreadedCaller() throws Exception {

    final int maxPoolSize = 0; // 0 meaning unlimited
    final int poolTimeout = 1000;
    final SimpleConsumerPool consumersPool =
        new SimpleConsumerPool(maxPoolSize, poolTimeout, new SystemTime(), simpleConsumerFactory);

    final ExecutorService executorService = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      executorService.execute(new PoolCaller(consumersPool));
    }
    executorService.shutdown();

    final boolean allThreadTerminated =
        executorService.awaitTermination(AWAIT_TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS);
    assertTrue(allThreadTerminated);
    // Most of time, the size of the consumers pool will be 10 in the end, but we don't have any guarantee on that,
    // so we limit the test to checking the pool is not empty
    assertTrue(consumersPool.size() > 0);
  }

  @Test
  public void testPoolTimeoutError() throws Exception {

    final int maxPoolSize = 1; // Only one SimpleConsumer instance
    final int poolTimeout = 1; // And we don't allow allow to wait a lot to get it
    final SimpleConsumerPool consumersPool =
        new SimpleConsumerPool(maxPoolSize, poolTimeout, new SystemTime(), simpleConsumerFactory);

    final ExecutorService executorService = Executors.newFixedThreadPool(10);
    final ArrayList<Future<?>> futures = new ArrayList<Future<?>>();
    for (int i = 0; i < 10; i++) {
      futures.add(executorService.submit(new PoolCaller(consumersPool)));
    }
    executorService.shutdown();

    final boolean allThreadTerminated =
        executorService.awaitTermination(AWAIT_TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS);
    assertTrue(allThreadTerminated);

    boolean poolTimeoutErrorEncountered = false;
    try {
      for (Future<?> future : futures) {
        future.get();
      }
    } catch (ExecutionException e) {
      if (((RestServerErrorException)e.getCause()).getErrorCode() == Errors.NO_SIMPLE_CONSUMER_AVAILABLE_ERROR_CODE) {
        poolTimeoutErrorEncountered = true;
      }
    }
    assertTrue(poolTimeoutErrorEncountered);
  }

  @Test
  public void testPoolNoTimeout() throws Exception {

    final int maxPoolSize = 1; // Only one SimpleConsumer instance
    final int poolTimeout = 0; // No timeout. A request will wait as long as needed to get a SimpleConsumer instance
    final SimpleConsumerPool consumersPool =
        new SimpleConsumerPool(maxPoolSize, poolTimeout, new SystemTime(), simpleConsumerFactory);

    final ExecutorService executorService = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      executorService.submit(new PoolCaller(consumersPool));
    }
    executorService.shutdown();

    final boolean allThreadTerminated =
        executorService.awaitTermination(POOL_CALLER_SLEEP_TIME*5, TimeUnit.MILLISECONDS);

    // We simulate 10 concurrent requests taking each POOL_CALLER_SLEEP_TIME using the single SimpleConsumer instance
    // We check that after POOL_CALLER_SLEEP_TIME * 5, there are still requests that are not finished
    assertFalse(allThreadTerminated);
  }

}
