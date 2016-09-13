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

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.confluent.kafkarest.AssignedConsumerPool;
import io.confluent.kafkarest.ConsumerFactory;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.SystemTime;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.mock.MockTime;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.RestServerErrorException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AssignedConsumerPoolTest {

  private final int AWAIT_TERMINATION_TIMEOUT = 2000;
  private final int POOL_CALLER_SLEEP_TIME = 50;

  private ConsumerFactory simpleConsumerFactory;
  private Time mockTime;

  public AssignedConsumerPoolTest() throws RestConfigException {
    simpleConsumerFactory = EasyMock.createMock(ConsumerFactory.class);
  }

  @Before
  public void setUp() throws Exception {
    mockTime = new MockTime();
    EasyMock.reset(simpleConsumerFactory);

    EasyMock.expect(simpleConsumerFactory.createConsumer()).andStubAnswer(new IAnswer<Consumer<byte[], byte[]>>() {
      @Override
      public Consumer<byte[], byte[]> answer() throws Throwable {
        Consumer<byte[], byte[]> mockConsumer = new MockConsumer<byte[], byte[]>(
            OffsetResetStrategy.EARLIEST);
        return mockConsumer;
      }
    });

    EasyMock.replay(simpleConsumerFactory);
  }

  @Test
  public void testPoolWhenOneSingleThreadedCaller() throws Exception {

    final int maxPoolSize = 3;
    final int poolTimeout = 1000;
    final int maxPollRecords = 1000;
    final AssignedConsumerPool pool =
        new AssignedConsumerPool(
            maxPoolSize,
            poolTimeout,
            maxPollRecords, new SystemTime(), simpleConsumerFactory);

    // poll with 0 count should not create new consumer within the pool
    AssignedConsumerPool.RecordsFetcher fetcher = pool.getRecordsFetcher(new TopicPartition("topic", 0));
    fetcher.close();
    fetcher.poll(10000, 0);
    assertTrue(pool.size() == 0);

    int currentOffset = 100;
    TopicPartition topicPartition = new TopicPartition("topic", 0);
    for (int i = 0; i < 10; i++) {
      Consumer<byte[], byte[]> consumer = pool.get(topicPartition, currentOffset);
      pool.release(consumer, topicPartition, currentOffset += 100);
    }

    assertTrue(pool.size() == 1);
  }

  private class PoolCaller implements Runnable {

    private AssignedConsumerPool pool;

    public PoolCaller(AssignedConsumerPool pool) {
      this.pool = pool;
    }

    @Override
    public void run() {
      TopicPartition topicPartition = new TopicPartition("topic", 0);
      Consumer<byte[], byte[]> consumer = pool.get(topicPartition, 100);
      try {
        // Waiting to simulate data fetching from kafka
        Thread.sleep(POOL_CALLER_SLEEP_TIME);
        pool.release(consumer, topicPartition, 100);
      } catch (Exception e) {
        fail(e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testPoolWhenMultiThreadedCaller() throws Exception {

    final int maxPoolSize = 3;
    final int poolTimeout = 3000;
    final int maxPollTime = 10;
    final AssignedConsumerPool consumersPool =
        new AssignedConsumerPool(maxPoolSize, poolTimeout, maxPollTime, new SystemTime(), simpleConsumerFactory);

    final ExecutorService executorService = Executors.newFixedThreadPool(100);
    for (int i = 0; i < 100; i++) {
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
    final int maxPollTime = 1000;
    final AssignedConsumerPool consumersPool =
        new AssignedConsumerPool(maxPoolSize, poolTimeout, maxPollTime, new SystemTime(), simpleConsumerFactory);

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
    final int poolTimeout = 1; // And we don't allow to wait a lot to get it
    final int maxPollTime = 1000;
    final AssignedConsumerPool consumersPool =
        new AssignedConsumerPool(maxPoolSize, poolTimeout, maxPollTime, new SystemTime(), simpleConsumerFactory);

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
    final int maxPollTime = 100000;
    final AssignedConsumerPool consumersPool =
        new AssignedConsumerPool(maxPoolSize, poolTimeout, maxPollTime, new SystemTime(), simpleConsumerFactory);

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
