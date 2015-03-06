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

import io.confluent.kafkarest.SimpleConsumerFactory;
import io.confluent.kafkarest.SimpleFetcher;
import io.confluent.kafkarest.simpleconsumerspool.SimpleConsumerPool;
import io.confluent.kafkarest.simpleconsumerspool.SizeLimitedSimpleConsumerPool;
import io.confluent.rest.RestConfigException;
import kafka.javaapi.consumer.SimpleConsumer;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class SimpleConsumerPoolTest {

  private SimpleConsumerFactory simpleConsumerFactory;

  public SimpleConsumerPoolTest() throws RestConfigException {
    simpleConsumerFactory = EasyMock.createMock(SimpleConsumerFactory.class);
  }

  @Before
  public void setUp() throws Exception {
    EasyMock.reset(simpleConsumerFactory);

    EasyMock.expect(simpleConsumerFactory.createConsumer("", 0)).andStubAnswer(new IAnswer<SimpleConsumer>() {

      private AtomicInteger clientIdCounter = new AtomicInteger(0);

      @Override
      public SimpleConsumer answer() throws Throwable {
        final SimpleConsumer simpleConsumer = EasyMock.createMockBuilder(SimpleConsumer.class)
            .addMockedMethod("clientId").createMock();
        EasyMock.expect(simpleConsumer.clientId()).andReturn("clientid-"+clientIdCounter.getAndIncrement()).anyTimes();
        EasyMock.replay(simpleConsumer);
        return simpleConsumer;
      }
    });

    EasyMock.replay(simpleConsumerFactory);
  }

  @Test
  public void testPoolWhenOneSingleThreadedCaller() throws Exception {

    final int maxPoolSize = 3;
    final SizeLimitedSimpleConsumerPool pool = new SizeLimitedSimpleConsumerPool(maxPoolSize, simpleConsumerFactory);

    for (int i = 0; i < 10; i++) {
      SimpleFetcher fetcher = pool.get("", 0);
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
      SimpleFetcher fetcher = pool.get("", 0);
      synchronized(this) {
        try {
          // Waiting to simulate data fetching from kafka
          wait(50);
          fetcher.close();
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void testPoolWhenMultiThreadedCaller() throws Exception {

    final int maxPoolSize = 3;
    final SizeLimitedSimpleConsumerPool consumersPool =
        new SizeLimitedSimpleConsumerPool(maxPoolSize, simpleConsumerFactory);

    final ExecutorService executorService = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      executorService.execute(new PoolCaller(consumersPool));
    }
    executorService.shutdown();

    final boolean allThreadTerminated = executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
    assertTrue(allThreadTerminated);
    assertEquals(maxPoolSize, consumersPool.size());
  }


}
