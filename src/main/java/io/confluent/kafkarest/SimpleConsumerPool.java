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
package io.confluent.kafkarest;

import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * The SizeLimitedSimpleConsumerPool keeps a pool of SimpleConsumers
 * and can increase the pool within a specified limit
 */
public class SimpleConsumerPool {
  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerPool.class);

  // maxPoolSize = 0 means unlimited
  private final int maxPoolSize;

  private final SimpleConsumerFactory simpleConsumerFactory;
  private final Map<String, SimpleConsumer> simpleConsumers;
  private final Queue<String> availableConsumers;

  public SimpleConsumerPool(int maxPoolSize, SimpleConsumerFactory simpleConsumerFactory) {
    this.maxPoolSize = maxPoolSize;
    this.simpleConsumerFactory = simpleConsumerFactory;

    simpleConsumers = new HashMap<String, SimpleConsumer>();
    availableConsumers = new PriorityQueue<String>();
  }

  synchronized public SimpleFetcher get(final String host, final int port) {

    while (true) {
      // If there is a SimpleConsumer available
      if (availableConsumers.size() > 0) {
        final String consumerId = availableConsumers.remove();
        return new SimpleFetcher(simpleConsumers.get(consumerId), this);
      }

      // If not consumer is available, but we can instantiate a new one
      if (simpleConsumers.size() < maxPoolSize || maxPoolSize == 0) {
        final SimpleConsumer simpleConsumer = simpleConsumerFactory.createConsumer(host, port);
        simpleConsumers.put(simpleConsumer.clientId(), simpleConsumer);
        return new SimpleFetcher(simpleConsumer, this);
      }

      // If no consumer is available and we reached the limit
      try {
        wait();
      } catch (InterruptedException e) {
        log.warn("A thread requesting a SimpleConsumer has been interrupted while waiting");
      }
    }

  }

  synchronized public void release(SimpleFetcher simpleFetcher) {
    log.debug("Releasing into the pool SimpleConsumer with id " + simpleFetcher.clientId());
    availableConsumers.add(simpleFetcher.clientId());
    notify();
  }

  public void shutdown() {
    for (SimpleConsumer simpleConsumer : simpleConsumers.values()) {
      simpleConsumer.close();
    }
  }

  public int size() {
    return simpleConsumers.size();
  }
}
