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
package io.confluent.kafkarest.simpleconsumerspool;

import io.confluent.kafkarest.SimpleConsumerFactory;
import io.confluent.kafkarest.SimpleFetcher;
import kafka.cluster.Broker;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * The NaiveSimpleConsumerPool instantiates a new SimpleConsumer each time it is called
 */
public class NaiveSimpleConsumerPool implements SimpleConsumerPool {

  private static final Logger log = LoggerFactory.getLogger(NaiveSimpleConsumerPool.class);

  private final SimpleConsumerFactory simpleConsumerFactory;

  private final Map<String, SimpleConsumer> simpleConsumers;

  public NaiveSimpleConsumerPool(SimpleConsumerFactory simpleConsumerFactory) {
    this.simpleConsumerFactory = simpleConsumerFactory;
    simpleConsumers = new HashMap<String, SimpleConsumer>();
  }

  synchronized public SimpleFetcher get(final String host, final int port) {
    final SimpleConsumer simpleConsumer = simpleConsumerFactory.createConsumer(host, port);
    simpleConsumers.put(simpleConsumer.clientId(), simpleConsumer);
    return new SimpleFetcher(simpleConsumer, this);
  }

  synchronized public void release(SimpleFetcher simpleFetcher) {
    log.debug("Removing SimpleConsumer with id " + simpleFetcher.clientId());
    simpleConsumers.remove(simpleFetcher.clientId()).close();
  }

  public void shutdown() {
    for (SimpleConsumer simpleConsumer : simpleConsumers.values()) {
      simpleConsumer.close();
    }
  }

}
