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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleConsumerFactory {

  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerManager.class);

  private final KafkaRestConfig config;

  private final Properties simpleConsumerProperties;
  private final AtomicInteger clientIdCounter;

  public SimpleConsumerFactory(final KafkaRestConfig config) {
    this.config = config;

    clientIdCounter = new AtomicInteger(0);
    simpleConsumerProperties = config.getOriginalProperties();
  }

  // The factory *must* return a SimpleConsumer with a unique clientId, as the clientId is
  // used by the SimpleConsumerPool to uniquely identify the consumer
  private String nextClientId() {

    final StringBuilder id = new StringBuilder();
    id.append("rest-simpleconsumer-");

    final String serverId = this.config.getString(KafkaRestConfig.ID_CONFIG);
    if (!serverId.isEmpty()) {
      id.append(serverId);
      id.append("-");
    }

    id.append(Integer.toString(clientIdCounter.incrementAndGet()));

    return id.toString();
  }

  public ConsumerProvider createConsumer() {

    final String clientId = nextClientId();

    log.debug("Creating SimpleConsumer with id " + clientId);
    Properties properties = (Properties) simpleConsumerProperties.clone();

    properties.setProperty("client.id", clientId);
    properties.setProperty("max.poll.records", String.valueOf(
        config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_CACHE_MAX_RECORDS_CONFIG)));

    Consumer<byte[], byte[]> consumer =  new KafkaConsumer<byte[], byte[]>(properties,
      new ByteArrayDeserializer(),
      new ByteArrayDeserializer());
    return new ConsumerProvider(consumer, clientId);
  }


  public static class ConsumerProvider {
    private Consumer<byte[], byte[]> consumer;
    private String clientId;

    public ConsumerProvider(Consumer<byte[], byte[]> consumer, String clientId) {
      this.consumer = consumer;
      this.clientId = clientId;
    }

    public Consumer<byte[], byte[]> consumer() {
      return consumer;
    }

    public String clientId() {
      return clientId;
    }
  }

}
