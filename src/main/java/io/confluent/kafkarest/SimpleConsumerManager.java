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

import io.confluent.kafkarest.entities.*;

import io.confluent.kafkarest.simpleconsumerspool.NaiveSimpleConsumerPool;
import io.confluent.kafkarest.simpleconsumerspool.SimpleConsumerPool;
import io.confluent.kafkarest.simpleconsumerspool.SizeLimitedSimpleConsumerPool;
import io.confluent.rest.exceptions.RestException;
import kafka.api.PartitionFetchInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleConsumerManager {

  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerManager.class);

  private final KafkaRestConfig config;
  private final MetadataObserver mdObserver;
  private final SimpleConsumerFactory simpleConsumerFactory;

  private final Map<Broker, SimpleConsumerPool> simpleConsumersPools;

  private AtomicInteger correlationId = new AtomicInteger(0);

  public SimpleConsumerManager(final KafkaRestConfig config,
                               final MetadataObserver mdObserver,
                               final SimpleConsumerFactory simpleConsumerFactory) {

    this.config = config;
    this.mdObserver = mdObserver;
    this.simpleConsumerFactory = simpleConsumerFactory;

    simpleConsumersPools = new HashMap<Broker, SimpleConsumerPool>();
  }

  private SimpleConsumerPool createSimpleConsumerPool() {
    // TODO Param type of pool
    return new NaiveSimpleConsumerPool(simpleConsumerFactory);
  }

  private SimpleFetcher getSimpleFetcher(final Broker broker) {
    if (!simpleConsumersPools.containsKey(broker)) {
      simpleConsumersPools.put(broker, createSimpleConsumerPool());
    }
    final SimpleConsumerPool pool = simpleConsumersPools.get(broker);
    return pool.get(broker.host(), broker.port());
  }

  public void consume(final String topicName,
                      final int partitionId,
                      long offset,
                      long count,
                      final EmbeddedFormat embeddedFormat,
                      final ConsumerManager.ReadCallback callback) {

    List<ConsumerRecord> records = null;
    Exception exception = null;
    SimpleFetcher simpleFetcher = null;

    try {
      final Broker broker = mdObserver.getLeader(topicName, partitionId);
      simpleFetcher = getSimpleFetcher(broker);

      records = new ArrayList<ConsumerRecord>();

      int fetchIterations = 0;
      while (count > 0) {
        fetchIterations++;
        log.debug("Simple consumer " + simpleFetcher.clientId() +
            ": fetch " + fetchIterations + "; " + count + " messages remaining");

        final ByteBufferMessageSet messageAndOffsets =
            fetchRecords(topicName, partitionId, offset, simpleFetcher);

        for (final MessageAndOffset messageAndOffset : messageAndOffsets) {
          records.add(embeddedFormat.createConsumerRecord(messageAndOffset, partitionId));
          count--;
          offset++;

          // If all requested messages have already been fetched, we can break early
          if (count == 0) {
            break;
          }
        }
      }

    } catch (RestException e) {
      exception = e;
    } finally {

      // When the project migrates to java 1.7, the finally can be replaced with a try-with-resource
      if (simpleFetcher != null) {
        try {
          simpleFetcher.close();
        } catch (Exception e) {
          log.error("Unable to release SimpleConsumer " + simpleFetcher.clientId() + " into the pool", e);
        }
      }
    }

    callback.onCompletion(records, exception);
  }

  private ByteBufferMessageSet fetchRecords(final String topicName,
                                            final int partitionId,
                                            final long offset,
                                            final SimpleFetcher simpleFetcher) {

    final ConsumerConfig consumerConfig = simpleConsumerFactory.getConsumerConfig();

    final Map<TopicAndPartition, PartitionFetchInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionFetchInfo>();

    requestInfo.put(new TopicAndPartition(topicName, partitionId),
        new PartitionFetchInfo(offset, consumerConfig.fetchMessageMaxBytes()));

    final int corId = correlationId.incrementAndGet();

    final FetchRequest req =
        new FetchRequest(
            corId,
            simpleFetcher.clientId(),
            consumerConfig.socketTimeoutMs(),
            consumerConfig.socketReceiveBufferBytes(),
            requestInfo);

    final FetchResponse fetchResponse = simpleFetcher.fetch(req);

    if (fetchResponse.hasError()) {
      final short kafkaErrorCode = fetchResponse.errorCode(topicName, partitionId);
      throw Errors.kafkaErrorException(new Exception("Fetch response contains an error code: " + kafkaErrorCode));
    }

    return fetchResponse.messageSet(topicName, partitionId);
  }

  public void shutdown() {
    for (SimpleConsumerPool pool : simpleConsumersPools.values()) {
      pool.shutdown();
    }
  }

}
