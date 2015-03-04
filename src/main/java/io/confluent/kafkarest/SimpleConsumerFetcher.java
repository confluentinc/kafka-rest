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

import io.confluent.rest.exceptions.RestException;
import kafka.api.PartitionFetchInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleConsumerFetcher {

  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerFetcher.class);

  private final KafkaRestConfig config;
  private final MetadataObserver mdObserver;
  private final ZkClient zkClient;
  private final ConsumerConfig consumerConfig;

  private AtomicInteger clientIdCounter = new AtomicInteger(0);
  private AtomicInteger correlationId = new AtomicInteger(0);

  public SimpleConsumerFetcher(KafkaRestConfig config, MetadataObserver mdObserver, ZkClient zkClient) {
    this.config = config;
    this.mdObserver = mdObserver;
    this.zkClient = zkClient;

    final Properties props = (Properties) config.getOriginalProperties().clone();
    // ConsumerConfig is intended to be used with the HighLevelConsumer. Therefore, it requires some properties
    // to be instantiated that are useless for a SimpleConsumer.
    // We use ConsumerConfig for our SimpleConsumer, because it contains sensible defaults (buffer size, ...).
    // Maybe, it would make more sense to directly define these defaults here or in a SimpleConsumerConfig ?
    props.setProperty("zookeeper.connect", "");
    props.setProperty("group.id", "");
    consumerConfig = new ConsumerConfig(props);
  }

  private int leaderForPartition(final String topicName, final int partitionId) {
    final List<Partition> partitions = mdObserver.getTopicPartitions(topicName);
    for (final Partition partition : partitions) {
      if (partition.getPartition() == partitionId) {
        return partition.getLeader();
      }
    }

    throw Errors.partitionNotFoundException();
  }

  private Broker brokerInfo(final int brokerId) {
    final Seq<Broker> brokers = ZkUtils.getAllBrokersInCluster(zkClient);

    for (Broker broker : JavaConversions.asJavaCollection(brokers)) {
      if (broker.id() == brokerId) {
        return broker;
      }
    }

    throw Errors.brokerDataNotFoundException();
  }

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

  public void consume(final String topicName,
                      final int partitionId,
                      long offset,
                      long count,
                      final EmbeddedFormat embeddedFormat,
                      final ConsumerManager.ReadCallback callback) {

    List<ConsumerRecord> records = null;
    Exception exception = null;

    try {
      final int leader = leaderForPartition(topicName, partitionId);
      final Broker broker = brokerInfo(leader);

      final String clientId = nextClientId();

      log.debug("Creating simple consumer " + clientId);
      final SimpleConsumer simpleConsumer =
          new SimpleConsumer(broker.host(), broker.port(),
              consumerConfig.socketReceiveBufferBytes(), consumerConfig.socketReceiveBufferBytes(), clientId);

      records = new ArrayList<ConsumerRecord>();

      int fetchIterations = 0;
      while (count > 0) {
        fetchIterations++;
        log.debug("Simple consumer " + clientId + ": fetch " + fetchIterations + "; " + count + " messages remaining");

        final ByteBufferMessageSet messageAndOffsets =
            fetchRecords(topicName, partitionId, offset, simpleConsumer);

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
    }

    callback.onCompletion(records, exception);
  }

  private ByteBufferMessageSet fetchRecords(final String topicName,
                                            final int partitionId,
                                            final long offset,
                                            final SimpleConsumer simpleConsumer) {

    final Map<TopicAndPartition, PartitionFetchInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionFetchInfo>();

    requestInfo.put(new TopicAndPartition(topicName, partitionId),
        new PartitionFetchInfo(offset, consumerConfig.fetchMessageMaxBytes()));

    final int corId = correlationId.incrementAndGet();

    final FetchRequest req =
        new FetchRequest(
            corId,
            simpleConsumer.clientId(),
            consumerConfig.socketTimeoutMs(),
            consumerConfig.socketReceiveBufferBytes(),
            requestInfo);

    final FetchResponse fetchResponse = simpleConsumer.fetch(req);

    if (fetchResponse.hasError()) {
      final short kafkaErrorCode = fetchResponse.errorCode(topicName, partitionId);
      throw Errors.kafkaErrorException(new Exception("Fetch response contains an error code: " + kafkaErrorCode));
    }

    return fetchResponse.messageSet(topicName, partitionId);
  }

}
