/*
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

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.core.Response;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaJsonDecoder;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.JsonConsumerRecord;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestServerErrorException;
import kafka.api.PartitionFetchInfo;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import scala.collection.JavaConversions;

public class SimpleConsumerManager {

  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerManager.class);

  private final int maxPoolSize;
  private final int poolInstanceAvailabilityTimeoutMs;
  private final Time time;

  private final MetadataObserver mdObserver;
  private final SimpleConsumerFactory simpleConsumerFactory;

  private final ConcurrentMap<Broker, SimpleConsumerPool> simpleConsumersPools;

  private AtomicInteger correlationId = new AtomicInteger(0);

  private final Decoder<Object> avroDecoder;
  private final Decoder<byte[]> binaryDecoder;
  private final Decoder<Object> jsonDecoder;

  public SimpleConsumerManager(
      final KafkaRestConfig config,
      final MetadataObserver mdObserver,
      final SimpleConsumerFactory simpleConsumerFactory
  ) {

    this.mdObserver = mdObserver;
    this.simpleConsumerFactory = simpleConsumerFactory;

    maxPoolSize = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_MAX_POOL_SIZE_CONFIG);
    poolInstanceAvailabilityTimeoutMs =
        config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_POOL_TIMEOUT_MS_CONFIG);
    time = config.getTime();

    simpleConsumersPools = new ConcurrentHashMap<Broker, SimpleConsumerPool>();

    // Load decoders
    Properties props = new Properties();
    props.setProperty(
        "schema.registry.url",
        config.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG)
    );
    avroDecoder = new KafkaAvroDecoder(new VerifiableProperties(props));

    binaryDecoder = new DefaultDecoder(new VerifiableProperties());

    jsonDecoder = new KafkaJsonDecoder<Object>(new VerifiableProperties());
  }

  private SimpleConsumerPool createSimpleConsumerPool() {
    return new SimpleConsumerPool(
        maxPoolSize,
        poolInstanceAvailabilityTimeoutMs,
        time,
        simpleConsumerFactory
    );
  }

  private SimpleFetcher getSimpleFetcher(final Broker broker) {
    // When upgrading to Java 1.8, use simpleConsumersPools.computeIfAbsent() instead
    SimpleConsumerPool pool = simpleConsumersPools.get(broker);
    if (pool == null) {
      simpleConsumersPools.putIfAbsent(broker, createSimpleConsumerPool());
      pool = simpleConsumersPools.get(broker);
    }

    // TODO: Add support for SSL when simple consumer is changed to use new consumer
    for (EndPoint ep : JavaConversions.asJavaCollection(broker.endPoints())) {
      if (ep.securityProtocol() == SecurityProtocol.PLAINTEXT) {
        return pool.get(ep.host(), ep.port());
      }
    }
    throw Errors.noSslSupportException();
  }

  public void consume(
      final String topicName,
      final int partitionId,
      long offset,
      long count,
      final EmbeddedFormat embeddedFormat,
      final ConsumerReadCallback callback
  ) {

    List<ConsumerRecord> records = null;
    RestException exception = null;
    SimpleFetcher simpleFetcher = null;

    try {
      final Broker broker = mdObserver.getLeader(topicName, partitionId);
      simpleFetcher = getSimpleFetcher(broker);

      records = new ArrayList<ConsumerRecord>();

      int fetchIterations = 0;
      while (count > 0) {
        fetchIterations++;
        log.debug("Simple consumer " + simpleFetcher.clientId()
                  + ": fetch " + fetchIterations + "; " + count + " messages remaining");

        final ByteBufferMessageSet messageAndOffsets =
            fetchRecords(topicName, partitionId, offset, simpleFetcher);

        // If there is no more messages available, we break early
        if (!messageAndOffsets.iterator().hasNext()) {
          break;
        }

        for (final MessageAndOffset messageAndOffset : messageAndOffsets) {
          records.add(createConsumerRecord(
              messageAndOffset,
              topicName,
              partitionId,
              embeddedFormat
          ));
          count--;
          offset++;

          // If all requested messages have already been fetched, we can break early
          if (count == 0) {
            break;
          }
        }
      }

    } catch (Throwable e) {
      if (e instanceof RestException) {
        exception = (RestException) e;
      } else {
        exception = Errors.kafkaErrorException(e);
      }
    } finally {

      // When the project migrates to java 1.7, the finally can be replaced with a try-with-resource
      if (simpleFetcher != null) {
        try {
          simpleFetcher.close();
        } catch (Exception e) {
          log.error(
              "Unable to release SimpleConsumer {} into the pool",
              simpleFetcher.clientId(),
              e
          );
        }
      }
    }

    callback.onCompletion(records, exception);
  }

  private BinaryConsumerRecord createBinaryConsumerRecord(
      final MessageAndOffset messageAndOffset,
      final String topicName,
      final int partitionId
  ) {
    final MessageAndMetadata<byte[], byte[]> messageAndMetadata =
        new MessageAndMetadata<>(
            topicName,
            partitionId,
            messageAndOffset.message(),
            messageAndOffset.offset(),
            binaryDecoder,
            binaryDecoder,
            0,
            TimestampType.CREATE_TIME
        );
    return new BinaryConsumerRecord(
        topicName,
        messageAndMetadata.key(),
        messageAndMetadata.message(),
        partitionId,
        messageAndOffset.offset()
    );
  }

  private AvroConsumerRecord createAvroConsumerRecord(
      final MessageAndOffset messageAndOffset,
      final String topicName,
      final int partitionId
  ) {
    final MessageAndMetadata<Object, Object> messageAndMetadata =
        new MessageAndMetadata<>(
            topicName,
            partitionId,
            messageAndOffset.message(),
            messageAndOffset.offset(),
            avroDecoder,
            avroDecoder,
            0,
            TimestampType.CREATE_TIME
        );
    return new AvroConsumerRecord(
        topicName,
        AvroConverter.toJson(messageAndMetadata.key()).json,
        AvroConverter.toJson(messageAndMetadata.message()).json,
        partitionId,
        messageAndOffset.offset()
    );
  }


  private JsonConsumerRecord createJsonConsumerRecord(
      final MessageAndOffset messageAndOffset,
      final String topicName,
      final int partitionId
  ) {
    final MessageAndMetadata<Object, Object> messageAndMetadata =
        new MessageAndMetadata<>(
            topicName,
            partitionId,
            messageAndOffset.message(),
            messageAndOffset.offset(),
            jsonDecoder,
            jsonDecoder,
            0,
            TimestampType.CREATE_TIME
        );
    return new JsonConsumerRecord(
        topicName,
        messageAndMetadata.key(),
        messageAndMetadata.message(),
        partitionId,
        messageAndOffset.offset()
    );
  }

  private ConsumerRecord createConsumerRecord(
      final MessageAndOffset messageAndOffset,
      final String topicName,
      final int partitionId,
      final EmbeddedFormat embeddedFormat
  ) {

    switch (embeddedFormat) {
      case BINARY:
        return createBinaryConsumerRecord(messageAndOffset, topicName, partitionId);

      case AVRO:
        return createAvroConsumerRecord(messageAndOffset, topicName, partitionId);

      case JSON:
        return createJsonConsumerRecord(messageAndOffset, topicName, partitionId);

      default:
        throw new RestServerErrorException(
            "Invalid embedded format for new consumer.",
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
        );
    }
  }

  private ByteBufferMessageSet fetchRecords(
      final String topicName,
      final int partitionId,
      final long offset,
      final SimpleFetcher simpleFetcher
  ) {

    final SimpleConsumerConfig
        simpleConsumerConfig =
        simpleConsumerFactory.getSimpleConsumerConfig();

    final Map<TopicAndPartition, PartitionFetchInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionFetchInfo>();

    requestInfo.put(
        new TopicAndPartition(topicName, partitionId),
        new PartitionFetchInfo(offset, simpleConsumerConfig.fetchMessageMaxBytes())
    );

    final int corId = correlationId.incrementAndGet();

    final FetchRequest req =
        new FetchRequest(
            corId,
            simpleFetcher.clientId(),
            simpleConsumerConfig.fetchWaitMaxMs(),
            simpleConsumerConfig.fetchMinBytes(),
            requestInfo
        );

    final FetchResponse fetchResponse = simpleFetcher.fetch(req);

    if (fetchResponse.hasError()) {
      final short kafkaErrorCode = fetchResponse.errorCode(topicName, partitionId);
      throw Errors.kafkaErrorException(new Exception("Fetch response contains an error code: "
                                                     + kafkaErrorCode));
    }

    return fetchResponse.messageSet(topicName, partitionId);
  }

  public void shutdown() {
    for (SimpleConsumerPool pool : simpleConsumersPools.values()) {
      pool.shutdown();
    }
  }

}
