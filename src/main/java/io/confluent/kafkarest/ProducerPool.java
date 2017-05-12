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

import static io.confluent.kafkarest.ProducerBuilder.buildAvroConfig;
import static io.confluent.kafkarest.ProducerBuilder.buildAvroProducer;
import static io.confluent.kafkarest.ProducerBuilder.buildBinaryProducer;
import static io.confluent.kafkarest.ProducerBuilder.buildJsonProducer;
import static io.confluent.kafkarest.ProducerBuilder.buildStandardConfig;

import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.SchemaHolder;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.SecurityContext;

/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking
 * all required acks for a batch and managing timeouts. Currently this pool only contains one
 * producer per serialization format (e.g. byte[], Avro).
 */
public class ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(ProducerPool.class);
  private Map<EmbeddedFormat, RestProducer> producers = new HashMap<>();
  private final AuthenticatedProducerPool authenticatedProducerPool;

  public ProducerPool(KafkaRestConfig appConfig, ZkUtils zkUtils) {
    this(appConfig, zkUtils, null);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      ZkUtils zkUtils,
      Properties producerConfigOverrides
  ) {
    this(appConfig, getBootstrapBrokers(zkUtils), producerConfigOverrides);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {

    Map<String, Object> binaryProps =
        buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.BINARY, buildBinaryProducer(binaryProps));

    Map<String, Object> jsonProps =
        buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.JSON, buildJsonProducer(jsonProps));

    Map<String, Object> avroProps =
        buildAvroConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.AVRO, buildAvroProducer(avroProps));

    authenticatedProducerPool = new AuthenticatedProducerPool(appConfig,
                                                              bootstrapBrokers,
                                                              producerConfigOverrides);
  }

  private static String getBootstrapBrokers(ZkUtils zkUtils) {
    Seq<Broker> brokerSeq = zkUtils.getAllBrokersInCluster();
    List<Broker> brokers = JavaConversions.seqAsJavaList(brokerSeq);
    StringBuilder bootstrapBrokers = new StringBuilder();
    for (Broker broker : brokers) {
      for (EndPoint ep : JavaConversions.asJavaCollection(broker.endPoints())) {
        if (bootstrapBrokers.length() > 0) {
          bootstrapBrokers.append(",");
        }
        String hostPort =
            ep.host() == null ? ":" + ep.port() : Utils.formatAddress(ep.host(), ep.port());
        bootstrapBrokers.append(ep.securityProtocol()).append("://").append(hostPort);
      }
    }
    return bootstrapBrokers.toString();
  }

  @SuppressWarnings("unchecked")
  public <K, V> void produce(
      String topic,
      Integer partition,
      EmbeddedFormat recordFormat,
      SecurityContext securityContext,
      SchemaHolder schemaHolder,
      Collection<? extends ProduceRecord<K, V>> records,
      ProduceRequestCallback callback
  ) {
    ProduceTask task = new ProduceTask(schemaHolder, records.size(), callback);
    log.trace("Starting produce task " + task.toString());
    JaasModule jaasModule = authenticatedProducerPool.getJaasModule(securityContext);
    RestProducer restProducer;
    if (jaasModule == null) {
      restProducer = producers.get(recordFormat);
    } else {
      restProducer = authenticatedProducerPool.get(recordFormat, jaasModule);
    }
    restProducer.produce(task, topic, partition, records);
  }

  public void shutdown() {
    for (RestProducer restProducer : producers.values()) {
      restProducer.close();
    }
  }

  public interface ProduceRequestCallback {

    /**
     * Invoked when all messages have either been recorded or received an error
     *
     * @param results
     *     list of responses, in the same order as the request. Each entry can be either
     *     a RecordAndMetadata for successful responses or an exception
     */
    void onCompletion(
        Integer keySchemaId,
        Integer valueSchemaId,
        List<RecordMetadataOrException> results
    );
  }

  public static ProducerPool.ProduceRequestCallback defaultProduceRequestCallback(
      final AsyncResponse asyncResponse,
      final Logger log) {
    return new ProducerPool.ProduceRequestCallback() {
      public void onCompletion(Integer keySchemaId, Integer valueSchemaId,
                               List<RecordMetadataOrException> results) {
        ProduceResponse response = new ProduceResponse();
        List<PartitionOffset> offsets = new Vector<>();
        for (RecordMetadataOrException result : results) {
          if (result.getException() != null) {
            int errorCode = Errors.codeFromProducerException(result.getException());
            String errorMessage = result.getException().getMessage();
            offsets.add(new PartitionOffset(null, null, errorCode, errorMessage));
          } else {
            offsets.add(new PartitionOffset(result.getRecordMetadata().partition(),
                                            result.getRecordMetadata().offset(),
                                            null, null));
          }
        }
        response.setOffsets(offsets);
        response.setKeySchemaId(keySchemaId);
        response.setValueSchemaId(valueSchemaId);
        log.trace("Completed topic produce request id={} response={}", asyncResponse, response);
        asyncResponse.resume(response);
      }
    };
  }
}