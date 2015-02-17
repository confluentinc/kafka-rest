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

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking
 * all required acks for a batch and managing timeouts. Currently this pool only contains one
 * producer per serialization format (e.g. byte[], Avro).
 */
public class ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(ProducerPool.class);
  private Map<EmbeddedFormat, RestProducer> producers =
      new HashMap<EmbeddedFormat, RestProducer>();

  public ProducerPool(KafkaRestConfig appConfig, ZkClient zkClient) {
    this(appConfig, zkClient, null);
  }

  public ProducerPool(KafkaRestConfig appConfig, ZkClient zkClient,
                      Properties producerConfigOverrides) {
    this(appConfig, getBootstrapBrokers(zkClient), producerConfigOverrides);
  }

  public ProducerPool(KafkaRestConfig appConfig, String bootstrapBrokers,
                      Properties producerConfigOverrides) {

    Properties originalUserProps = appConfig.getOriginalProperties();

    // Note careful ordering: built-in values we look up automatically first, then configs
    // specified by user with initial KafkaRestConfig, and finally explicit overrides passed to
    // this method (only used for tests)
    Map<String, Object> binaryProps = new HashMap<String, Object>();
    binaryProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    for (String propName : originalUserProps.stringPropertyNames()) {
      binaryProps.put(propName, originalUserProps.getProperty(propName));
    }
    if (producerConfigOverrides != null) {
      for (String propName : producerConfigOverrides.stringPropertyNames()) {
        binaryProps.put(propName, producerConfigOverrides.getProperty(propName));
      }
    }
    ByteArraySerializer keySerializer = new ByteArraySerializer();
    keySerializer.configure(binaryProps, true);
    ByteArraySerializer valueSerializer = new ByteArraySerializer();
    keySerializer.configure(binaryProps, false);
    KafkaProducer<byte[], byte[]> byteArrayProducer
        = new KafkaProducer<byte[], byte[]>(binaryProps, keySerializer, valueSerializer);
    producers.put(
        EmbeddedFormat.BINARY,
        new BinaryRestProducer(byteArrayProducer, keySerializer, valueSerializer));

    Map<String, Object> avroProps = new HashMap<String, Object>();
    avroProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    avroProps.put("schema.registry.url",
              appConfig.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG));
    for (String propName : originalUserProps.stringPropertyNames()) {
      avroProps.put(propName, originalUserProps.getProperty(propName));
    }
    if (producerConfigOverrides != null) {
      for (String propName : producerConfigOverrides.stringPropertyNames()) {
        avroProps.put(propName, producerConfigOverrides.getProperty(propName));
      }
    }
    final KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
    avroKeySerializer.configure(avroProps, true);
    final KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
    avroValueSerializer.configure(avroProps, false);
    KafkaProducer<Object, Object> avroProducer
        = new KafkaProducer<Object, Object>(avroProps, avroKeySerializer, avroValueSerializer);
    producers.put(
        EmbeddedFormat.AVRO,
        new AvroRestProducer(avroProducer, avroKeySerializer, avroValueSerializer));
  }

  private static String getBootstrapBrokers(ZkClient zkClient) {
    Seq<Broker> brokerSeq = ZkUtils.getAllBrokersInCluster(zkClient);
    List<Broker> brokers = JavaConversions.seqAsJavaList(brokerSeq);
    String bootstrapBrokers = "";
    for (int i = 0; i < brokers.size(); i++) {
      bootstrapBrokers += brokers.get(i).connectionString();
      if (i != (brokers.size() - 1)) {
        bootstrapBrokers += ",";
      }
    }
    return bootstrapBrokers;
  }

  public <K, V> void produce(String topic, Integer partition,
                             EmbeddedFormat recordFormat,
                             SchemaHolder schemaHolder,
                             Collection<? extends ProduceRecord<K, V>> records,
                             ProduceRequestCallback callback) {
    ProduceTask task = new ProduceTask(schemaHolder, records.size(), callback);
    log.trace("Starting produce task " + task.toString());
    RestProducer restProducer = producers.get(recordFormat);
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
     * @param results list of responses, in the same order as the request. Each entry can be either
     *                a RecordAndMetadata for successful responses or an exception
     */
    public void onCompletion(Integer keySchemaId, Integer valueSchemaId,
                             List<RecordMetadataOrException> results);
  }
}
