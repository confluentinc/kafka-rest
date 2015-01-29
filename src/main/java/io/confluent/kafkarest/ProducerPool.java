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

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking
 * all required acks for a batch and managing timeouts.
 */
public class ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(ProducerPool.class);
  private Map<Versions.EmbeddedFormat,RestProducer> producers =
      new HashMap<Versions.EmbeddedFormat,RestProducer>();

  public ProducerPool(KafkaRestConfig appConfig, ZkClient zkClient) {
    Seq<Broker> brokerSeq = ZkUtils.getAllBrokersInCluster(zkClient);
    List<Broker> brokers = JavaConversions.seqAsJavaList(brokerSeq);
    String bootstrapBrokers = "";
    for (int i = 0; i < brokers.size(); i++) {
      bootstrapBrokers += brokers.get(i).connectionString();
      if (i != (brokers.size() - 1)) {
        bootstrapBrokers += ",";
      }
    }

    {
      Map<String,Object> props = new HashMap<String,Object>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
      ByteArraySerializer keySerializer = new ByteArraySerializer();
      keySerializer.configure(props, true);
      ByteArraySerializer valueSerializer = new ByteArraySerializer();
      keySerializer.configure(props, false);
      KafkaProducer<byte[],byte[]> producer = new KafkaProducer<byte[],byte[]>(props,
                                                                               keySerializer,
                                                                               valueSerializer);
      producers.put(
          Versions.EmbeddedFormat.BINARY,
          new BinaryRestProducer(producer,
              keySerializer, valueSerializer));
    }

    {
      Map<String,Object> props = new HashMap<String,Object>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
      props.put("schema.registry.url",
                appConfig.getString(KafkaRestConfig.SCHEMA_REGISTRY_CONNECT_CONFIG));
      final KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
      avroKeySerializer.configure(props, true);
      final KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
      avroValueSerializer.configure(props, false);
      KafkaProducer producer = new KafkaProducer<Object,Object>(props,
                                                                avroKeySerializer,
                                                                avroValueSerializer);
      producers.put(
          Versions.EmbeddedFormat.AVRO,
          new AvroRestProducer(producer, avroKeySerializer, avroValueSerializer));
    }
  }

  public <K,V> void produce(String topic, Integer partition,
                            Versions.EmbeddedFormat recordFormat,
                            SchemaHolder schemaHolder,
                            Collection<? extends ProduceRecord<K,V>> records,
                            ProduceRequestCallback callback) {
    ProduceTask task = new ProduceTask(schemaHolder, records.size(), callback);
    log.trace("Starting produce task " + task.toString());
    RestProducer restProducer = producers.get(recordFormat);
    restProducer.produce(task, topic, partition, records);
  }

  public void shutdown() {
    for(RestProducer restProducer : producers.values()) {
      restProducer.close();
    }
  }

  public interface ProduceRequestCallback {

    public void onCompletion(Integer keySchemaId, Integer valueSchemaId,
                             Map<Integer, Long> partitionOffsets);

    public void onException(Exception e);
  }
}
