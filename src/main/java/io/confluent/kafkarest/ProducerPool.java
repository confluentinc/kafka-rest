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
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking
 * all required acks for a batch and managing timeouts.
 */
public class ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);
  private KafkaProducer producer;

  public ProducerPool(ZkClient zkClient) {
    Seq<Broker> brokerSeq = ZkUtils.getAllBrokersInCluster(zkClient);
    List<Broker> brokers = JavaConversions.seqAsJavaList(brokerSeq);
    String bootstrapBrokers = "";
    for (int i = 0; i < brokers.size(); i++) {
      bootstrapBrokers += brokers.get(i).connectionString();
      if (i != (brokers.size() - 1)) {
        bootstrapBrokers += ",";
      }
    }
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    this.producer = new KafkaProducer(props);
  }

  public void produce(ProducerRecordProxyCollection records, ProduceRequestCallback callback) {
    ProduceRequest request = new ProduceRequest(records.size(), callback);
    log.trace("Starting produce request " + request.toString());
    for (ProducerRecord record : records) {
      producer.send(record, request);
    }
  }

  public void shutdown() {
    producer.close();
  }

  public interface ProduceRequestCallback {

    public void onCompletion(Map<Integer, Long> partitionOffsets);

    public void onException(Exception e);
  }

  // Container for state associated with one REST-ful produce request, i.e. a batched send
  private static class ProduceRequest implements Callback {

    int numRecords;
    ProduceRequestCallback callback;
    int completed;
    Map<Integer, Long> partitionOffsets;
    Exception firstException;

    public ProduceRequest(int numRecords, ProduceRequestCallback callback) {
      this.numRecords = numRecords;
      this.callback = callback;
      this.completed = 0;
      this.partitionOffsets = new HashMap<Integer, Long>();
      this.firstException = null;
    }

    @Override
    public synchronized void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null) {
        if (firstException == null) {
          firstException = exception;
          log.error("Producer error for request " + this.toString(), exception);
        }
      } else {
        // With a single producer, these should always arrive in order.
        partitionOffsets.put(metadata.partition(), metadata.offset());
      }

      completed += 1;

      if (completed == numRecords) {
        if (firstException != null) {
          this.callback.onException(firstException);
        } else {
          this.callback.onCompletion(partitionOffsets);
        }
      }
    }
  }
}
