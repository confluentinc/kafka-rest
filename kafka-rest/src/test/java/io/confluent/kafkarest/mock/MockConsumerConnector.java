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
package io.confluent.kafkarest.mock;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.ConsumerRecord;
import kafka.common.MessageStreamsExistException;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.ConsumerRebalanceListener;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MockConsumerConnector implements ConsumerConnector {

  public String clientId;
  public Set<String> subscribedTopics = new HashSet<String>();

  private Time time;
  private Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>> streamDataSchedules;
  private static int consumerTimeoutMs;
  private boolean allowMissingSchedule;
  private boolean messageStreamCreated;
  private static Decoder<byte[]> decoder = new DefaultDecoder(null);

  /**
   * MockConsumerConnector lets you give a predetermined schedule for how when data should be
   * delivered to consumers.
   */
  public MockConsumerConnector(
      Time time, String clientId,
      Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>> streamDataSchedules,
      int consumerTimeoutMs,
      boolean allowMissingSchedule) {
    this.time = time;
    this.clientId = clientId;
    this.streamDataSchedules = streamDataSchedules;
    this.consumerTimeoutMs = consumerTimeoutMs;
    this.allowMissingSchedule = allowMissingSchedule;
    this.messageStreamCreated = false;
  }

  @Override
  public <K, V> Map<String, List<KafkaStream<K, V>>> createMessageStreams(
      Map<String, Integer> topicCountMap, Decoder<K> kDecoder, Decoder<V> vDecoder) {
    assert (kDecoder instanceof DefaultDecoder);
    assert (vDecoder instanceof DefaultDecoder);

    if (messageStreamCreated) {
      throw new MessageStreamsExistException("Subscribed twice", null);
    }

    Map<String, List<KafkaStream<K, V>>>
        result =
        new HashMap<String, List<KafkaStream<K, V>>>();

    for (Map.Entry<String, Integer> topicEntry : topicCountMap.entrySet()) {
      String topic = topicEntry.getKey();
      assertFalse(
          "MockConsumerConnector does not support multiple subscription requests to a topic",
          subscribedTopics.contains(topic));
      if (!allowMissingSchedule) {
        assertTrue(
            "MockConsumerConnector should have a predetermined schedule for requested streams",
            streamDataSchedules.containsKey(topic));
        assertTrue(
            "Calls to MockConsumerConnector.createMessageStreams should request the same number "
            + "of streams as provided to the constructor",
            streamDataSchedules.get(topic).size() == topicEntry.getValue());
      }
      if (streamDataSchedules.get(topic) != null) {
        List<KafkaStream<K, V>> streams = new Vector<KafkaStream<K, V>>();
        for (int i = 0; i < topicEntry.getValue(); i++) {
          streams.add(new KafkaStream<K, V>(
              new MockConsumerQueue(time, streamDataSchedules.get(topic).get(i)),
              consumerTimeoutMs, kDecoder, vDecoder, clientId));
        }
        subscribedTopics.add(topic);
        result.put(topic, streams);
      }
    }

    messageStreamCreated = true;
    return result;
  }

  @Override
  public Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(
      Map<String, Integer> topicCountMap) {
    return createMessageStreams(topicCountMap,
                                new DefaultDecoder(new VerifiableProperties()),
                                new DefaultDecoder(new VerifiableProperties()));
  }

  @Override
  public <K, V> List<KafkaStream<K, V>> createMessageStreamsByFilter(TopicFilter topicFilter, int i,
                                                                     Decoder<K> kDecoder,
                                                                     Decoder<V> vDecoder) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter,
                                                                        int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitOffsets() {
    return;
  }

  @Override
  public void commitOffsets(boolean b) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitOffsets(Map<TopicAndPartition, OffsetAndMetadata> offsetsToCommit,
                            boolean retryOnFailure) {

  }

  @Override
  public void setConsumerRebalanceListener(ConsumerRebalanceListener listener) {

  }

  @Override
  public void shutdown() {

  }
}
