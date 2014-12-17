/**
 * Copyright 2014 Confluent Inc.
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
 */
package io.confluent.kafkarest.mock;

import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.ConsumerRecord;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static org.junit.Assert.*;

public class MockConsumerConnector implements ConsumerConnector {
    public String clientId;
    public Map<String, List<KafkaStream<byte[], byte[]>>> subscribedStreams = new HashMap<String, List<KafkaStream<byte[], byte[]>>>();

    private Time time;
    private Map<String,List<Map<Integer,List<ConsumerRecord>>>> streamDataSchedules;
    private static int consumerTimeoutMs;

    private static Decoder<byte[]> decoder = new DefaultDecoder(null);

    /**
     * MockConsumerConnector lets you give a predetermined schedule for how when data should be delivered to consumers.
     */
    public MockConsumerConnector(Time time, String clientId, Map<String,List<Map<Integer,List<ConsumerRecord>>>> streamDataSchedules,
                                 int consumerTimeoutMs)
    {
        this.time = time;
        this.clientId = clientId;
        this.streamDataSchedules = streamDataSchedules;
        this.consumerTimeoutMs = consumerTimeoutMs;
    }

    public MockConsumerConnector(Time time, String clientId, Map<String,List<Map<Integer,List<ConsumerRecord>>>> streamDataSchedules)
    {
        this(time, clientId, streamDataSchedules, -1);
    }

    @Override
    public <K, V> Map<String, List<KafkaStream<K, V>>> createMessageStreams(Map<String, Integer> stringIntegerMap, Decoder<K> kDecoder, Decoder<V> vDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap) {
        Map<String, List<KafkaStream<byte[], byte[]>>> result = new HashMap<String, List<KafkaStream<byte[], byte[]>>>();

        for(Map.Entry<String,Integer> topicEntry : topicCountMap.entrySet()) {
            String topic = topicEntry.getKey();
            assertFalse("MockConsumerConnector does not support multiple subscription requests to a topic",
                    subscribedStreams.containsKey(topic));
            assertTrue("MockConsumerConnector should have a predetermined schedule for requested streams",
                    streamDataSchedules.containsKey(topic));
            assertTrue("Calls to MockConsumerConnector.createMessageStreams should request the same number of streams as provided to the constructor",
                    streamDataSchedules.get(topic).size() == topicEntry.getValue());
            List<KafkaStream<byte[],byte[]>> streams = new Vector<KafkaStream<byte[],byte[]>>();
            for(int i = 0; i < topicEntry.getValue(); i++) {
                streams.add(new KafkaStream<byte[],byte[]>(new MockConsumerQueue(time, streamDataSchedules.get(topic).get(i)),
                        consumerTimeoutMs, decoder, decoder, clientId));
            }
            subscribedStreams.put(topic, streams);
            result.put(topic, streams);
        }

        return result;
    }

    @Override
    public <K, V> List<KafkaStream<K, V>> createMessageStreamsByFilter(TopicFilter topicFilter, int i, Decoder<K> kDecoder, Decoder<V> vDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter, int i) {
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
    public void shutdown() {

    }
}
