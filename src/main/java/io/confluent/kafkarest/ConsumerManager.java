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
package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.ConsumerRecord;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import javax.ws.rs.NotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages consumer instances by mapping instance IDs to consumer objects, processing read requests, and cleaning
 * up when consumers disappear.
 */
public class ConsumerManager {

    private final Time time;
    private final String zookeeperConnect;
    private final MetadataObserver mdObserver;
    private final int iteratorTimeoutMs;
    private final int requestTimeoutMs;
    private final int requestMaxMessages;

    private final AtomicInteger nextId = new AtomicInteger(0);
    private final Map<ConsumerInstanceId,ConsumerState> consumers = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private ConsumerFactory consumerFactory;

    public ConsumerManager(Config config, MetadataObserver mdObserver) {
        this.time = config.time;
        this.zookeeperConnect = config.zookeeperConnect;
        this.mdObserver = mdObserver;
        this.iteratorTimeoutMs = config.consumerIteratorTimeoutMs;
        this.requestTimeoutMs = config.consumerRequestTimeoutMs;
        this.requestMaxMessages = config.consumerRequestMaxMessages;
        this.executor = Executors.newFixedThreadPool(config.consumerThreads);
        this.consumerFactory = null;
    }

    public ConsumerManager(Config config, MetadataObserver mdObserver, ConsumerFactory consumerFactory) {
        this(config, mdObserver);
        this.consumerFactory = consumerFactory;
    }

    /**
     * Creates a new consumer instance and returns its unique ID.
     * @param group Name of the consumer group to join
     * @return Unique consumer instance ID
     */
    public String createConsumer(String group) {
        String id = ((Integer)nextId.incrementAndGet()).toString();

        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperConnect);
        props.put("group.id", group);
        props.put("consumer.id", id);
        // To support the old consumer interface with broken peek()/missing poll(timeout) functionality, we always use
        // a timeout. This can't perfectly guarantee a total request timeout, but can get as close as this timeout's value
        props.put("consumer.timeout.ms", ((Integer)iteratorTimeoutMs).toString());
        ConsumerConnector consumer;
        if (consumerFactory == null)
            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        else
            consumer = consumerFactory.createConsumer(new ConsumerConfig(props));

        synchronized (this) {
            consumers.put(new ConsumerInstanceId(group, id), new ConsumerState(consumer));
        }

        return id;
    }

    public interface ReadCallback {
        public void onCompletion(List<ConsumerRecord> records, Exception e);
    }

    public Future readTopic(final String group, final String instance, final String topic, final ReadCallback callback) {
        final ConsumerState state = consumers.get(new ConsumerInstanceId(group, instance));
        if (state == null) {
            callback.onCompletion(null, notFound(group, instance));
            return null;
        }

        // Consumer will try reading even if it doesn't exist, so we need to check this explicitly.
        if (!mdObserver.topicExists(topic)) {
            callback.onCompletion(null, new NotFoundException("Topic \"" + topic + "\" not found."));
            return null;
        }

        return executor.submit(new Runnable() {
            @Override
            public void run() {
                List<ConsumerRecord> result = state.readTopic(topic);
                if (result == null)
                    callback.onCompletion(null, notFound(group, instance));
                else
                    callback.onCompletion(result, null);
            }
        });
    }

    public void deleteConsumer(String group, String instance) {
        ConsumerState state = consumers.remove(new ConsumerInstanceId(group, instance));
        if (state == null)
            throw notFound(group, instance);
        state.close();
    }

    private NotFoundException notFound(String group, String instance) {
        return new NotFoundException("Consumer instance \"" + instance + "\" in group \"" + group + "\" does not exist.");
    }

    public class ConsumerInstanceId {
        private final String group;
        private final String instance;

        public ConsumerInstanceId(String group, String instance) {
            this.group = group;
            this.instance = instance;
        }

        public String getGroup() {
            return group;
        }

        public String getInstance() {
            return instance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConsumerInstanceId that = (ConsumerInstanceId) o;

            if (group != null ? !group.equals(that.group) : that.group != null) return false;
            if (instance != null ? !instance.equals(that.instance) : that.instance != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = group != null ? group.hashCode() : 0;
            result = 31 * result + (instance != null ? instance.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ConsumerInstanceId{" +
                    "group='" + group + '\'' +
                    ", instance='" + instance + '\'' +
                    '}';
        }
    }

    private class ConsumerState {
        private ConsumerConnector consumer;
        private Map<String, KafkaStream<byte[],byte[]>> streams;

        private ConsumerState(ConsumerConnector consumer) {
            this.consumer = consumer;
            this.streams = new HashMap<>();
        }

        public List<ConsumerRecord> readTopic(String topic) {
            KafkaStream<byte[],byte[]> stream = getOrCreateStream(topic);
            if (stream == null)
                return null;
            synchronized(stream) {
                ConsumerIterator<byte[], byte[]> iter = stream.iterator();
                List<ConsumerRecord> messages = new Vector<>();
                final long started = time.milliseconds();
                long elapsed = 0;
                while (elapsed < requestTimeoutMs && messages.size() < requestMaxMessages) {
                    try {
                        if (!iter.hasNext())
                            break;
                        MessageAndMetadata<byte[], byte[]> msg = iter.next();
                        messages.add(new ConsumerRecord(msg.key(), msg.message(), msg.partition()));
                    } catch (ConsumerTimeoutException cte) {
                        // Ignore since we may get a few of these while still under our time limit. The while condition
                        // ensures correct behavior
                    }
                    elapsed = time.milliseconds() - started;
                }
                return messages;
            }
        }

        public void close() {
            synchronized(this) {
                consumer.shutdown();
                // Marks this state entry as no longer valid because the consumer group is being destroyed.
                consumer = null;
                streams = null;
            }
        }

        private synchronized KafkaStream<byte[],byte[]> getOrCreateStream(String topic) {
            if (streams == null) return null;
            KafkaStream<byte[],byte[]> stream = streams.get(topic);
            if (stream == null) {
                Map<String,Integer> subscriptions = new TreeMap<>();
                subscriptions.put(topic,1);
                Map<String, List<KafkaStream<byte[],byte[]>>> streamsByTopic = consumer.createMessageStreams(subscriptions);
                stream = streamsByTopic.get(topic).get(0);
                streams.put(topic, stream);
            }
            return stream;
        }
    }

    public interface ConsumerFactory {
        ConsumerConnector createConsumer(ConsumerConfig config);
    }
}
