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

import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.resources.TopicsResource;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import javax.ws.rs.NotFoundException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages consumer instances by mapping instance IDs to consumer objects, processing read requests, and cleaning
 * up when consumers disappear.
 */
public class ConsumerManager {
    public final static String MESSAGE_CONSUMER_INSTANCE_NOT_FOUND = "Consumer instance not found.";

    private final Time time;
    private final String zookeeperConnect;
    private final MetadataObserver mdObserver;
    private final int iteratorTimeoutMs;
    private final int requestTimeoutMs;
    private final int requestMaxMessages;
    private final int consumerInstanceTimeoutMs;

    private final AtomicInteger nextId = new AtomicInteger(0);
    private final Map<ConsumerInstanceId,ConsumerState> consumers = new HashMap<>();
    private final ExecutorService executor;
    private ConsumerFactory consumerFactory;
    private final PriorityQueue<ConsumerState> consumersByExpiration = new PriorityQueue<>();
    private final ExpirationThread expirationThread;

    public ConsumerManager(Config config, MetadataObserver mdObserver) {
        this.time = config.time;
        this.zookeeperConnect = config.zookeeperConnect;
        this.mdObserver = mdObserver;
        this.iteratorTimeoutMs = config.consumerIteratorTimeoutMs;
        this.requestTimeoutMs = config.consumerRequestTimeoutMs;
        this.requestMaxMessages = config.consumerRequestMaxMessages;
        this.consumerInstanceTimeoutMs = config.consumerInstanceTimeoutMs;
        this.executor = Executors.newFixedThreadPool(config.consumerThreads);
        this.consumerFactory = null;
        this.expirationThread = new ExpirationThread();
        this.expirationThread.start();
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
        props.put("auto.commit.enable", "false");
        ConsumerConnector consumer;
        if (consumerFactory == null)
            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        else
            consumer = consumerFactory.createConsumer(new ConsumerConfig(props));

        synchronized (this) {
            ConsumerInstanceId cid = new ConsumerInstanceId(group, id);
            ConsumerState state = new ConsumerState(cid, consumer, time.milliseconds() + consumerInstanceTimeoutMs);
            consumers.put(cid, state);
            consumersByExpiration.add(state);
            this.notifyAll();
        }

        return id;
    }

    public interface ReadCallback {
        public void onCompletion(List<ConsumerRecord> records, Exception e);
    }

    public Future readTopic(final String group, final String instance, final String topic, final ReadCallback callback) {
        final ConsumerState state;
        try {
            state = getConsumerInstance(group, instance);
        } catch (NotFoundException e) {
            callback.onCompletion(null, e);
            return null;
        }

        // Consumer will try reading even if it doesn't exist, so we need to check this explicitly.
        if (!mdObserver.topicExists(topic)) {
            callback.onCompletion(null, new NotFoundException(TopicsResource.MESSAGE_TOPIC_NOT_FOUND));
            return null;
        }

        return executor.submit(new Runnable() {
            @Override
            public void run() {
                List<ConsumerRecord> result = state.readTopic(topic);
                updateExpiration(state);
                if (result == null)
                    callback.onCompletion(null, new NotFoundException(MESSAGE_CONSUMER_INSTANCE_NOT_FOUND));
                else
                    callback.onCompletion(result, null);
            }
        });
    }

    public interface CommitCallback {
        public void onCompletion(List<TopicPartitionOffset> offsets, Exception e);
    }

    public Future commitOffsets(String group, String instance, final CommitCallback callback) {
        final ConsumerState state;
        try {
            state = getConsumerInstance(group, instance);
        } catch (NotFoundException e) {
            callback.onCompletion(null, e);
            return null;
        }

        return executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    List<TopicPartitionOffset> offsets = state.commitOffsets();
                    callback.onCompletion(offsets, null);
                } catch (Exception e) {
                    e.printStackTrace();
                    callback.onCompletion(null, e);
                } finally {
                    updateExpiration(state);
                }
            }
        });
    }

    public void deleteConsumer(String group, String instance) {
        final ConsumerState state = getConsumerInstance(group, instance, true);
        state.close();
    }

    /**
     * Gets the specified consumer instance or throws a not found exception. Also removes the consumer's expiration timeout
     * so it is not cleaned up mid-operation.
     */
    private synchronized ConsumerState getConsumerInstance(String group, String instance, boolean remove) {
        ConsumerInstanceId id = new ConsumerInstanceId(group, instance);
        final ConsumerState state = remove ? consumers.remove(id) : consumers.get(id);
        if (state == null)
            throw new NotFoundException(MESSAGE_CONSUMER_INSTANCE_NOT_FOUND);
        // Clear from the timeout queue immediately so it isn't removed during the read operation, but don't update
        // the timeout until we finish the read since that can significantly affect the timeout.
        consumersByExpiration.remove(state);
        return state;
    }

    private ConsumerState getConsumerInstance(String group, String instance) {
        return getConsumerInstance(group, instance, false);
    }

    private synchronized void updateExpiration(ConsumerState state) {
        state.updateExpiration(time.milliseconds() + consumerInstanceTimeoutMs);
        consumersByExpiration.add(state);
        this.notifyAll();
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

    private class ConsumerState implements Comparable<ConsumerState> {
        private ConsumerInstanceId instanceId;
        private ConsumerConnector consumer;
        private Map<String, TopicState> topics;
        private long expiration;

        private ConsumerState(ConsumerInstanceId instanceId, ConsumerConnector consumer, long expiration) {
            this.instanceId = instanceId;
            this.consumer = consumer;
            this.topics = new HashMap<>();
            this.expiration = expiration;
        }

        public ConsumerInstanceId getId() {
            return instanceId;
        }

        public synchronized List<ConsumerRecord> readTopic(String topic) {
            TopicState topicState = getOrCreateTopicState(topic);
            if (topicState == null)
                return null;
            synchronized(topicState) {
                ConsumerIterator<byte[], byte[]> iter = topicState.stream.iterator();
                List<ConsumerRecord> messages = new Vector<>();
                final long started = time.milliseconds();
                long elapsed = 0;
                while (elapsed < requestTimeoutMs && messages.size() < requestMaxMessages) {
                    try {
                        if (!iter.hasNext())
                            break;
                        MessageAndMetadata<byte[], byte[]> msg = iter.next();
                        messages.add(new ConsumerRecord(msg.key(), msg.message(), msg.partition(), msg.offset()));
                        topicState.consumedOffsets.put(msg.partition(), msg.offset());
                    } catch (ConsumerTimeoutException cte) {
                        // Ignore since we may get a few of these while still under our time limit. The while condition
                        // ensures correct behavior
                    }
                    elapsed = time.milliseconds() - started;
                }
                return messages;
            }
        }

        public List<TopicPartitionOffset> commitOffsets() {
            List<TopicPartitionOffset> result = getOffsets(true);
            consumer.commitOffsets();
            setCommittedOffsets(result);
            return result;
        }

        public void close() {
            synchronized(this) {
                consumer.shutdown();
                // Marks this state entry as no longer valid because the consumer group is being destroyed.
                consumer = null;
                topics = null;
            }
        }

        public boolean expired(long nowMs) {
            return expiration <= nowMs;
        }

        public void updateExpiration(long expiration) {
            this.expiration = expiration;
        }

        public long untilExpiration(long nowMs) {
            return this.expiration - nowMs;
        }

        @Override
        public int compareTo(ConsumerState o) {
            if (this.expiration < o.expiration)
                return -1;
            else if (this.expiration == o.expiration)
                return 0;
            else
                return 1;
        }

        private synchronized TopicState getOrCreateTopicState(String topic) {
            if (topics == null) return null;
            TopicState state = topics.get(topic);
            if (state == null) {
                Map<String,Integer> subscriptions = new TreeMap<>();
                subscriptions.put(topic,1);
                Map<String, List<KafkaStream<byte[],byte[]>>> streamsByTopic = consumer.createMessageStreams(subscriptions);
                KafkaStream<byte[],byte[]> stream = streamsByTopic.get(topic).get(0);
                state = new TopicState(stream);
                topics.put(topic, state);
            }
            return state;
        }

        /**
         * Gets a list of TopicPartitionOffsets describing the current state of consumer offsets.
         * @param precommit if true, sets the committed offset to the consumed offset, which is useful for generating the
         *                  result.
         * @return
         */
        private synchronized List<TopicPartitionOffset> getOffsets(boolean precommit) {
            List<TopicPartitionOffset> result = new Vector<>();
            for(Map.Entry<String, TopicState> entry : topics.entrySet()) {
                TopicState state = entry.getValue();
                synchronized(entry.getValue()) {
                    for(Map.Entry<Integer,Long> partEntry : state.consumedOffsets.entrySet()) {
                        Long committed = precommit ? partEntry.getValue() : state.committedOffsets.get(partEntry.getKey());
                        result.add(new TopicPartitionOffset(entry.getKey(), partEntry.getKey(),
                                partEntry.getValue(), (committed == null ? 0 : committed)));
                    }
                }
            }
            return result;
        }

        private synchronized void setCommittedOffsets(List<TopicPartitionOffset> offsets) {
            for(TopicPartitionOffset tpo : offsets) {
                TopicState state = topics.get(tpo.getTopic());
                if (state == null)
                    continue;
                state.committedOffsets.put(tpo.getPartition(), tpo.getCommitted());
            }
        }

        private class TopicState {
            KafkaStream<byte[],byte[]> stream;
            Map<Integer, Long> consumedOffsets;
            Map<Integer, Long> committedOffsets;

            public TopicState(KafkaStream<byte[],byte[]> stream) {
                this.stream = stream;
                this.consumedOffsets = new HashMap<>();
                this.committedOffsets = new HashMap<>();
            }
        }
    }

    public interface ConsumerFactory {
        ConsumerConnector createConsumer(ConsumerConfig config);
    }

    private class ExpirationThread extends Thread {
        public ExpirationThread() {
            super("Consumer Expiration Thread");
            setDaemon(true);
        }

        @Override
        public void run() {
            synchronized(ConsumerManager.this) {
                try {
                    // FIXME Currently ConsumerManager doesn't catch any shutdown signal, so this continues forever.
                    while (true) {
                        long now = time.milliseconds();
                        while (!consumersByExpiration.isEmpty() && consumersByExpiration.peek().expired(now)) {
                            final ConsumerState state = consumersByExpiration.remove();
                            consumers.remove(state.getId());
                            executor.submit(new Runnable() {
                                @Override
                                public void run() {
                                    state.close();
                                }
                            });
                        }
                        long timeout = (consumersByExpiration.isEmpty() ? Long.MAX_VALUE : consumersByExpiration.peek().untilExpiration(now));
                        ConsumerManager.this.wait(timeout);
                    }
                }
                catch (InterruptedException e) {
                    // Interrupted by other thread, do nothing to allow this thread to exit
                }
            }
        }
    }
}
