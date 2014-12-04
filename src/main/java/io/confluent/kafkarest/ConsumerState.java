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
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class ConsumerState implements Comparable<ConsumerState> {
    private Config config;
    private ConsumerInstanceId instanceId;
    private ConsumerConnector consumer;
    private Map<String, TopicState> topics;
    private long expiration;
    // A read/write lock on the ConsumerState allows concurrent readTopic calls, but allows commitOffsets to safely
    // lock the entire state in order to get correct information about all the topic/stream's current offset state.
    // All operations on individual TopicStates must be synchronized at that level as well (so, e.g., readTopic may
    // modify a single TopicState, but only needs read access to the ConsumerState).
    private ReadWriteLock lock;

    public ConsumerState(Config config, ConsumerInstanceId instanceId, ConsumerConnector consumer) {
        this.config = config;
        this.instanceId = instanceId;
        this.consumer = consumer;
        this.topics = new HashMap<>();
        this.expiration = config.time.milliseconds() + config.consumerInstanceTimeoutMs;
        this.lock = new ReentrantReadWriteLock();
    }

    public ConsumerInstanceId getId() {
        return instanceId;
    }

    public List<ConsumerRecord> readTopic(String topic) {
        TopicState topicState = getOrCreateTopicState(topic);
        if (topicState == null)
            return null;

        lock.readLock().lock();
        try {
            synchronized (topicState) {
                ConsumerIterator<byte[], byte[]> iter = topicState.stream.iterator();
                List<ConsumerRecord> messages = new Vector<>();
                final long started = config.time.milliseconds();
                long elapsed = 0;
                while (elapsed < config.consumerRequestTimeoutMs && messages.size() < config.consumerRequestMaxMessages) {
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
                    elapsed = config.time.milliseconds() - started;
                }
                return messages;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<TopicPartitionOffset> commitOffsets() {
        lock.writeLock().lock();
        try {
            consumer.commitOffsets();
            List<TopicPartitionOffset> result = getOffsets(true);
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void close() {
        lock.writeLock().lock();
        try {
            consumer.shutdown();
            // Marks this state entry as no longer valid because the consumer group is being destroyed.
            consumer = null;
            topics = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean expired(long nowMs) {
        return expiration <= nowMs;
    }

    public void updateExpiration() {
        this.expiration = config.time.milliseconds() + config.consumerInstanceTimeoutMs;
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

    private TopicState getOrCreateTopicState(String topic) {
        // Try getting the topic only using the read lock
        lock.readLock().lock();
        try {
            if (topics == null) return null;
            TopicState state = topics.get(topic);
            if (state != null) return state;
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            if (topics == null) return null;
            TopicState state = topics.get(topic);
            if (state != null) return state;

            Map<String, Integer> subscriptions = new TreeMap<>();
            subscriptions.put(topic, 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> streamsByTopic = consumer.createMessageStreams(subscriptions);
            KafkaStream<byte[], byte[]> stream = streamsByTopic.get(topic).get(0);
            state = new TopicState(stream);
            topics.put(topic, state);
            return state;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets a list of TopicPartitionOffsets describing the current state of consumer offsets, possibly updating
     * the commmitted offset record. This method is not synchronized.
     * @param updateCommitOffsets if true, updates committed offsets to be the same as the consumed offsets.
     * @return
     */
    private List<TopicPartitionOffset> getOffsets(boolean updateCommitOffsets) {
        List<TopicPartitionOffset> result = new Vector<>();
        for(Map.Entry<String, TopicState> entry : topics.entrySet()) {
            TopicState state = entry.getValue();
            synchronized(state) {
                for(Map.Entry<Integer,Long> partEntry : state.consumedOffsets.entrySet()) {
                    Integer partition = partEntry.getKey();
                    Long offset = partEntry.getValue();
                    Long committedOffset = 0L;
                    if (updateCommitOffsets) {
                        state.committedOffsets.put(partition, offset);
                        committedOffset = offset;
                    } else {
                        committedOffset = state.committedOffsets.get(partition);
                    }
                    result.add(new TopicPartitionOffset(entry.getKey(), partition,
                            offset, (committedOffset == null ? -1 : committedOffset)));
                }
            }
        }
        return result;
    }

    private void setCommittedOffsets(List<TopicPartitionOffset> offsets) {
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