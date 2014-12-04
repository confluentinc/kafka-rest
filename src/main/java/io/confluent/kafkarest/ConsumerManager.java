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

import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.resources.TopicsResource;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;

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

    private final Config config;
    private final Time time;
    private final String zookeeperConnect;
    private final MetadataObserver mdObserver;
    private final int iteratorTimeoutMs;

    private final AtomicInteger nextId = new AtomicInteger(0);
    private final Map<ConsumerInstanceId,ConsumerState> consumers = new HashMap<>();
    private final ExecutorService executor;
    private ConsumerFactory consumerFactory;
    private final PriorityQueue<ConsumerState> consumersByExpiration = new PriorityQueue<>();
    private final ExpirationThread expirationThread;

    public ConsumerManager(Config config, MetadataObserver mdObserver) {
        this.config = config;
        this.time = config.time;
        this.zookeeperConnect = config.zookeeperConnect;
        this.mdObserver = mdObserver;
        this.iteratorTimeoutMs = config.consumerIteratorTimeoutMs;
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
     * @param config configuration parameters for the consumer
     * @return Unique consumer instance ID
     */
    public String createConsumer(String group, ConsumerInstanceConfig config) {
        String id = config.getId();
        if (id == null) {
            id = "rest-consumer-";
            if (!this.config.id.isEmpty())
                id += this.config.id + "-";
            id += ((Integer) nextId.incrementAndGet()).toString();
        }

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
            ConsumerState state = new ConsumerState(this.config, cid, consumer);
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
        state.updateExpiration();
        consumersByExpiration.add(state);
        this.notifyAll();
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
