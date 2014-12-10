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
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker thread for consumers that multiplexes multiple consumer operations onto a single thread.
 */
public class ConsumerWorker extends Thread {
    KafkaRestConfiguration config;

    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    Queue<ReadTask> tasks = new LinkedList<>();
    Queue<ReadTask> backoffTasks = new LinkedList<>();

    public ConsumerWorker(KafkaRestConfiguration config) {
        this.config = config;
    }

    public synchronized Future readTopic(ConsumerState state, String topic, ReadCallback callback) {
        ReadTask task = new ReadTask(state, topic, callback);
        if (!task.isDone()) {
            tasks.add(task);
            this.notifyAll();
        }
        return task;
    }

    @Override
    public void run() {
        while(isRunning.get()) {
            ReadTask task = null;
            synchronized(this) {
                if (tasks.isEmpty()) {
                    try {
                        long now = config.time.milliseconds();
                        long nextExpiration = nextBackoffExpiration();
                        config.time.waitOn(this, (nextExpiration == Long.MAX_VALUE ? 0 : nextExpiration - now));
                    } catch (InterruptedException e) {
                        // Indication of shutdown
                    }
                }

                long now = config.time.milliseconds();
                while (nextBackoffExpiration() <= now)
                    tasks.add(backoffTasks.remove());

                task = tasks.poll();
                if (task != null) {
                    boolean backoff = task.doPartialRead();
                    if (!task.isDone()) {
                        if (backoff)
                            backoffTasks.add(task);
                        else
                            tasks.add(task);
                    }
                }
            }
        }
        shutdownLatch.countDown();
    }

    private long nextBackoffExpiration() {
        if (backoffTasks.isEmpty())
            return Long.MAX_VALUE;
        else
            return backoffTasks.peek().backoffExpiration;
    }

    public void shutdown() {
        try {
            isRunning.set(false);
            this.interrupt();
            shutdownLatch.await();
        } catch (InterruptedException e) {
            throw new Error("Interrupted when shutting down consumer worker thread.");
        }
    }

    public interface ReadCallback {
        public void onCompletion(List<ConsumerRecord> records);
    }

    private class ReadTask implements Future<List<ConsumerRecord>> {
        ConsumerState state;
        String topic;
        ReadCallback callback;
        CountDownLatch finished;

        ConsumerState.TopicState topicState;
        ConsumerIterator<byte[], byte[]> iter;
        List<ConsumerRecord> messages;
        final long started;

        long backoffExpiration;

        public ReadTask(ConsumerState state, String topic, ReadCallback callback) {
            this.state = state;
            this.topic = topic;
            this.callback = callback;
            this.finished = new CountDownLatch(1);

            started = config.time.milliseconds();
            topicState = state.getOrCreateTopicState(topic);
            if (topicState == null) {
                finish();
                return;
            }
        }

        /**
         * Performs one iteration of reading from a consumer iterator.
         * @return true if this read timed out, indicating the scheduler should back off
         */
        public boolean doPartialRead() {
            try {
                // Initial setup requires locking, which must be done on this thread.
                if (iter == null) {
                    state.startRead(topicState);
                    iter = topicState.stream.iterator();
                    messages = new Vector<>();
                    backoffExpiration = 0;
                }

                boolean backoff = false;

                long startedIteration = config.time.milliseconds();

                try {
                    // Read off as many messages as we can without triggering a timeout exception. The consumer timeout
                    // should be set very small, so the expectation is that even in the worst case,
                    // num_messages * consumer_timeout << request_timeout, so it's safe to only check the elapsed time
                    // once this loop finishes.
                    while (messages.size() < config.consumerRequestMaxMessages && iter.hasNext()) {
                        MessageAndMetadata<byte[], byte[]> msg = iter.next();
                        messages.add(new ConsumerRecord(msg.key(), msg.message(), msg.partition(), msg.offset()));
                        topicState.consumedOffsets.put(msg.partition(), msg.offset());
                    }
                } catch (ConsumerTimeoutException cte) {
                    backoff = true;
                }

                long now = config.time.milliseconds();
                long elapsed = now - started;
                // Compute backoff based on starting time. This makes reasoning about when timeouts should occur simpler
                // for tests.
                backoffExpiration = startedIteration + config.consumerIteratorBackoffMs;

                if (elapsed >= config.consumerRequestTimeoutMs || messages.size() >= config.consumerRequestMaxMessages) {
                    state.finishRead(topicState);
                    finish();
                }

                return backoff;
            } catch (Exception e) {
                state.finishRead(topicState);
                finish();
                System.out.println("Unexpected exception in consumer read thread: " + e.getMessage());
                return false;
            }
        }

        public void finish() {
            callback.onCompletion(messages);
            finished.countDown();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return (finished.getCount() == 0);
        }

        @Override
        public List<ConsumerRecord> get() throws InterruptedException, ExecutionException {
            finished.await();
            return messages;
        }

        @Override
        public List<ConsumerRecord> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            finished.await(timeout, unit);
            if (finished.getCount() > 0)
                throw new TimeoutException();
            return messages;
        }
    }
}
