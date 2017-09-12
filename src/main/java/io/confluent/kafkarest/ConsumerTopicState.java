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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * Tracks a consumer's state for a single topic, including the underlying stream and consumed and
 * committed offsets. It provides manual synchronization primitives to support ConsumerWorkers
 * protecting access to the state while they process a read request in their processing loop.
 */
public class ConsumerTopicState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> {

  private final Lock lock = new ReentrantLock();
  private final KafkaStream<KafkaKeyT, KafkaValueT> stream;
  private final Map<Integer, Long> consumedOffsets;
  private final Map<Integer, Long> committedOffsets;

  // The last read task on this topic that failed. Allows the next read to pick up where this one
  // left off, including accounting for response size limits
  private ConsumerReadTask failedTask;

  public ConsumerTopicState(KafkaStream<KafkaKeyT, KafkaValueT> stream) {
    this.stream = stream;
    this.consumedOffsets = new HashMap<Integer, Long>();
    this.committedOffsets = new HashMap<Integer, Long>();
  }

  public void lock() {
    lock.lock();
  }

  public void unlock() {
    lock.unlock();
  }

  public KafkaStream<KafkaKeyT, KafkaValueT> getStream() {
    return stream;
  }

  public ConsumerIterator<KafkaKeyT, KafkaValueT> getIterator() {
    return stream.iterator();
  }

  public Map<Integer, Long> getConsumedOffsets() {
    return consumedOffsets;
  }

  public Map<Integer, Long> getCommittedOffsets() {
    return committedOffsets;
  }

  public ConsumerReadTask clearFailedTask() {
    ConsumerReadTask t = failedTask;
    failedTask = null;
    return t;
  }

  public void setFailedTask(ConsumerReadTask failedTask) {
    this.failedTask = failedTask;
  }
}
