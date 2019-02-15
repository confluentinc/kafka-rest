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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.ConsumerRecord;
import kafka.common.KafkaException;
import kafka.consumer.FetchedDataChunk;
import kafka.consumer.PartitionTopicInfo;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import scala.collection.JavaConversions;

/**
 * Mock blocking queue that can be used to back a KafkaQueue for mock consumers. This class requires
 * that all data to be produced is available at construction. It also only works for binary
 * consumers since it uses Kafka consumer internal classes that work with raw data before
 * deserialization.
 */
public class MockConsumerQueue implements BlockingQueue<FetchedDataChunk> {

  private Time time;
  private PriorityQueue<ScheduledItems> scheduled = new PriorityQueue<ScheduledItems>();
  private Queue<ConsumerRecord<byte[], byte[]>> ready
      = new LinkedList<ConsumerRecord<byte[], byte[]>>();

  public MockConsumerQueue(Time time, Map<Integer,
      List<ConsumerRecord<byte[], byte[]>>> schedule) {
    this.time = time;
    for (Map.Entry<Integer, List<ConsumerRecord<byte[], byte[]>>> item : schedule.entrySet()) {
      scheduled.add(new ScheduledItems(item.getKey(), item.getValue()));
    }
  }

  @Override
  public boolean add(FetchedDataChunk fetchedDataChunk) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(FetchedDataChunk fetchedDataChunk) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(FetchedDataChunk fetchedDataChunk) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(FetchedDataChunk fetchedDataChunk, long timeout, TimeUnit unit)
      throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FetchedDataChunk take() throws InterruptedException {
    FetchedDataChunk result = null;
    while (result == null) {
      poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
    return result;
  }

  @Override
  public FetchedDataChunk poll(long timeout, TimeUnit unit) throws InterruptedException {
    long now = time.milliseconds();

    while (!scheduled.isEmpty() && scheduled.peek().time <= now) {
      ScheduledItems readyItems = scheduled.poll();
      ready.addAll(readyItems.records);
    }

    if (ready.isEmpty()) {
      long msToSleep;
      if (scheduled.isEmpty()) {
        msToSleep = timeout;
      } else {
        msToSleep = Math.min(scheduled.peek().time - now, timeout);
      }
      time.sleep(msToSleep);
      now = time.milliseconds();
    }

    while (!scheduled.isEmpty() && scheduled.peek().time <= now) {
      ScheduledItems readyItems = scheduled.poll();
      ready.addAll(readyItems.records);
    }

    if (!ready.isEmpty()) {
      ConsumerRecord<byte[], byte[]> c = ready.remove();
      // Special case that schedules can use to indicate we should throw an exception
      if (c == null) {
        throw new KafkaException("null value in mock consumer used to trigger KafkaException");
      }

      ByteBufferMessageSet msgSet = new ByteBufferMessageSet(
          JavaConversions.asScalaBuffer(Arrays.asList(new Message(c.getValue(), c.getKey(), -1L, (byte) 0)))
      );
      AtomicLong consumedOffset = new AtomicLong(0);
      AtomicLong fetchOffset = new AtomicLong(0);
      PartitionTopicInfo
          pti =
          new PartitionTopicInfo(c.getTopic(),
                                 c.getPartition(),
                                 null,
                                 consumedOffset,
                                 fetchOffset,
                                 null,
                                 "clientId");
      return new FetchedDataChunk(msgSet, pti, fetchOffset.get());
    }

    return null;
  }

  @Override
  public int remainingCapacity() {
    return 0;
  }

  @Override
  public boolean remove(Object o) {
    return false;
  }

  @Override
  public boolean contains(Object o) {
    return false;
  }

  @Override
  public int drainTo(Collection<? super FetchedDataChunk> c) {
    return 0;
  }

  @Override
  public int drainTo(Collection<? super FetchedDataChunk> c, int maxElements) {
    return 0;
  }

  @Override
  public FetchedDataChunk remove() {
    return null;
  }

  @Override
  public FetchedDataChunk poll() {
    return null;
  }

  @Override
  public FetchedDataChunk element() {
    return null;
  }

  @Override
  public FetchedDataChunk peek() {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public Iterator<FetchedDataChunk> iterator() {
    return null;
  }

  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return null;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends FetchedDataChunk> c) {
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return false;
  }

  @Override
  public void clear() {

  }

  private class ScheduledItems implements Comparable<ScheduledItems> {

    long time;
    List<ConsumerRecord<byte[], byte[]>> records;

    private ScheduledItems(long time, List<ConsumerRecord<byte[], byte[]>> records) {
      this.time = time;
      this.records = records;
    }

    @Override
    public int compareTo(ScheduledItems o) {
      if (time < o.time) {
        return -1;
      } else if (time == o.time) {
        return 0;
      } else {
        return 1;
      }
    }
  }
}
