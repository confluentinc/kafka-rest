package io.confluent.kafkarest.mock;


import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.ConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


public class MockKafkaConsumer implements Consumer<byte[], byte[]> {

  private List<List<ConsumerRecord<byte[], byte[]>>> schedules;
  private List<Long> relativeTimePoints;
  private Time time;
  private Collection<String> subscription;
  private Collection<TopicPartition> assignment;

  public MockKafkaConsumer(List<List<ConsumerRecord<byte[], byte[]>>> schedules,
                           List<Long> relativeTimePoints,
                           Time time) {
    if (schedules == null || relativeTimePoints == null) {
      this.schedules = new ArrayList<List<ConsumerRecord<byte[], byte[]>>>();
      this.relativeTimePoints = new ArrayList<Long>();
    } else {
      Assert.assertEquals("Size of time points and records does not match", schedules.size(), relativeTimePoints.size());
      this.schedules = schedules;
      this.relativeTimePoints = relativeTimePoints;
    }
    this.time = time;
  }


  @Override
  public Set<TopicPartition> assignment() {
    return new HashSet<>(assignment);
  }

  @Override
  public Set<String> subscription() {
    return new HashSet<>(subscription);
  }

  @Override
  public void subscribe(Collection<String> topics) {
    if (assignment == null) {
      if (subscription == null) {
        subscription = topics;
      }
    } else {
      Assert.fail("Assigned consumer cannot be subscribed");
    }
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    if (assignment == null) {
      if (subscription == null) {
        subscription = topics;
      }
    } else {
      Assert.fail("Assigned consumer cannot be subscribed");
    }
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    if (subscription == null) {
      if (assignment == null) {
        assignment = partitions;
      }
    } else {
      Assert.fail("Subscribed consumer cannot be assigned");
    }
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public void unsubscribe() {
    subscription = null;
    assignment = null;
  }

  /**
   * poll
   */
  @Override
  public ConsumerRecords<byte[], byte[]> poll(long timeout) {
    // if there is no records left wait timeout milliseconds and return empty consumer records
    if (relativeTimePoints.isEmpty()) {
      time.sleep(timeout);
      return ConsumerRecords.empty();
    } else {
      long nearest = relativeTimePoints.get(0);
      if (nearest > timeout) {
        // the nearest record appears later than requested timeout
        relativeTimePoints.set(0, nearest - timeout);
        time.sleep(timeout);
        return ConsumerRecords.empty();
      } else {

        Map<TopicPartition, List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>>> records =
          new HashMap<>();
        for (ConsumerRecord<byte[],byte[]> record: schedules.get(0)) {
          TopicPartition tp = new TopicPartition(record.getTopic(), record.getPartition());
          org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> rec =
            new org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>(
              record.getTopic(),
              record.getPartition(),
              record.getOffset(),
              record.getKey(),
              record.getValue());
          if (records.containsKey(tp)) {
            records.get(tp).add(rec);
          } else {
            ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> consumerRecords =
              new ArrayList<>();
            consumerRecords.add(rec);
            records.put(tp, consumerRecords);
          }
        }

        // remove from queue
        time.sleep(relativeTimePoints.get(0));
        relativeTimePoints.remove(0);
        schedules.remove(0);
        return new ConsumerRecords<>(records);
      }
    }
  }

  @Override
  public void commitSync() {

  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {

  }

  @Override
  public void commitAsync() {
    throw new AssertionError("Not implemented");
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public void seek(TopicPartition partition, long offset) {

  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public long position(TopicPartition partition) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new AssertionError("Not implemented");
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    throw new AssertionError("Not implemented");
  }

  @Override
  public Set<TopicPartition> paused() {
    throw new AssertionError("Not implemented");
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public void close() {

  }

  @Override
  public void wakeup() {

  }
}
