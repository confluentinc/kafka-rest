/**
 * Copyright 2017 Confluent Inc.
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
package io.confluent.kafkarest.v2;

import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Vector;
import java.util.Collection;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.TopicPartitionOffsetMetadata;
import io.confluent.kafkarest.entities.ConsumerSeekToRequest;
import io.confluent.kafkarest.entities.ConsumerSeekToOffsetRequest;
import io.confluent.kafkarest.entities.ConsumerAssignmentRequest;
import io.confluent.kafkarest.entities.ConsumerCommittedRequest;
import io.confluent.kafkarest.entities.ConsumerCommittedResponse;

import kafka.serializer.Decoder;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import io.confluent.kafkarest.entities.ConsumerSubscriptionRecord;

import io.confluent.kafkarest.*;

/**
 * Tracks all the state for a consumer. This class is abstract in order to support multiple
 * serialization formats. Implementations must provide decoders and a method to convert Kafka
 * MessageAndMetadata<K,V> values to ConsumerRecords that can be returned to the client (including
 * translation if the decoded Kafka consumer type and ConsumerRecord types differ).
 */
public abstract class KafkaConsumerState<KafkaK, KafkaV, ClientK, ClientV>
    implements Comparable<KafkaConsumerState> {

  private KafkaRestConfig config;
  private ConsumerInstanceId instanceId;
  private Consumer consumer;
  private Map<String, KafkaConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV>> topics;
  private long expiration;
  // A read/write lock on the KafkaConsumerState allows concurrent readRecord calls, but allows
  // commitOffsets to safely lock the entire state in order to get correct information about all
  // the topic/stream's current offset state. All operations on individual TopicStates must be
  // synchronized at that level as well (so, e.g., readRecord may modify a single TopicState, but
  // only needs read access to the KafkaConsumerState).
  private ReadWriteLock lock;

  public KafkaConsumerState(KafkaRestConfig config, ConsumerInstanceId instanceId,
                       Consumer consumer) {
    this.config = config;
    this.instanceId = instanceId;
    this.consumer = consumer;
    this.topics = new HashMap<String, KafkaConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV>>();
    this.expiration = config.getTime().milliseconds() +
                      config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
    this.lock = new ReentrantReadWriteLock();
  }

  public ConsumerInstanceId getId() {
    return instanceId;
  }

  /**
   * Gets the key decoder for the Kafka consumer.
   */
  protected abstract Decoder<KafkaK> getKeyDecoder();

  /**
   * Gets the value decoder for the Kafka consumer.
   */
  protected abstract Decoder<KafkaV> getValueDecoder();

  /**
   * Converts a MessageAndMetadata using the Kafka decoder types into a ConsumerRecord using the
   * client's requested types. While doing so, computes the approximate size of the message in
   * bytes, which is used to track the approximate total payload size for consumer read responses to
   * determine when to trigger the response.
   */
  public abstract ConsumerRecordAndSize<ClientK, ClientV> createConsumerRecord(
      ConsumerRecord<KafkaK, KafkaV> msg);

  /**
   * Start a read on the given topic, enabling a read lock on this KafkaConsumerState and a full lock on
   * the KafkaConsumerTopicState.
   */
  public void startRead(KafkaConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV> topicState) {
    lock.readLock().lock();
    topicState.lock();
  }

  /**
   * Finish a read request, releasing the lock on the KafkaConsumerTopicState and the read lock on this
   * KafkaConsumerState.
   */
  public void finishRead(KafkaConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV> topicState) {
    topicState.unlock();
    lock.readLock().unlock();
  }

  public List<TopicPartitionOffset> commitOffsets(String async, ConsumerOffsetCommitRequest offsetCommitRequest) {
    lock.writeLock().lock();
    try {
	if (offsetCommitRequest == null) {
	    if (async == null) {
		consumer.commitSync();
	    }
	    else {
		consumer.commitAsync();		
	    }
	} else {
	    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<TopicPartition, OffsetAndMetadata>();

	    
	    //commit each given offset
	    for(TopicPartitionOffsetMetadata t: offsetCommitRequest.offsets) {
		if (t.getMetadata() == null) {
		    offsetMap.put(new TopicPartition(t.getTopic(), t.getPartition()),
				  new OffsetAndMetadata(t.getOffset()+1)   );
		} else {
		    offsetMap.put(new TopicPartition(t.getTopic(), t.getPartition()),
				  new OffsetAndMetadata(t.getOffset()+1, t.getMetadata()) );				    
		}

	    }
	    consumer.commitSync(offsetMap);			    
	}
      List<TopicPartitionOffset> result = getOffsets(true);
      return result;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void seekToBeginning(ConsumerSeekToRequest seekToRequest) {
    lock.writeLock().lock();
    try {
	if (seekToRequest != null) {
	    Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

	    for(io.confluent.kafkarest.entities.TopicPartition t: seekToRequest.partitions) {
		topicPartitions.add(new TopicPartition(t.getTopic(), t.getPartition()));
	    }
	    consumer.seekToBeginning(topicPartitions);			    
	}
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void seekToEnd(ConsumerSeekToRequest seekToRequest) {
    lock.writeLock().lock();
    try {
	if (seekToRequest != null) {
	    Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

	    for(io.confluent.kafkarest.entities.TopicPartition t: seekToRequest.partitions) {
		topicPartitions.add(new TopicPartition(t.getTopic(), t.getPartition()));
	    }
	    consumer.seekToEnd(topicPartitions);			    
	}
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void seekToOffset(ConsumerSeekToOffsetRequest seekToOffsetRequest) {
    lock.writeLock().lock();
    try {
	if (seekToOffsetRequest != null) {
	    Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

	    for(TopicPartitionOffsetMetadata t: seekToOffsetRequest.offsets) {
		TopicPartition topicPartition = new TopicPartition(t.getTopic(), t.getPartition());
		consumer.seek(topicPartition, t.getOffset());
	    }

	}
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void assign(ConsumerAssignmentRequest assignmentRequest) {
    lock.writeLock().lock();
    try {
	if (assignmentRequest != null) {
	    Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

	    for(io.confluent.kafkarest.entities.TopicPartition t: assignmentRequest.partitions) {
		topicPartitions.add(new TopicPartition(t.getTopic(), t.getPartition()));
	    }
	    consumer.assign(topicPartitions);			    
	}
    } finally {
      lock.writeLock().unlock();
    }
  }

    
  public void close() {
    lock.writeLock().lock();
    try {
	if (consumer != null) {	
	    consumer.close();
	}
      // Marks this state entry as no longer valid because the consumer group is being destroyed.
      consumer = null;
      topics = null;
    } finally {
      lock.writeLock().unlock();
    }
  }


  public void subscribe(ConsumerSubscriptionRecord subscription) {
    if (subscription == null){
	  return;
    }
    
    lock.writeLock().lock();
    try {
	if (consumer != null) {
	    if (subscription.topics != null) {
		consumer.subscribe(subscription.topics);
	    }
	    else if (subscription.getTopicPattern() != null) {
		Pattern topicPattern = Pattern.compile(subscription.getTopicPattern());
		NoOpOnRebalance noOpOnRebalance = new NoOpOnRebalance();
		    consumer.subscribe(topicPattern, noOpOnRebalance);		
	    }
	}
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void unsubscribe() {
    lock.writeLock().lock();
    try {
	if (consumer != null) {
	    consumer.unsubscribe();
	}
    } finally {
      lock.writeLock().unlock();
    }
  }

  public java.util.Set<String> subscription() {
    java.util.Set<String> currSubscription = null;
    lock.writeLock().lock();
    try {
	if (consumer != null) {
	    currSubscription = consumer.subscription();
	}
    } finally {
      lock.writeLock().unlock();
    }
    return currSubscription;
  }
    

  public java.util.Set<TopicPartition> assignment() {
    java.util.Set<TopicPartition> currAssignment = null;
    lock.writeLock().lock();
    try {
	if (consumer != null) {
	    currAssignment = consumer.assignment();
	}
    } finally {
      lock.writeLock().unlock();
    }
    return currAssignment;
  }
    

  public ConsumerCommittedResponse committed(ConsumerCommittedRequest request) {
    ConsumerCommittedResponse response  = new ConsumerCommittedResponse();
    response.offsets = new Vector<TopicPartitionOffsetMetadata>();
    lock.writeLock().lock();
    try {
	if (consumer != null) {
	    for (io.confluent.kafkarest.entities.TopicPartition t: request.partitions) {
		TopicPartition partition = new TopicPartition(t.getTopic(), t.getPartition());
		OffsetAndMetadata offsetMetadata = consumer.committed(partition);
		if (offsetMetadata != null) {
		    response.offsets.add(new TopicPartitionOffsetMetadata(partition.topic(), partition.partition(), offsetMetadata.offset(), offsetMetadata.metadata()));
		}
	    }
	}
    } finally {
      lock.writeLock().unlock();
    }
    return response;
  }
    
    
  public boolean expired(long nowMs) {
    return expiration <= nowMs;
  }

  public void updateExpiration() {
    this.expiration = config.getTime().milliseconds() +
                      config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
  }

  public long untilExpiration(long nowMs) {
    return this.expiration - nowMs;
  }

  public KafkaRestConfig getConfig() {
    return config;
  }

  public void setConfig(KafkaRestConfig config) {
    this.config = config;
  }

  @Override
  public int compareTo(KafkaConsumerState o) {
    if (this.expiration < o.expiration) {
      return -1;
    } else if (this.expiration == o.expiration) {
      return 0;
    } else {
      return 1;
    }
  }

    public KafkaConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV> getOrCreateTopicState(String topic, long timeout) {

    lock.writeLock().lock();
    try {
      if (topics == null) {
        return null;
      }
      if (topic != null) {
	  Map<String, Integer> subscriptions = new TreeMap<String, Integer>();
	  subscriptions.put(topic, 1);

	  consumer.subscribe(Arrays.asList(topic));
      }
      ConsumerRecords<ClientK, ClientV> consumerRecords = consumer.poll(timeout);
      KafkaConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV>  state = new KafkaConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV>(consumerRecords);
      return state;

    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Gets a list of TopicPartitionOffsets describing the current state of consumer offsets, possibly
   * updating the committed offset record. This method is not synchronized.
   *
   * @param updateCommitOffsets if true, updates committed offsets to be the same as the consumed
   *                            offsets.
   */
  private List<TopicPartitionOffset> getOffsets(boolean updateCommitOffsets) {
    List<TopicPartitionOffset> result = new Vector<TopicPartitionOffset>();
    for (Map.Entry<String, KafkaConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV>> entry
        : topics.entrySet()) {
      KafkaConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV> state = entry.getValue();
      state.lock();
      try {
        for (Map.Entry<Integer, Long> partEntry : state.getConsumedOffsets().entrySet()) {
          Integer partition = partEntry.getKey();
          Long offset = partEntry.getValue();
          Long committedOffset = 0L;
          if (updateCommitOffsets) {
            state.getCommittedOffsets().put(partition, offset);
            committedOffset = offset;
          } else {
            committedOffset = state.getCommittedOffsets().get(partition);
          }
          result.add(new TopicPartitionOffset(entry.getKey(), partition,
                                              offset,
                                              (committedOffset == null ? -1 : committedOffset)));
        }
      } finally {
        state.unlock();
      }
    }
    return result;
  }


  private class NoOpOnRebalance implements ConsumerRebalanceListener {

       public NoOpOnRebalance() {

       }

       public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
       }

       public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
       }
  }
    
}

