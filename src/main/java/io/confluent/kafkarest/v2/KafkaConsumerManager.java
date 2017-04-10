
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

import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerWorkerReadCallback;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.ConsumerAssignmentRequest;
import io.confluent.kafkarest.entities.ConsumerAssignmentResponse;
import io.confluent.kafkarest.entities.ConsumerCommittedRequest;
import io.confluent.kafkarest.entities.ConsumerCommittedResponse;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerSeekToOffsetRequest;
import io.confluent.kafkarest.entities.ConsumerSeekToRequest;
import io.confluent.kafkarest.entities.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.ConsumerSubscriptionResponse;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.TopicPartitionOffsetMetadata;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;
import io.confluent.rest.exceptions.RestServerErrorException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages consumer instances by mapping instance IDs to consumer objects, processing read requests,
 * and cleaning up when consumers disappear.
 */
public class KafkaConsumerManager {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerManager.class);

  private final KafkaRestConfig config;
  private final Time time;
  private final String bootstrapServers;

  // KafkaConsumerState is generic, but we store them untyped here. This allows many operations to
  // work without having to know the types for the consumer, only requiring type information
  // during read operations.
  private final Map<ConsumerInstanceId, KafkaConsumerState> consumers =
      new HashMap<ConsumerInstanceId, KafkaConsumerState>();
  // Read operations are common and there may be many concurrently, so they are farmed out to
  // worker threads that can efficiently interleave the operations. Currently we're just using a
  // simple round-robin scheduler.
  private final List<KafkaConsumerWorker> workers;
  private final AtomicInteger nextWorker;
  // A few other operations, like commit offsets and closing a consumer can't be interleaved, but
  // they're also comparatively rare. These are executed serially in a dedicated thread.
  private final ExecutorService executor;
  private KafkaConsumerFactory consumerFactory;
  private final PriorityQueue<KafkaConsumerState> consumersByExpiration =
      new PriorityQueue<KafkaConsumerState>();
  private final ExpirationThread expirationThread;

  public KafkaConsumerManager(KafkaRestConfig config) {
    this.config = config;
    this.time = config.getTime();
    this.bootstrapServers = config.getString(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG);
    this.workers = new Vector<KafkaConsumerWorker>();
    for (int i = 0; i < config.getInt(KafkaRestConfig.CONSUMER_THREADS_CONFIG); i++) {
      KafkaConsumerWorker worker = new KafkaConsumerWorker(config);
      workers.add(worker);
      worker.start();
    }
    nextWorker = new AtomicInteger(0);
    this.executor = Executors.newFixedThreadPool(1);
    this.consumerFactory = null;
    this.expirationThread = new ExpirationThread();
    this.expirationThread.start();
  }

  KafkaConsumerManager(KafkaRestConfig config, KafkaConsumerFactory consumerFactory) {
    this(config);
    this.consumerFactory = consumerFactory;
  }

  /**
   * Creates a new consumer instance and returns its unique ID.
   *
   * @param group Name of the consumer group to join
   * @param instanceConfig configuration parameters for the consumer
   * @return Unique consumer instance ID
   */
  public String createConsumer(String group, ConsumerInstanceConfig instanceConfig) {
    // The terminology here got mixed up for historical reasons, and remaining compatible moving
    // forward is tricky. To maintain compatibility, if the 'id' field is specified we maintain
    // the previous behavior of using it's value in both the URLs for the consumer (i.e. the
    // local name) and the ID (consumer.id setting in the consumer). Otherwise, the 'name' field
    // only applies to the local name. When we replace with the new consumer, we may want to
    // provide an alternate app name, or just reuse the name.
    String name = instanceConfig.getName();
    if (instanceConfig.getId() != null) { // Explicit ID request always overrides name
      name = instanceConfig.getId();
    }
    if (name == null) {
      name = "rest-consumer-";
      String serverId = this.config.getString(KafkaRestConfig.ID_CONFIG);
      if (!serverId.isEmpty()) {
        name += serverId + "-";
      }
      name += UUID.randomUUID().toString();
    }

    ConsumerInstanceId cid = new ConsumerInstanceId(group, name);
    // Perform this check before
    synchronized (this) {
      if (consumers.containsKey(cid)) {
        throw Errors.consumerAlreadyExistsException();
      } else {
        // Placeholder to reserve this ID
        consumers.put(cid, null);
      }
    }

    // Ensure we clean up the placeholder if there are any issues creating the consumer instance
    boolean succeeded = false;
    try {
      log.debug("Creating consumer " + name + " in group " + group);

      // Note the ordering here. We want to allow overrides, but almost all the
      // consumer-specific settings don't make sense to override globally (e.g. group ID, consumer
      // ID), and others we want to ensure get overridden (e.g. consumer.timeout.ms, which we
      // intentionally name differently in our own configs).
      //Properties props = (Properties) config.getOriginalProperties().clone();
      Properties props = config.getConsumerProperties();
      props.setProperty(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
      props.setProperty("group.id", group);
      // This ID we pass here has to be unique, only pass a value along if the deprecated ID field
      // was passed in. This generally shouldn't be used, but is maintained for compatibility.
      if (instanceConfig.getId() != null) {
        props.setProperty("consumer.id", instanceConfig.getId());
      }
      if (instanceConfig.getAutoCommitEnable() != null) {
        props.setProperty("enable.auto.commit", instanceConfig.getAutoCommitEnable());
      }
      if (instanceConfig.getAutoOffsetReset() != null) {
        props.setProperty("auto.offset.reset", instanceConfig.getAutoOffsetReset());
      }

      props.setProperty("schema.registry.url",
          config.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG));

      switch (instanceConfig.getFormat()) {
        case AVRO:
          props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
          props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
          break;
        case JSON:
        case BINARY:
        default:
          props.put("key.deserializer",
              "org.apache.kafka.common.serialization.ByteArrayDeserializer");
          props.put("value.deserializer",
              "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      }

      Consumer consumer = null;
      try {
        if (consumerFactory == null) {
          consumer = new KafkaConsumer(props);
        } else {
          consumer = consumerFactory.createConsumer(props);
        }
      } finally {
      }

      KafkaConsumerState state = null;
      switch (instanceConfig.getFormat()) {
        case BINARY:
          state = new BinaryKafkaConsumerState(this.config, cid, consumer);
          break;
        case AVRO:
          state = new AvroKafkaConsumerState(this.config, cid, consumer);
          break;
        case JSON:
          state = new JsonKafkaConsumerState(this.config, cid, consumer);
          break;
        default:
          throw new RestServerErrorException("Invalid embedded format for new consumer.",
              Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
      }
      synchronized (this) {
        consumers.put(cid, state);
        consumersByExpiration.add(state);
        this.notifyAll();
      }
      succeeded = true;
      return name;
    } finally {
      if (!succeeded) {
        synchronized (this) {
          consumers.remove(cid);
        }
      }
    }
  }

  public interface ReadCallback<K, V> {

    public void onCompletion(List<? extends ConsumerRecord<K, V>> records, Exception e);
  }


  // The parameter consumerStateType works around type erasure, allowing us to verify at runtime
  // that the KafkaConsumerState we looked up is of the expected type and will therefore contain the
  // correct decoders
  public <KafkaK, KafkaV, ClientK, ClientV>
  Future readRecords(final String group, final String instance,
      Class<? extends KafkaConsumerState<KafkaK, KafkaV, ClientK, ClientV>> consumerStateType,
      final long timeout, final long maxBytes, final ReadCallback callback) {
    final KafkaConsumerState state;
    try {
      state = getConsumerInstance(group, instance);
    } catch (RestNotFoundException e) {
      callback.onCompletion(null, e);
      return null;
    }

    if (!consumerStateType.isInstance(state)) {
      callback.onCompletion(null, Errors.consumerFormatMismatch());
      return null;
    }

    int workerId = nextWorker.getAndIncrement() % workers.size();
    KafkaConsumerWorker worker = workers.get(workerId);
    return worker.readRecords(
        state, timeout, maxBytes,
        new ConsumerWorkerReadCallback<ClientK, ClientV>() {
          @Override
          public void onCompletion(
              List<? extends ConsumerRecord<ClientK, ClientV>> records, Exception e) {
            updateExpiration(state);
            if (e != null) {
              // Ensure caught exceptions are converted to RestExceptions so the user gets a
              // nice error message. Currently we don't define any more specific errors because
              // the old consumer interface doesn't classify the errors well like the new
              // consumer does. When the new consumer is available we may be able to update this
              // to provide better feedback to the user.
              Exception responseException = e;
              if (!(e instanceof RestException)) {
                responseException = Errors.kafkaErrorException(e);
              }
              callback.onCompletion(null, responseException);
            } else {
              callback.onCompletion(records, null);
            }
          }
        }
    );
  }

  public interface CommitCallback {

    public void onCompletion(List<TopicPartitionOffset> offsets, Exception e);
  }

  public Future commitOffsets(String group, String instance, final String async,
      final ConsumerOffsetCommitRequest offsetCommitRequest, final CommitCallback callback) {
    final KafkaConsumerState state;
    try {
      state = getConsumerInstance(group, instance);
    } catch (RestNotFoundException e) {
      callback.onCompletion(null, e);
      return null;
    }

    return executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          List<TopicPartitionOffset> offsets = state.commitOffsets(async, offsetCommitRequest);
          callback.onCompletion(offsets, null);
        } catch (Exception e) {
          log.error("Failed to commit offsets for consumer " + state.getId().toString(), e);
          Exception responseException = e;
          if (!(e instanceof RestException)) {
            responseException = Errors.kafkaErrorException(e);
          }
          callback.onCompletion(null, responseException);
        } finally {
          updateExpiration(state);
        }
      }
    });
  }

  public ConsumerCommittedResponse committed(String group, String instance,
      ConsumerCommittedRequest request) {
    log.debug("Committed offsets for consumer " + instance + " in group " + group);
    ConsumerCommittedResponse response = new ConsumerCommittedResponse();
    KafkaConsumerState state = getConsumerInstance(group, instance);
    if (state != null) {
      response = state.committed(request);

    } else {

      response.offsets = new ArrayList<TopicPartitionOffsetMetadata>();
    }
    return response;
  }

  public void deleteConsumer(String group, String instance) {
    log.debug("Destroying consumer " + instance + " in group " + group);
    final KafkaConsumerState state = getConsumerInstance(group, instance, true);
    state.close();
  }

  public void subscribe(String group, String instance, ConsumerSubscriptionRecord subscription) {
    log.debug("Subscribing consumer " + instance + " in group " + group);
    KafkaConsumerState state = getConsumerInstance(group, instance);
    if (state != null) {
      state.subscribe(subscription);
    }

  }

  public void unsubscribe(String group, String instance) {
    log.debug("Unsubcribing consumer " + instance + " in group " + group);
    KafkaConsumerState state = getConsumerInstance(group, instance);
    if (state != null) {
      state.unsubscribe();
    }

  }

  public ConsumerSubscriptionResponse subscription(String group, String instance) {
    ConsumerSubscriptionResponse response = new ConsumerSubscriptionResponse();
    KafkaConsumerState state = getConsumerInstance(group, instance);
    if (state != null) {
      java.util.Set<java.lang.String> topics = state.subscription();
      response.topics = new ArrayList<String>(topics);
    } else {
      response.topics = new ArrayList<String>();
    }
    return response;
  }

  public void seekToBeginning(String group, String instance, ConsumerSeekToRequest seekToRequest) {
    log.debug("seeking to beginning " + instance + " in group " + group);
    KafkaConsumerState state = getConsumerInstance(group, instance);
    if (state != null) {
      state.seekToBeginning(seekToRequest);
    }

  }

  public void seekToEnd(String group, String instance, ConsumerSeekToRequest seekToRequest) {
    log.debug("seeking to end " + instance + " in group " + group);
    KafkaConsumerState state = getConsumerInstance(group, instance);
    if (state != null) {
      state.seekToEnd(seekToRequest);
    }

  }

  public void seekToOffset(String group, String instance,
      ConsumerSeekToOffsetRequest seekToOffsetRequest) {
    log.debug("seeking to offset " + instance + " in group " + group);
    KafkaConsumerState state = getConsumerInstance(group, instance);
    if (state != null) {
      state.seekToOffset(seekToOffsetRequest);
    }

  }

  public void assign(String group, String instance, ConsumerAssignmentRequest assignmentRequest) {
    log.debug("seeking to end " + instance + " in group " + group);
    KafkaConsumerState state = getConsumerInstance(group, instance);
    if (state != null) {
      state.assign(assignmentRequest);
    }

  }


  public ConsumerAssignmentResponse assignment(String group, String instance) {
    log.debug("getting assignment for  " + instance + " in group " + group);
    ConsumerAssignmentResponse response = new ConsumerAssignmentResponse();
    response.partitions = new Vector<io.confluent.kafkarest.entities.TopicPartition>();
    KafkaConsumerState state = getConsumerInstance(group, instance);
    if (state != null) {
      java.util.Set<TopicPartition> topicPartitions = state.assignment();
      for (TopicPartition t : topicPartitions) {

        response.partitions
            .add(new io.confluent.kafkarest.entities.TopicPartition(t.topic(), t.partition()));
      }

    }
    return response;
  }


  public void shutdown() {
    log.debug("Shutting down consumers");
    synchronized (this) {
      for (KafkaConsumerWorker worker : workers) {
        log.trace("Shutting down worker " + worker.toString());
        worker.shutdown();
      }
      workers.clear();
    }
    // Expiration thread needs to be able to acquire a lock on the KafkaConsumerManager to make sure
    // the shutdown will be able to complete.
    log.trace("Shutting down consumer expiration thread");
    expirationThread.shutdown();
    synchronized (this) {
      for (Map.Entry<ConsumerInstanceId, KafkaConsumerState> entry : consumers.entrySet()) {
        entry.getValue().close();
      }
      consumers.clear();
      consumersByExpiration.clear();
      executor.shutdown();
    }
  }

  /**
   * Gets the specified consumer instance or throws a not found exception. Also removes the
   * consumer's expiration timeout so it is not cleaned up mid-operation.
   */
  private synchronized KafkaConsumerState getConsumerInstance(String group, String instance,
      boolean remove) {
    ConsumerInstanceId id = new ConsumerInstanceId(group, instance);
    final KafkaConsumerState state = remove ? consumers.remove(id) : consumers.get(id);
    if (state == null) {
      throw Errors.consumerInstanceNotFoundException();
    }
    // Clear from the timeout queue immediately so it isn't removed during the read operation,
    // but don't update the timeout until we finish the read since that can significantly affect
    // the timeout.
    consumersByExpiration.remove(state);
    return state;
  }

  private KafkaConsumerState getConsumerInstance(String group, String instance) {
    return getConsumerInstance(group, instance, false);
  }

  private synchronized void updateExpiration(KafkaConsumerState state) {
    state.updateExpiration();
    consumersByExpiration.add(state);
    this.notifyAll();
  }


  public interface KafkaConsumerFactory {

    Consumer createConsumer(Properties props);
  }

  private class ExpirationThread extends Thread {

    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    public ExpirationThread() {
      super("Consumer Expiration Thread");
      setDaemon(true);
    }

    @Override
    public void run() {
      synchronized (KafkaConsumerManager.this) {
        try {
          while (isRunning.get()) {
            long now = time.milliseconds();
            while (!consumersByExpiration.isEmpty() && consumersByExpiration.peek().expired(now)) {
              final KafkaConsumerState state = consumersByExpiration.remove();
              consumers.remove(state.getId());
              executor.submit(new Runnable() {
                @Override
                public void run() {
                  state.close();
                }
              });
            }
            long
                timeout =
                (consumersByExpiration.isEmpty() ? Long.MAX_VALUE : consumersByExpiration.peek()
                    .untilExpiration(now));
            KafkaConsumerManager.this.wait(timeout);
          }
        } catch (InterruptedException e) {
          // Interrupted by other thread, do nothing to allow this thread to exit
        }
      }
      shutdownLatch.countDown();
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
  }
}
