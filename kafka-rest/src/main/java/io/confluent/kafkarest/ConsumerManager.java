/*
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

import io.confluent.kafkarest.entities.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.ws.rs.core.Response;

import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;
import io.confluent.rest.exceptions.RestServerErrorException;
import kafka.common.InvalidConfigException;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Manages consumer instances by mapping instance IDs to consumer objects, processing read requests,
 * and cleaning up when consumers disappear.
 */
public class ConsumerManager {

  private static final Logger log = LoggerFactory.getLogger(ConsumerManager.class);

  private final KafkaRestConfig config;
  private final Time time;
  private final String zookeeperConnect;
  private final MetadataObserver mdObserver;
  private final int iteratorTimeoutMs;

  // ConsumerState is generic, but we store them untyped here. This allows many operations to
  // work without having to know the types for the consumer, only requiring type information
  // during read operations.
  private final Map<ConsumerInstanceId, ConsumerState> consumers = new HashMap<>();
  // A few other operations, like commit offsets and closing a consumer can't be interleaved, but
  // they're also comparatively rare. These are executed serially in a dedicated thread.
  private final ExecutorService executor;
  private ConsumerFactory consumerFactory;
  private final PriorityQueue<ConsumerState> consumersByExpiration = new PriorityQueue<>();
  private final DelayQueue<RunnableReadTask> delayedReadTasks = new DelayQueue<>();
  private final ReadTaskSchedulerThread readTaskSchedulerThread;
  private final ExpirationThread expirationThread;

  public ConsumerManager(final KafkaRestConfig config, MetadataObserver mdObserver) {
    this.config = config;
    this.time = config.getTime();
    this.zookeeperConnect = config.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG);
    this.mdObserver = mdObserver;
    this.iteratorTimeoutMs = config.getInt(KafkaRestConfig.CONSUMER_ITERATOR_TIMEOUT_MS_CONFIG);

    // Cached thread pool
    int maxThreadCount = config.getInt(KafkaRestConfig.CONSUMER_MAX_THREADS_CONFIG) < 0
        ? Integer.MAX_VALUE : config.getInt(KafkaRestConfig.CONSUMER_MAX_THREADS_CONFIG);

    this.executor = new ThreadPoolExecutor(0, maxThreadCount,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new RejectedExecutionHandler() {
          @Override
          public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (r instanceof RunnableReadTask) {
              RunnableReadTask readTask = (RunnableReadTask) r;
              int delayMs = ThreadLocalRandom.current().nextInt(25, 75 + 1);
              readTask.waitExpirationMs = config.getTime().milliseconds() + delayMs;
              delayedReadTasks.add(readTask);
            } else {
              // run commitOffset tasks from the caller thread
              if (!executor.isShutdown()) {
                r.run();
              }
            }
          }
        }
    );
    this.consumerFactory = null;
    this.expirationThread = new ExpirationThread();
    this.expirationThread.start();
    this.readTaskSchedulerThread = new ReadTaskSchedulerThread();
    this.readTaskSchedulerThread.start();
  }

  public ConsumerManager(
      KafkaRestConfig config,
      MetadataObserver mdObserver,
      ConsumerFactory consumerFactory
  ) {
    this(config, mdObserver);
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
      Properties props = (Properties) config.getOriginalProperties().clone();
      props.setProperty("zookeeper.connect", zookeeperConnect);
      props.setProperty("group.id", group);
      // This ID we pass here has to be unique, only pass a value along if the deprecated ID field
      // was passed in. This generally shouldn't be used, but is maintained for compatibility.
      if (instanceConfig.getId() != null) {
        props.setProperty("consumer.id", instanceConfig.getId());
      }
      // To support the old consumer interface with broken peek()/missing poll(timeout)
      // functionality, we always use a timeout. This can't perfectly guarantee a total request
      // timeout, but can get as close as this timeout's value
      props.setProperty("consumer.timeout.ms", Integer.toString(iteratorTimeoutMs));
      if (instanceConfig.getAutoCommitEnable() != null) {
        props.setProperty("auto.commit.enable", instanceConfig.getAutoCommitEnable());
      } else {
        props.setProperty("auto.commit.enable", "false");
      }
      if (instanceConfig.getAutoOffsetReset() != null) {
        props.setProperty("auto.offset.reset", instanceConfig.getAutoOffsetReset());
      }
      ConsumerConnector consumer;
      try {
        if (consumerFactory == null) {
          consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        } else {
          consumer = consumerFactory.createConsumer(new ConsumerConfig(props));
        }
      } catch (InvalidConfigException e) {
        throw Errors.invalidConsumerConfigException(e);
      }

      ConsumerState state;
      switch (instanceConfig.getFormat()) {
        case BINARY:
          state = new BinaryConsumerState(this.config, cid, consumer);
          break;
        case AVRO:
          state = new AvroConsumerState(this.config, cid, consumer);
          break;
        case JSON:
          state = new JsonConsumerState(this.config, cid, consumer);
          break;
        default:
          throw new RestServerErrorException(
              "Invalid embedded format for new consumer.",
              Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
          );
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

  // The parameter consumerStateType works around type erasure, allowing us to verify at runtime
  // that the ConsumerState we looked up is of the expected type and will therefore contain the
  // correct decoders
  public <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>
        Future<List<ConsumerRecord<ClientKeyT, ClientValueT>>> readTopic(
      final String group,
      final String instance,
      final String topic,
      Class<? extends ConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>>
          consumerStateType,
      final long maxBytes,
      final ConsumerReadCallback<ClientKeyT, ClientValueT> callback
  ) {
    final ConsumerState state;
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

    // Consumer will try reading even if it doesn't exist, so we need to check this explicitly.
    if (!mdObserver.topicExists(topic)) {
      callback.onCompletion(null, Errors.topicNotFoundException());
      return null;
    }

    ConsumerReadTask<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> task =
        new ConsumerReadTask<>(state,topic, maxBytes, callback);
    executor.submit(new RunnableReadTask(new ReadTaskState(task, state, callback)));
    return task;
  }

  public interface CommitCallback {

    void onCompletion(List<TopicPartitionOffset> offsets, Exception e);
  }

  public Future commitOffsets(String group, String instance, final CommitCallback callback) {
    final ConsumerState state;
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
          List<TopicPartitionOffset> offsets = state.commitOffsets();
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

  public void deleteConsumer(String group, String instance) {
    log.debug("Destroying consumer " + instance + " in group " + group);
    final ConsumerState state = getConsumerInstance(group, instance, true);
    state.close();
  }

  public void shutdown() {
    // Expiration thread needs to be able to acquire a lock on the ConsumerManager to make sure
    // the shutdown will be able to complete.
    log.trace("Shutting down consumer expiration thread");
    expirationThread.shutdown();
    log.trace("Shutting down read task scheduler thread");
    readTaskSchedulerThread.shutdown();
    synchronized (this) {
      for (Map.Entry<ConsumerInstanceId, ConsumerState> entry : consumers.entrySet()) {
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
  private synchronized ConsumerState getConsumerInstance(
      String group,
      String instance,
      boolean remove
  ) {
    ConsumerInstanceId id = new ConsumerInstanceId(group, instance);
    final ConsumerState state = remove ? consumers.remove(id) : consumers.get(id);
    if (state == null) {
      throw Errors.consumerInstanceNotFoundException();
    }
    // Clear from the timeout queue immediately so it isn't removed during the read operation,
    // but don't update the timeout until we finish the read since that can significantly affect
    // the timeout.
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

  class RunnableReadTask implements Runnable, Delayed {
    private final ReadTaskState taskState;
    private final KafkaRestConfig consumerConfig;
    private final long started;
    private final long requestExpiration;
    // Expiration if this task is waiting, considering both the expiration of the whole task and
    // a single backoff, if one is in progress
    private long waitExpirationMs;

    public RunnableReadTask(ReadTaskState taskState) {
      this.taskState = taskState;
      this.started = config.getTime().milliseconds();
      this.consumerConfig = taskState.consumerState.getConfig();
      this.requestExpiration = this.started
          + consumerConfig.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
      this.waitExpirationMs = 0;
    }

    @Override
    public void run() {
      try {
        log.trace("Executing consumer read task ({})", taskState.task);
        if (taskState.task.isDone()) {
          // in the case where an exception is raised in the consumer
          return;
        }

        taskState.task.doPartialRead();
        ConsumerManager.this.updateExpiration(taskState.consumerState);
        if (!taskState.task.isDone()) {
          long backoffTime = config.getTime().milliseconds()
              + consumerConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG);
          waitExpirationMs = Math.min(backoffTime, requestExpiration);

          // add to delayedReadTasks so the scheduler thread can re-schedule another partial read
          delayedReadTasks.add(this);
        } else {
          log.trace("Finished executing consumer read task ({})", taskState.task);
        }
      } catch (Exception e) {
        log.error("Failed to read records consumer "
                + taskState.consumerState.getId().toString(),
            e);
        Exception responseException = e;
        if (!(e instanceof RestException)) {
          responseException = Errors.kafkaErrorException(e);
        }
        taskState.callback.onCompletion(null, (RestException) responseException);
      }
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return waitExpirationMs - config.getTime().milliseconds();
    }

    @Override
    public int compareTo(Delayed o) {
      if (o == null) {
        throw new NullPointerException("Delayed comparator cannot compare with null");
      }
      long otherObjDelay = o.getDelay(TimeUnit.MILLISECONDS);
      long delay = this.getDelay(TimeUnit.MILLISECONDS);

      return Long.compare(delay, otherObjDelay);
    }
  }

  private static class ReadTaskState {
    final ConsumerReadTask task;
    final ConsumerState consumerState;
    final ConsumerReadCallback callback;

    public ReadTaskState(ConsumerReadTask task,
                         ConsumerState state,
                         ConsumerReadCallback callback) {

      this.task = task;
      this.consumerState = state;
      this.callback = callback;
    }
  }

  private class ReadTaskSchedulerThread extends Thread {
    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    ReadTaskSchedulerThread() {
      super("Read Task Scheduler Thread");
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        while (isRunning.get()) {
          RunnableReadTask readTask = delayedReadTasks.poll(500, TimeUnit.MILLISECONDS);
          if (readTask != null) {
            executor.submit(readTask);
          }
        }
      } catch (InterruptedException e) {
        // Interrupted by other thread, do nothing to allow this thread to exit
      } finally {
        shutdownLatch.countDown();
      }
    }

    public void shutdown() {
      try {
        isRunning.set(false);
        this.interrupt();
        shutdownLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted when shutting down read task scheduler thread.");
      }
    }
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
      synchronized (ConsumerManager.this) {
        try {
          while (isRunning.get()) {
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
            long timeout =
                consumersByExpiration.isEmpty()
                ? Long.MAX_VALUE
                : consumersByExpiration.peek().untilExpiration(now);
            ConsumerManager.this.wait(timeout);
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

