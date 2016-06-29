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

import io.confluent.kafkarest.entities.AbstractConsumerRecord;
import io.confluent.rest.exceptions.RestException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * State for tracking the progress of a single consumer read request.
 *
 * To support embedded formats that require translation between the format deserialized by the Kafka
 * deserializer and the format returned in the AbstractConsumerRecord entity sent back to the client, this class
 * uses two pairs of key-value generic type parameters: KafkaK/KafkaV is the format returned by the
 * Kafka consumer's deserializer, ClientK/ClientV is the format returned to the client in
 * the HTTP response. In some cases these may be identical.
 */
class ConsumerReadTask<KafkaK, KafkaV, ClientK, ClientV>
    implements Future<List<AbstractConsumerRecord<ClientK, ClientV>>> {

  private static final Logger log = LoggerFactory.getLogger(ConsumerReadTask.class);

  private ConsumerState parent;
  private final long maxResponseBytes;
  private final ConsumerWorkerReadCallback<ClientK, ClientV> callback;
  private CountDownLatch finished;

  private ConsumerSubscriptionState subscriptionState;
  private Iterator<ConsumerRecord<KafkaK, KafkaV>> iter;
  private Consumer<KafkaK, KafkaV> consumer;
  private List<AbstractConsumerRecord<ClientK, ClientV>> messages;
  private long bytesConsumed = 0;
  private final long started;

  // Expiration if this task is waiting, considering both the expiration of the whole task and
  // a single backoff, if one is in progress
  long waitExpiration;

  public ConsumerReadTask(ConsumerState parent, String topic, long maxBytes,
                          ConsumerWorkerReadCallback<ClientK, ClientV> callback) {
    this.parent = parent;
    this.maxResponseBytes = Math.min(
        maxBytes,
        parent.getConfig().getLong(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG));
    this.callback = callback;
    this.finished = new CountDownLatch(1);

    started = parent.getConfig().getTime().milliseconds();
    try {
      subscriptionState = parent.tryToSubscribe(topic);

      // If the previous call failed, restore any outstanding data into this task.
      ConsumerReadTask previousTask = subscriptionState.clearFailedTask();
      if (previousTask != null) {
        this.messages = previousTask.messages;
        this.bytesConsumed = previousTask.bytesConsumed;
      }
    } catch (RestException e) {
      finish(e);
    }
  }

  /**
   * Performs one iteration of reading from a consumer iterator.
   *
   * @return true if this read timed out, indicating the scheduler should back off
   */
  public boolean doPartialRead() {
    try {
      // Initial setup requires locking, which must be done on this thread.
      if (consumer == null) {
        parent.startRead(subscriptionState);
        consumer = subscriptionState.getConsumer();
        messages = new Vector<AbstractConsumerRecord<ClientK, ClientV>>();
        waitExpiration = 0;
      }

      boolean backoff = false;

      long startedIteration = parent.getConfig().getTime().milliseconds();
      final int requestTimeoutMs = parent.getConfig().getInt(
          KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
      final long endTime = startedIteration + requestTimeoutMs;
      final int itBackoff = parent.getConfig().getInt(
        KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG);
      try {
        while (parent.getConfig().getTime().milliseconds() < endTime) {

          if (iter == null || !iter.hasNext()) {
            // The consumer timeout should be set very small, so the expectation is that even in the
            // worst case, num_messages * consumer_timeout << request_timeout, so it's safe to only
            // check the elapsed time once this loop finishes.
            ConsumerRecords<KafkaK, KafkaV> records = consumer.poll(itBackoff);
            iter = records.iterator();
            if (!iter.hasNext()) {
              // there are no records left
              backoff = true;
              break;
            }
          }

          while (iter.hasNext()) {
            ConsumerRecord<KafkaK, KafkaV> record = iter.next();
            ConsumerRecordAndSize<ClientK, ClientV> recordAndSize = parent.convertConsumerRecord(record);
            long roughMsgSize = recordAndSize.getSize();
            if (bytesConsumed + roughMsgSize > maxResponseBytes) {
              break;
            }

            messages.add(recordAndSize.getRecord());
            bytesConsumed += roughMsgSize;
            // Updating the consumed offsets isn't done until we're actually going to return the
            // data since we may encounter an error during a subsequent read, in which case we'll
            // have to defer returning the data so we can return an HTTP error instead

          }
        }
      } catch (WakeupException cte) {
        backoff = true;
      }

      long now = parent.getConfig().getTime().milliseconds();
      long elapsed = now - started;
      // Compute backoff based on starting time. This makes reasoning about when timeouts
      // should occur simpler for tests.
      long backoffExpiration = startedIteration + itBackoff;
      long requestExpiration =
          started + parent.getConfig().getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
      waitExpiration = Math.min(backoffExpiration, requestExpiration);

      if (elapsed >= requestTimeoutMs || bytesConsumed >= maxResponseBytes) {
        finish();
      }

      return backoff;
    } catch (Exception e) {
      finish(e);
      log.error("Unexpected exception in consumer read thread: ", e);
      return false;
    }
  }

  public void finish() {
    finish(null);
  }

  public void finish(Exception e) {
    if (e == null) {
      // Now it's safe to mark these messages as consumed by updating offsets since we're actually
      // going to return the data.
      Map<TopicPartition, OffsetAndMetadata> consumedOffsets = subscriptionState.getConsumedOffsets();
      for (AbstractConsumerRecord<ClientK, ClientV> msg : messages) {
        TopicPartition topicPartition = new TopicPartition(msg.getTopic(), msg.getPartition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(msg.getOffset(), "");
        consumedOffsets.put(topicPartition, offsetAndMetadata);
      }
    } else {
      // If we read any messages before the exception occurred, keep this task so we don't lose
      // messages. Subsequent reads will add the outstanding messages before attempting to read
      // any more from the consumer stream iterator
      if (subscriptionState != null && messages != null && messages.size() > 0) {
        subscriptionState.setFailedTask(this);
      }
    }
    if (subscriptionState != null) { // May have failed trying to get subscriptionState
      parent.finishRead(subscriptionState);
    }
    try {
      callback.onCompletion((e == null) ? messages : null, e);
    } catch (Throwable t) {
      // This protects the worker thread from any issues with the callback code. Nothing to be
      // done here but log it since it indicates a bug in the calling code.
      log.error("Consumer read callback threw an unhandled exception", e);
    }
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
  public List<AbstractConsumerRecord<ClientK, ClientV>> get()
      throws InterruptedException, ExecutionException {
    finished.await();
    return messages;
  }

  @Override
  public List<AbstractConsumerRecord<ClientK, ClientV>> get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    finished.await(timeout, unit);
    if (finished.getCount() > 0) {
      throw new TimeoutException();
    }
    return messages;
  }
}
