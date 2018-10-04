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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.rest.exceptions.RestException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;

/**
 * State for tracking the progress of a single consumer read request.
 *
 * <p>To support embedded formats that require translation between the format deserialized by the
 * Kafka decoder and the format returned in the ConsumerRecord entity sent back to the client,
 * this class uses two pairs of key-value generic type parameters: KafkaK/KafkaV is the format
 * returned by the Kafka consumer's decoder/deserializer, ClientK/ClientV is the format
 * returned to the client in the HTTP response. In some cases these may be identical.
 */
class ConsumerReadTask<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>
        implements Future<List<ConsumerRecord<ClientKeyT, ClientValueT>>> {

  private static final Logger log = LoggerFactory.getLogger(ConsumerReadTask.class);

  private ConsumerState parent;
  private final long maxResponseBytes;
  private final ConsumerReadCallback<ClientKeyT, ClientValueT> callback;
  private CountDownLatch finished;

  private ConsumerTopicState topicState;
  private ConsumerIterator<KafkaKeyT, KafkaValueT> iter;
  private List<ConsumerRecord<ClientKeyT, ClientValueT>> messages;
  private long bytesConsumed = 0;
  private final long started;

  public ConsumerReadTask(
          ConsumerState parent,
          String topic,
          long maxBytes,
          ConsumerReadCallback<ClientKeyT, ClientValueT> callback
  ) {
    this.parent = parent;
    this.maxResponseBytes = Math.min(
            maxBytes,
            parent.getConfig().getLong(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG)
    );
    this.callback = callback;
    this.finished = new CountDownLatch(1);

    started = parent.getConfig().getTime().milliseconds();
    try {
      topicState = parent.getOrCreateTopicState(topic);

      // If the previous call failed, restore any outstanding data into this task.
      ConsumerReadTask previousTask = topicState.clearFailedTask();
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
   */
  public void doPartialRead() {
    try {
      // Initial setup requires locking, which must be done on this thread.
      if (iter == null) {
        parent.startRead(topicState);
        iter = topicState.getIterator();

        messages = new Vector<ConsumerRecord<ClientKeyT, ClientValueT>>();
      }

      long roughMsgSize = 0;

      final int requestTimeoutMs = parent.getConfig().getInt(
              KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
      try {
        // Read off as many messages as we can without triggering a timeout exception. The
        // consumer timeout should be set very small, so the expectation is that even in the
        // worst case, num_messages * consumer_timeout << request_timeout, so it's safe to only
        // check the elapsed time once this loop finishes.
        while (iter.hasNext()) {
          MessageAndMetadata<KafkaKeyT, KafkaValueT> msg = iter.peek();
          ConsumerRecordAndSize<ClientKeyT, ClientValueT> recordAndSize =
                  parent.createConsumerRecord(msg);
          roughMsgSize = recordAndSize.getSize();
          if (bytesConsumed + roughMsgSize >= maxResponseBytes) {
            break;
          }

          iter.next();
          messages.add(recordAndSize.getRecord());
          bytesConsumed += roughMsgSize;
          // Updating the consumed offsets isn't done until we're actually going to return the
          // data since we may encounter an error during a subsequent read, in which case we'll
          // have to defer returning the data so we can return an HTTP error instead
        }
      } catch (ConsumerTimeoutException cte) {
        log.trace("ConsumerReadTask timed out, using backoff id={}", this);
      }

      log.trace(
              "ConsumerReadTask exiting read with id={} messages={} bytes={}",
              this,
              messages.size(),
              bytesConsumed
      );

      long now = parent.getConfig().getTime().milliseconds();
      long elapsed = now - started;

      // Including the rough message size here ensures processing finishes if the next
      // message exceeds the maxResponseBytes
      boolean requestTimedOut = elapsed >= requestTimeoutMs;
      boolean exceededMaxResponseBytes = bytesConsumed + roughMsgSize >= maxResponseBytes;
      if (requestTimedOut || exceededMaxResponseBytes) {
        log.trace("Finishing ConsumerReadTask id={} requestTimedOut={} exceededMaxResponseBytes={}",
                this, requestTimedOut, exceededMaxResponseBytes
        );
        finish();
      }
    } catch (Exception e) {
      if (!(e instanceof RestException)) {
        e = Errors.kafkaErrorException(e);
      }
      finish((RestException) e);
      log.error("Unexpected exception in consumer read task id={} ", this, e);
    }
  }

  public void finish() {
    finish(null);
  }

  public void finish(RestException e) {
    log.trace("Finishing ConsumerReadTask id={} exception={}", this, e);
    if (e == null) {
      // Now it's safe to mark these messages as consumed by updating offsets since we're actually
      // going to return the data.
      Map<Integer, Long> consumedOffsets = topicState.getConsumedOffsets();
      for (ConsumerRecord<ClientKeyT, ClientValueT> msg : messages) {
        consumedOffsets.put(msg.getPartition(), msg.getOffset());
      }
    } else {
      // If we read any messages before the exception occurred, keep this task so we don't lose
      // messages. Subsequent reads will add the outstanding messages before attempting to read
      // any more from the consumer stream iterator
      if (topicState != null && messages != null && messages.size() > 0) {
        log.trace("Saving failed ConsumerReadTask for subsequent call id={}", this, e);
        topicState.setFailedTask(this);
      }
    }
    if (topicState != null) { // May have failed trying to get topicState
      parent.finishRead(topicState);
    }
    try {
      callback.onCompletion((e == null) ? messages : null, e);
    } catch (Throwable t) {
      // This protects the worker thread from any issues with the callback code. Nothing to be
      // done here but log it since it indicates a bug in the calling code.
      log.error("Consumer read callback threw an unhandled exception id={}", this, e);
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
  public List<ConsumerRecord<ClientKeyT, ClientValueT>> get()
          throws InterruptedException, ExecutionException {
    finished.await();
    return messages;
  }

  @Override
  public List<ConsumerRecord<ClientKeyT, ClientValueT>> get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
    finished.await(timeout, unit);
    if (finished.getCount() > 0) {
      throw new TimeoutException();
    }
    return messages;
  }
}
