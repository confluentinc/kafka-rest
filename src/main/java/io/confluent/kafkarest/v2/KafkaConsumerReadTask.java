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

import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.ConsumerWorkerReadCallback;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.rest.exceptions.RestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * State for tracking the progress of a single consumer read request.
 *
 * To support embedded formats that require translation between the format deserialized by the Kafka
 * decoder and the format returned in the ConsumerRecord entity sent back to the client, this class
 * uses two pairs of key-value generic type parameters: KafkaK/KafkaV is the format returned by the
 * Kafka consumer's decoder/deserializer, ClientK/ClientV is the format returned to the client in
 * the HTTP response. In some cases these may be identical.
 */
class KafkaConsumerReadTask<KafkaK, KafkaV, ClientK, ClientV>
    implements Future<List<ConsumerRecord<ClientK, ClientV>>> {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerReadTask.class);

  private KafkaConsumerState<KafkaK, KafkaV, ClientK, ClientV> parent;
  private final long requestTimeoutMs;
  private final long maxResponseBytes;
  private final ConsumerWorkerReadCallback<ClientK, ClientV> callback;
  private CountDownLatch finished;

  private Iterator<org.apache.kafka.clients.consumer.ConsumerRecord<ClientK, ClientV>> iter;
  private List<ConsumerRecord<ClientK, ClientV>> messages;
  private long bytesConsumed = 0;
  private final long started;

  // Expiration if this task is waiting, considering both the expiration of the whole task and
  // a single backoff, if one is in progress
  long waitExpiration;

  public KafkaConsumerReadTask(KafkaConsumerState<KafkaK, KafkaV, ClientK, ClientV> parent, String topic, long timeout,
                               long maxBytes, ConsumerWorkerReadCallback<ClientK, ClientV> callback) {
    this.parent = parent;
    this.maxResponseBytes =
        Math.min(maxBytes,
            parent.getConfig().getLong(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG));
    long defaultRequestTimeout = parent.getConfig().getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
    this.requestTimeoutMs = timeout <= 0 ? defaultRequestTimeout : Math.min(timeout, defaultRequestTimeout);
    this.callback = callback;
    this.finished = new CountDownLatch(1);

    started = parent.getConfig().getTime().milliseconds();
  }

  /**
   * Performs one iteration of reading from a consumer iterator.
   *
   * @return true if this read timed out, indicating the scheduler should back off
   */
  public boolean doPartialRead() {
    try {
      // Initial setup requires locking, which must be done on this thread.
      if (messages == null) {
        parent.startRead();
        messages = new Vector<>();
      }

      long roughMsgSize = 0;

      long startedIteration = parent.getConfig().getTime().milliseconds();

      while (parent.hasNext()) {
        ConsumerRecordAndSize<ClientK, ClientV> recordAndSize = parent.createConsumerRecord(parent.peek());
        roughMsgSize = recordAndSize.getSize();
        if (bytesConsumed + roughMsgSize >= maxResponseBytes) {
          break;
        }

        messages.add(recordAndSize.getRecord());
        //inc iterator
        parent.next();
        bytesConsumed += roughMsgSize;
      }

      log.trace("KafkaConsumerReadTask exiting read with id={} messages={} bytes={}, backing off if not complete",
                this, messages.size(), bytesConsumed);

      long now = parent.getConfig().getTime().milliseconds();
      long elapsed = now - started;
      // Compute backoff based on starting time. This makes reasoning about when timeouts
      // should occur simpler for tests.
      int itbackoff
          = parent.getConfig().getInt(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG);
      long backoffExpiration = startedIteration + itbackoff;

      long requestExpiration = started + requestTimeoutMs;
      waitExpiration = Math.min(backoffExpiration, requestExpiration);

      // Including the rough message size here ensures processing finishes if the next
      // message exceeds the maxResponseBytes
      boolean requestTimedOut = elapsed >= requestTimeoutMs;
      boolean exceededMaxResponseBytes = bytesConsumed + roughMsgSize >= maxResponseBytes;
      if (requestTimedOut || exceededMaxResponseBytes) {
        log.trace("Finishing KafkaConsumerReadTask id={} requestTimedOut={} exceededMaxResponseBytes={}",
                this, requestTimedOut, exceededMaxResponseBytes);
        finish();
      }

      return true;
    } catch (Exception e) {
      finish(e);
      log.error("Unexpected exception in consumer read thread: ", e);
      return false;
    }
  }

  void finish() {
    finish(null);
  }

  void finish(Exception e) {
    log.trace("Finishing KafkaConsumerReadTask id={} exception={}", this, e);
    parent.finishRead();
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
  public List<ConsumerRecord<ClientK, ClientV>> get()
      throws InterruptedException, ExecutionException {
    finished.await();
    return messages;
  }

  @Override
  public List<ConsumerRecord<ClientK, ClientV>> get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    finished.await(timeout, unit);
    if (finished.getCount() > 0) {
      throw new TimeoutException();
    }
    return messages;
  }
}
