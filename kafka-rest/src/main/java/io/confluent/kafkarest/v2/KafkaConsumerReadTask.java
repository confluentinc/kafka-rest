/*
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

import io.confluent.kafkarest.ConsumerReadCallback;
import io.confluent.kafkarest.Errors;
import io.confluent.rest.exceptions.RestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Vector;

import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;

/**
 * State for tracking the progress of a single consumer read request.
 *
 * <p>To support embedded formats that require translation between the format deserialized
 * by the Kafka decoder and the format returned in the ConsumerRecord entity sent back to
 * the client, this class uses two pairs of key-value generic type parameters: KafkaK/KafkaV
 * is the format returned by the Kafka consumer's decoder/deserializer, ClientK/ClientV is
 * the format returned to the client in the HTTP response. In some cases these may be identical.
 */
class KafkaConsumerReadTask<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerReadTask.class);

  private KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> parent;
  private final long requestTimeoutMs;
  private final long maxResponseBytes;
  private final ConsumerReadCallback<ClientKeyT, ClientValueT> callback;
  private boolean finished;

  private List<ConsumerRecord<ClientKeyT, ClientValueT>> messages;
  private long bytesConsumed = 0;
  private final long started;


  public KafkaConsumerReadTask(
      KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> parent,
      long timeout,
      long maxBytes,
      ConsumerReadCallback<ClientKeyT, ClientValueT> callback
  ) {
    this.parent = parent;
    this.maxResponseBytes =
        Math.min(
            maxBytes,
            parent.getConfig().getLong(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG)
        );
    long defaultRequestTimeout =
        parent.getConfig().getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
    this.requestTimeoutMs =
        timeout <= 0 ? defaultRequestTimeout : Math.min(timeout, defaultRequestTimeout);
    this.callback = callback;
    this.finished = false;

    started = parent.getConfig().getTime().milliseconds();
  }

  /**
   * Performs one iteration of reading from a consumer iterator.
   */
  public void doPartialRead() {
    try {
      // Initial setup requires locking, which must be done on this thread.
      if (messages == null) {
        messages = new Vector<>();
      }

      long roughMsgSize = 0;

      while (parent.hasNext()) {
        ConsumerRecordAndSize<ClientKeyT, ClientValueT> recordAndSize =
            parent.createConsumerRecord(parent.peek());
        roughMsgSize = recordAndSize.getSize();
        if (bytesConsumed + roughMsgSize >= maxResponseBytes) {
          break;
        }

        messages.add(recordAndSize.getRecord());
        parent.next();
        bytesConsumed += roughMsgSize;
      }

      log.trace(
          "KafkaConsumerReadTask exiting read with id={} messages={} bytes={}, backing off if not"
          + " complete",
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
        log.trace(
            "Finishing KafkaConsumerReadTask id={} requestTimedOut={} exceededMaxResponseBytes={}",
            this,
            requestTimedOut,
            exceededMaxResponseBytes
        );
        finish();
      }
    } catch (Exception e) {
      finish(e);
      log.error("Unexpected exception in consumer read task id={} ", this, e);
    }
  }

  public boolean isDone() {
    return finished;
  }

  void finish() {
    finish(null);
  }

  private void finish(Exception e) {
    log.trace("Finishing KafkaConsumerReadTask id={}", this, e);
    try {
      if (e != null && !(e instanceof RestException)) {
        e = Errors.kafkaErrorException(e);
      }
      callback.onCompletion((e == null) ? messages : null, (RestException) e);
    } catch (Throwable t) {
      // This protects the worker thread from any issues with the callback code. Nothing to be
      // done here but log it since it indicates a bug in the calling code.
      log.error("Consumer read callback threw an unhandled exception id={}", this, e);
    }
    finished = true;
  }
}
