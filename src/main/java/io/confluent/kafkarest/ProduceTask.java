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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafkarest.entities.SchemaHolder;

/**
 * Container for state associated with one REST-ful produce request, i.e. a batched send
 */
class ProduceTask implements Callback {

  private static final Logger log = LoggerFactory.getLogger(ProduceTask.class);

  private final SchemaHolder schemaHolder;
  private final int numRecords;
  private final ProducerPool.ProduceRequestCallback callback;
  private int completed;
  private Map<Integer, Long> partitionOffsets;
  private Exception firstException;
  private Integer keySchemaId;
  private Integer valueSchemaId;

  public ProduceTask(SchemaHolder schemaHolder, int numRecords,
                     ProducerPool.ProduceRequestCallback callback) {
    this.schemaHolder = schemaHolder;
    this.numRecords = numRecords;
    this.callback = callback;
    this.completed = 0;
    this.partitionOffsets = new HashMap<Integer, Long>();
    this.firstException = null;
  }

  @Override
  public synchronized void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      if (firstException == null) {
        firstException = exception;
        log.error("Producer error for request " + this.toString(), exception);
      }
    } else {
      // With a single producer, these should always arrive in order.
      partitionOffsets.put(metadata.partition(), metadata.offset());
    }

    completed += 1;

    if (completed == numRecords) {
      if (firstException != null) {
        this.callback.onException(firstException);
      } else {
        this.callback.onCompletion(keySchemaId, valueSchemaId, partitionOffsets);
      }
    }
  }

  public SchemaHolder getSchemaHolder() {
    return schemaHolder;
  }

  public void setSchemaIds(Integer keySchemaId, Integer valueSchemaId) {
    this.keySchemaId = keySchemaId;
    this.valueSchemaId = valueSchemaId;
  }
}
