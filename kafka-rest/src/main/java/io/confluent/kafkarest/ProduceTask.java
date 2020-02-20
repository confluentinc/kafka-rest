/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.ProduceRequest;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container for state associated with one REST-ful produce request, i.e. a batched send
 */
public class ProduceTask {

  private static final Logger log = LoggerFactory.getLogger(ProduceTask.class);

  private final ProduceRequest<?, ?> produceRequest;
  private final int numRecords;
  private final ProducerPool.ProduceRequestCallback callback;
  private int completed;
  private Integer keySchemaId;
  private Integer valueSchemaId;
  private List<RecordMetadataOrException> results;

  public ProduceTask(ProduceRequest<?, ?> produceRequest, int numRecords,
      ProducerPool.ProduceRequestCallback callback) {
    this.produceRequest = produceRequest;
    this.numRecords = numRecords;
    this.callback = callback;
    this.completed = 0;
    this.results = new ArrayList<RecordMetadataOrException>();
  }

  public synchronized Callback createCallback() {
    final int index = results.size();
    // Dummy data, which just helps us keep track of the index & ensures there's a slot for
    // storage when we get the callback
    results.add(null);
    return new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        ProduceTask.this.onCompletion(index, metadata, exception);
      }
    };
  }

  public synchronized void onCompletion(int messageNum, RecordMetadata metadata,
                                        Exception exception) {
    results.set(messageNum, new RecordMetadataOrException(metadata, exception));

    if (exception != null) {
      log.error("Producer error for request " + this.toString(), exception);
    }

    completed += 1;

    if (completed == numRecords) {
      this.callback.onCompletion(keySchemaId, valueSchemaId, results);
    }
  }

  public ProduceRequest<?, ?> getSchemaHolder() {
    return produceRequest;
  }

  public void setSchemaIds(Integer keySchemaId, Integer valueSchemaId) {
    this.keySchemaId = keySchemaId;
    this.valueSchemaId = valueSchemaId;
  }
}
