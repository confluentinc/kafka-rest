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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafkarest.ConsumerWorkerReadCallback;
import io.confluent.kafkarest.KafkaRestConfig;

/**
 * Synchronous worker thread for a consumer
 */
public class KafkaConsumerWorker {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerWorker.class);

  KafkaRestConfig config;

  public KafkaConsumerWorker(KafkaRestConfig config) {
    this.config = config;
  }

  public <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void readRecords(
      KafkaConsumerState state,
      long timeout,
      long maxBytes,
      ConsumerWorkerReadCallback<ClientKeyT, ClientValueT> callback
  ) {
    KafkaConsumerReadTask<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> task
        = new KafkaConsumerReadTask<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>(
            state,
            timeout,
            maxBytes,
            callback
    );

    log.trace("Executing consumer read task worker={} task={}", this, task);
    while (!task.isDone()) {
      task.doPartialRead();
      long now = config.getTime().milliseconds();
      long waitTime = task.waitExpiration - now;
      if (waitTime > 0) {
        config.getTime().sleep(waitTime);
      }
    }
    log.trace("Finished executing consumer read task worker={} task={}", this, task);
  }
}
