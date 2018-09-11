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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous worker thread for a consumer
 */
public class ConsumerWorker {

  private static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);

  KafkaRestConfig config;

  public ConsumerWorker(KafkaRestConfig config) {
    this.config = config;
  }

  public <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void readTopic(
      ConsumerState state,
      String topic,
      long maxBytes,
      ConsumerWorkerReadCallback<ClientKeyT, ClientValueT> callback
  ) {
    ConsumerReadTask<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> task =
        new ConsumerReadTask<>(
            state,
            topic,
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
