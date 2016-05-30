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

import org.apache.kafka.clients.consumer.Consumer;

/**
 * Wraps state of assigned Consumer to a single topic partition.
 * The consumer may be automatically released if opened in a
 * try-with-resources block.
 */
public class TPConsumerState implements AutoCloseable {

  private Consumer<byte[], byte[]> consumer;
  private SimpleConsumerPool ownerPool;
  private String clientId;

  public TPConsumerState(Consumer<byte[], byte[]> consumer, SimpleConsumerPool ownerPool, String clientId) {
    this.consumer = consumer;
    this.ownerPool = ownerPool;
    this.clientId = clientId;
  }

  public String clientId() {
    return clientId;
  }

  public Consumer<byte[], byte[]> consumer() {
    return consumer;
  }

  public void close() throws Exception {
    // release partition
    consumer.unsubscribe();
    ownerPool.release(this);
  }

}
