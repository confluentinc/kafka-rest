/*
 * Copyright 2021 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import org.apache.kafka.clients.producer.Producer;

/**
 * @deprecated This class only exists to satisfy the {@link KafkaRestContext} interface. It is soon
 *     to be deleted. Access the producer directly, either via {@link
 *     KafkaRestContext#getProducer()} or via injection of {@code Producer<byte[], byte[]>}.
 */
@Deprecated
public final class ProducerPool {

  private final Producer<byte[], byte[]> producer;

  public ProducerPool(Producer<byte[], byte[]> producer) {
    this.producer = requireNonNull(producer);
  }

  public Producer<byte[], byte[]> getProducer() {
    return producer;
  }
}
