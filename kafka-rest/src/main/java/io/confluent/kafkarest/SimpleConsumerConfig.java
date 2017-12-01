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

import java.util.Properties;

import kafka.consumer.ConsumerConfig;

public class SimpleConsumerConfig {

  final ConsumerConfig consumerConfig;

  public SimpleConsumerConfig(final Properties originalProperties) {
    final Properties props = (Properties) originalProperties.clone();
    // ConsumerConfig is intended to be used with the HighLevelConsumer. Therefore, it requires
    // some properties
    // to be instantiated that are useless for a SimpleConsumer.
    // We use ConsumerConfig as a basis for SimpleConsumerConfig, because it contains
    // sensible defaults (buffer size, ...).
    props.setProperty("zookeeper.connect", "");
    props.setProperty("group.id", "");
    consumerConfig = new ConsumerConfig(props);
  }

  public int socketTimeoutMs() {
    return consumerConfig.socketTimeoutMs();
  }

  public int socketReceiveBufferBytes() {
    return consumerConfig.socketReceiveBufferBytes();
  }

  public int fetchMessageMaxBytes() {
    return consumerConfig.fetchMessageMaxBytes();
  }

  public int fetchWaitMaxMs() {
    return consumerConfig.fetchWaitMaxMs();
  }

  public int fetchMinBytes() {
    return consumerConfig.fetchMinBytes();
  }
}
