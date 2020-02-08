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

import io.confluent.kafkarest.v2.KafkaConsumerManager;

public interface KafkaRestContext {
  KafkaRestConfig getConfig();

  ProducerPool getProducerPool();

  @Deprecated
  ScalaConsumersContext getScalaConsumersContext();

  @Deprecated
  ConsumerManager getConsumerManager();

  @Deprecated
  SimpleConsumerManager getSimpleConsumerManager();

  KafkaConsumerManager getKafkaConsumerManager();

  AdminClientWrapper getAdminClientWrapper();

  GroupMetadataObserver getGroupMetadataObserver();

  void shutdown();
}
