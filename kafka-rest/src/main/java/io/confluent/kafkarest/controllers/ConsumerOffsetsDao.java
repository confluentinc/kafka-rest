/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.controllers.ConsumerOffsetsDaoImpl.MemberId;
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import io.confluent.kafkarest.entities.ConsumerLag;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

public interface ConsumerOffsetsDao {

  ConsumerGroupLag getConsumerGroupOffsets(
      String clusterId, String consumerGroupId, IsolationLevel isolationLevel)
      throws InterruptedException, ExecutionException, TimeoutException;

  List<ConsumerLag> getConsumerLags(
      String clusterId, String consumerGroupId, IsolationLevel isolationLevel)
      throws InterruptedException, ExecutionException, TimeoutException;

  CompletableFuture<ConsumerGroupDescription> getConsumerGroupDescription(
      String consumerGroupId);

  CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getCurrentOffsets(
      String consumerGroupId);

  CompletableFuture<Map<TopicPartition, ListOffsetsResultInfo>> getLatestOffsets(
      IsolationLevel isolationLevel,
      CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> currentOffsets);

  CompletableFuture<Map<TopicPartition, MemberId>> getMemberIds(
      CompletableFuture<ConsumerGroupDescription> cgDesc);

  Admin getKafkaAdminClient();
}
