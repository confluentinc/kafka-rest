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

import static io.confluent.kafkarest.controllers.Entities.checkEntityExists;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroupLagSummary;
import io.confluent.kafkarest.entities.Partition;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConsumerGroupLagSummaryManagerImpl
    extends AbstractConsumerLagManager implements ConsumerGroupLagSummaryManager {

  private final ConsumerGroupManager consumerGroupManager;
  private static final Logger log =
      LoggerFactory.getLogger(ConsumerGroupLagSummaryManagerImpl.class);

  @Inject
  ConsumerGroupLagSummaryManagerImpl(
      Admin kafkaAdminClient,
      ConsumerGroupManager consumerGroupManager) {
    super(kafkaAdminClient);
    this.consumerGroupManager = requireNonNull(consumerGroupManager);
  }

  @Override
  public CompletableFuture<Optional<ConsumerGroupLagSummary>> getConsumerGroupLagSummary(
      String clusterId,
      String consumerGroupId
  ) {
    return consumerGroupManager.getConsumerGroup(clusterId, consumerGroupId)
        .thenApply(
            consumerGroup ->
                checkEntityExists(consumerGroup,
                    "Consumer Group %s could not be found.", consumerGroupId))
        .thenCompose(
            consumerGroup ->
                getCurrentOffsets(consumerGroupId)
                    .thenApply(
                        fetchedCurrentOffsets ->
                            checkOffsetsExist(
                                fetchedCurrentOffsets,
                                "Consumer group offsets could not be found."))
                    .thenCompose(
                        fetchedCurrentOffsets ->
                            getLatestOffsets(fetchedCurrentOffsets)
                                .thenApply(
                                    latestOffsets ->
                                       Optional.of(createConsumerGroupLagSummary(
                                           clusterId,
                                           consumerGroup,
                                           fetchedCurrentOffsets,
                                           latestOffsets)))));
  }

  private static ConsumerGroupLagSummary createConsumerGroupLagSummary(
      String clusterId,
      ConsumerGroup consumerGroup,
      Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsets,
      Map<TopicPartition, ListOffsetsResultInfo> latestOffsets) {
    Map<Partition, Consumer> partitionAssignment = consumerGroup.getPartitionAssignment();
    ConsumerGroupLagSummary.Builder consumerGroupLagSummary =
        ConsumerGroupLagSummary.builder()
            .setClusterId(clusterId)
            .setConsumerGroupId(consumerGroup.getConsumerGroupId());
    fetchedCurrentOffsets.keySet().forEach(
        topicPartition -> {
          Optional<Consumer> consumer =
              Optional.ofNullable(partitionAssignment.get(
                  Partition.create(
                      clusterId,
                      topicPartition.topic(),
                      topicPartition.partition(),
                      emptyList())));
          Optional<Long> currentOffset =
              getCurrentOffset(fetchedCurrentOffsets, topicPartition);
          Optional<Long> latestOffset =
              getLatestOffset(latestOffsets, topicPartition);
          if (currentOffset.isPresent() && latestOffset.isPresent()) {
            consumerGroupLagSummary.addOffset(
                topicPartition.topic(),
                consumer.map(Consumer::getConsumerId).orElse(""),
                consumer.flatMap(Consumer::getInstanceId),
                consumer.map(Consumer::getClientId).orElse(""),
                topicPartition.partition(),
                currentOffset.get(),
                latestOffset.get());
          } else {
            log.debug("missing offset for consumerId={} topic={} partition={} "
                    + "current={} latest={}",
                consumer.map(Consumer::getConsumerId).orElse(""),
                topicPartition.topic(),
                topicPartition.partition(),
                currentOffset.orElse(null),
                latestOffset.orElse(null));
          }
        });
    return consumerGroupLagSummary.build();
  }
}
