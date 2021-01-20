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
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.controllers.ConsumerOffsetsDaoImpl.MemberId;
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.common.IsolationLevel;

final class ConsumerGroupLagManagerImpl implements ConsumerGroupLagManager {

  private final ConsumerOffsetsDao consumerOffsetsDao;
  private final ClusterManager clusterManager;
  private final IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

  @Inject
  ConsumerGroupLagManagerImpl(
      ConsumerOffsetsDao consumerOffsetsDao,
      ClusterManager clusterManager) {
    this.consumerOffsetsDao = requireNonNull(consumerOffsetsDao);
    this.clusterManager = requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<Optional<ConsumerGroupLag>> getConsumerGroupLag(
      String clusterId,
      String consumerGroupId
  ) {
    return clusterManager.getCluster(clusterId)
        .thenApply(
            cluster ->
                checkEntityExists(cluster, "Cluster %s could not be found.", clusterId))
        .thenCompose(
            cluster ->
                consumerOffsetsDao.getConsumerGroupDescription(consumerGroupId))
        .thenApply(
            cgDesc ->
                cgDesc
                    .orElseThrow(
                        () -> new NotFoundException("Consumer group " +
                            consumerGroupId + " could not be found.")))
        .thenCompose(
            cgDesc ->
                consumerOffsetsDao.getCurrentOffsets(consumerGroupId)
                    .thenCompose(
                        fetchedCurrentOffsets ->
                            consumerOffsetsDao.getLatestOffsets(isolationLevel, fetchedCurrentOffsets)
                                .thenCompose(
                                    latestOffsets ->
                                        consumerOffsetsDao.getMemberIds(cgDesc)
                                            .thenApply(
                                                tpMemberIds -> {
                                                  ConsumerGroupLag.Builder cgOffsets =
                                                      ConsumerGroupLag.builder()
                                                          .setClusterId(clusterId)
                                                          .setConsumerGroupId(consumerGroupId);
                                                  fetchedCurrentOffsets.keySet().stream().forEach(
                                                      tp -> {
                                                        MemberId memberId = tpMemberIds.getOrDefault(
                                                            tp, MemberId.builder()
                                                                .setConsumerId("")
                                                                .setClientId("")
                                                                .setInstanceId(Optional.empty())
                                                                .build());
                                                        long currentOffset = consumerOffsetsDao
                                                            .getCurrentOffset(fetchedCurrentOffsets, tp);
                                                        long latestOffset = consumerOffsetsDao
                                                            .getOffset(latestOffsets, tp);
                                                        // ahu todo: ask about log.debug
                                                        cgOffsets.addOffset(
                                                            tp.topic(),
                                                            memberId.getConsumerId(),
                                                            memberId.getClientId(),
                                                            memberId.getInstanceId(),
                                                            tp.partition(),
                                                            currentOffset,
                                                            latestOffset);
                                                      });
                                                  return Optional.of(cgOffsets.build());
                                                }))));
  }
}