/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.Collections.emptySet;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafkarest.controllers.PartitionManager;
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v3.ListPartitionOffsetsResponse;
import io.confluent.kafkarest.entities.v3.PartitionWithOffsetsData;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import io.confluent.rest.exceptions.RestNotFoundException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class ListPartitionOffsetsActionTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";

  private static final Partition PARTITION_1 =
      Partition.create(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 0,
          Arrays.asList(
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 1,
                  /* isLeader= */ true,
                  /* isInSync= */ false),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 2,
                  /* isLeader= */ false,
                  /* isInSync= */ true),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 3,
                  /* isLeader= */ false,
                  /* isInSync= */ false)),
          10L,
          100L);
  private static final Partition PARTITION_2 =
      Partition.create(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 1,
          Arrays.asList(
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 1,
                  /* brokerId= */ 2,
                  /* isLeader= */ true,
                  /* isInSync= */ false),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 1,
                  /* brokerId= */ 3,
                  /* isLeader= */ false,
                  /* isInSync= */ true),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 1,
                  /* brokerId= */ 1,
                  /* isLeader= */ false,
                  /* isInSync= */ false)));
  private static final Partition PARTITION_3 =
      Partition.create(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 2,
          Arrays.asList(
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 2,
                  /* brokerId= */ 3,
                  /* isLeader= */ true,
                  /* isInSync= */ false),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 2,
                  /* brokerId= */ 1,
                  /* isLeader= */ false,
                  /* isInSync= */ true),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 2,
                  /* brokerId= */ 2,
                  /* isLeader= */ false,
                  /* isInSync= */ false)));

  private static final Topic TOPIC =
      Topic.create(
          CLUSTER_ID,
          TOPIC_NAME,
          Arrays.asList(PARTITION_1, PARTITION_2, PARTITION_3),
          /* replicationFactor= */ (short) 3,
          /* isInternal= */ false,
          /* authorizedOperations= */ emptySet());
  @Mock private Admin adminClient;
  @Mock private TopicManager topicManager;
  @Mock private PartitionManager partitionManager;

  @Mock private ListOffsetsResult earliestResult;

  @Mock private ListOffsetsResult.ListOffsetsResultInfo earliestResultInfo1;

  @Mock private ListOffsetsResult latestResult;

  @Mock private ListOffsetsResult.ListOffsetsResultInfo latestResultInfo1;

  private ListPartitionOffsetsAction listPartitionOffsetsAction;

  @BeforeEach
  public void setUp() {
    listPartitionOffsetsAction =
        new ListPartitionOffsetsAction(
            () -> partitionManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listPartitionOffsets_existingPartitionWithOffsets_returnsEarliestAndLatestOffsets() {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_NAME))
        .andReturn(CompletableFuture.completedFuture(Optional.of(TOPIC)));
    expect(adminClient.listOffsets(anyObject(), anyObject())).andReturn(earliestResult);
    expect(earliestResult.partitionResult(toTopicPartition(PARTITION_1)))
        .andReturn(KafkaFuture.completedFuture(earliestResultInfo1));
    expect(earliestResultInfo1.offset()).andReturn(PARTITION_1.getEarliestOffset());
    expect(adminClient.listOffsets(anyObject(), anyObject())).andReturn(latestResult);
    expect(latestResult.partitionResult(toTopicPartition(PARTITION_1)))
        .andReturn(KafkaFuture.completedFuture(latestResultInfo1));
    expect(latestResultInfo1.offset()).andReturn(PARTITION_1.getLatestOffset());

    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()))
        .andReturn(CompletableFuture.completedFuture(Optional.of(PARTITION_1)));
    replay(
        adminClient,
        earliestResult,
        earliestResultInfo1,
        latestResult,
        latestResultInfo1,
        topicManager,
        partitionManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    listPartitionOffsetsAction.listPartitionOffsets(
        response, CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId(), "earliest_and_latest");

    ListPartitionOffsetsResponse expected =
        ListPartitionOffsetsResponse.create(
            PartitionWithOffsetsData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions/0/offset")
                        .setResourceName(
                            "crn:///kafka=cluster-1/topic=topic-1/partition=0/"
                                + "offset_type=earliest_and_latest")
                        .build())
                .setClusterId(CLUSTER_ID)
                .setTopicName(TOPIC_NAME)
                .setPartitionId(PARTITION_1.getPartitionId())
                .setEarliestOffset(PARTITION_1.getEarliestOffset())
                .setLatestOffset(PARTITION_1.getLatestOffset())
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listPartitionOffsets_invalidOffsetType_throwsBadRequestException() {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()))
        .andReturn(CompletableFuture.completedFuture(Optional.of(PARTITION_1)));
    FakeAsyncResponse response = new FakeAsyncResponse();

    assertThrows(
        BadRequestException.class,
        () ->
            listPartitionOffsetsAction.listPartitionOffsets(
                response, CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId(), "foobar"));
  }

  @Test
  public void listPartitionOffsets_nonExistingPartition_throwsPartitionNotFound() {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()))
        .andReturn(CompletableFuture.completedFuture(Optional.empty()));
    replay(partitionManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    listPartitionOffsetsAction.listPartitionOffsets(
        response, CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId(), "earliest_and_latest");

    assertEquals(RestNotFoundException.class, response.getException().getClass());
  }

  @Test
  public void listPartitionOffsets_nonExistingTopicOrCluster_throwsNotFound() {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(partitionManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    listPartitionOffsetsAction.listPartitionOffsets(
        response, CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId(), "earliest_and_latest");

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  private static TopicPartition toTopicPartition(Partition partition) {
    return new TopicPartition(partition.getTopicName(), partition.getPartitionId());
  }
}
