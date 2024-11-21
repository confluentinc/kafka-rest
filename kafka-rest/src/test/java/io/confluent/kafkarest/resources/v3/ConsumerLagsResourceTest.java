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

package io.confluent.kafkarest.resources.v3;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.controllers.ConsumerLagManager;
import io.confluent.kafkarest.entities.ConsumerLag;
import io.confluent.kafkarest.entities.v3.ConsumerLagData;
import io.confluent.kafkarest.entities.v3.ConsumerLagDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerLagResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerLagsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class ConsumerLagsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String CONSUMER_GROUP_ID = "consumer-group-1";
  private static final String TOPIC = "topic-1";

  private static final ConsumerLag CONSUMER_LAG_1 =
      ConsumerLag.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setTopicName(TOPIC)
          .setPartitionId(1)
          .setConsumerId("consumer-1")
          .setInstanceId(Optional.of("instance-1"))
          .setClientId("client-1")
          .setCurrentOffset(100L)
          .setLogEndOffset(101L)
          .build();

  private static final ConsumerLag CONSUMER_LAG_2 =
      ConsumerLag.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setTopicName(TOPIC)
          .setPartitionId(2)
          .setConsumerId("consumer-2")
          .setInstanceId(Optional.of("instance-2"))
          .setClientId("client-2")
          .setCurrentOffset(100L)
          .setLogEndOffset(200L)
          .build();

  private static final List<ConsumerLag> CONSUMER_LAG_LIST =
      Arrays.asList(CONSUMER_LAG_1, CONSUMER_LAG_2);

  @Mock private ConsumerLagManager consumerLagManager;

  private ConsumerLagsResource consumerLagsResource;

  @BeforeEach
  public void setUp() {
    consumerLagsResource =
        new ConsumerLagsResource(
            () -> consumerLagManager, new CrnFactoryImpl(""), new FakeUrlFactory());
  }

  @Test
  public void listConsumerLags_returnsConsumerLags() {
    expect(consumerLagManager.listConsumerLags(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(CONSUMER_LAG_LIST));
    replay(consumerLagManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerLagsResource.listConsumerLags(response, CLUSTER_ID, CONSUMER_GROUP_ID);

    ListConsumerLagsResponse expected =
        ListConsumerLagsResponse.create(
            ConsumerLagDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/consumer-groups/consumer-group-1/lags")
                        .build())
                .setData(
                    Arrays.asList(
                        ConsumerLagData.fromConsumerLag(CONSUMER_LAG_2)
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1/"
                                            + "lags/topic-1/partitions/2")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-1/"
                                            + "lag=topic-1/partition=2")
                                    .build())
                            .build(),
                        ConsumerLagData.fromConsumerLag(CONSUMER_LAG_1)
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1/"
                                            + "lags/topic-1/partitions/1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-1/"
                                            + "lag=topic-1/partition=1")
                                    .build())
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getConsumerLag_returnsConsumerLag() {
    expect(consumerLagManager.getConsumerLag(CLUSTER_ID, CONSUMER_GROUP_ID, TOPIC, 1))
        .andReturn(completedFuture(Optional.of(CONSUMER_LAG_1)));
    replay(consumerLagManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerLagsResource.getConsumerLag(response, CLUSTER_ID, CONSUMER_GROUP_ID, TOPIC, 1);

    GetConsumerLagResponse expected =
        GetConsumerLagResponse.create(
            ConsumerLagData.fromConsumerLag(CONSUMER_LAG_1)
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/consumer-groups/consumer-group-1/"
                                + "lags/topic-1/partitions/1")
                        .setResourceName(
                            "crn:///kafka=cluster-1/consumer-group=consumer-group-1/"
                                + "lag=topic-1/partition=1")
                        .build())
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getConsumerLag_nonExistingConsumerLag_throwsNotFound() {
    expect(consumerLagManager.getConsumerLag(CLUSTER_ID, CONSUMER_GROUP_ID, TOPIC, 1))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerLagManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerLagsResource.getConsumerLag(response, CLUSTER_ID, CONSUMER_GROUP_ID, TOPIC, 1);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
