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

import io.confluent.kafkarest.controllers.ConsumerGroupLagSummaryManager;
import io.confluent.kafkarest.entities.ConsumerGroupLagSummary;
import io.confluent.kafkarest.entities.v3.ConsumerGroupLagSummaryData;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupLagSummaryResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Optional;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class ConsumerGroupLagSummariesResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String CONSUMER_GROUP_ID = "consumer-group-1";
  private static final String CONSUMER_ID = "consumer-1";
  private static final String CLIENT_ID = "client-1";

  private static final ConsumerGroupLagSummary CONSUMER_GROUP_LAG_1 =
      ConsumerGroupLagSummary.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setMaxLag(100L)
          .setTotalLag(101L)
          .setMaxLagConsumerId(CONSUMER_ID)
          .setMaxLagClientId(CLIENT_ID)
          .setMaxLagTopicName("topic-1")
          .setMaxLagPartitionId(1)
          .build();

  @Mock private ConsumerGroupLagSummaryManager consumerGroupLagSummaryManager;

  private ConsumerGroupLagSummariesResource consumerGroupLagSummariesResource;

  @BeforeEach
  public void setUp() {
    consumerGroupLagSummariesResource =
        new ConsumerGroupLagSummariesResource(
            () -> consumerGroupLagSummaryManager, new CrnFactoryImpl(""), new FakeUrlFactory());
  }

  @Test
  public void getConsumerGroupLagSummary_returnsConsumerGroupLagSummary() {
    expect(consumerGroupLagSummaryManager.getConsumerGroupLagSummary(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(Optional.of(CONSUMER_GROUP_LAG_1)));
    replay(consumerGroupLagSummaryManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerGroupLagSummariesResource.getConsumerGroupLagSummary(
        response, CLUSTER_ID, CONSUMER_GROUP_ID);

    GetConsumerGroupLagSummaryResponse expected =
        GetConsumerGroupLagSummaryResponse.create(
            ConsumerGroupLagSummaryData.fromConsumerGroupLagSummary(CONSUMER_GROUP_LAG_1)
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/consumer-groups/consumer-group-1/lag-summary")
                        .setResourceName(
                            "crn:///kafka=cluster-1/consumer-group=consumer-group-1/lag-summary")
                        .build())
                .setMaxLagConsumer(
                    Relationship.create(
                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1/"
                            + "consumers/consumer-1"))
                .setMaxLagPartition(
                    Relationship.create("/v3/clusters/cluster-1/topics/topic-1/partitions/1"))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getConsumerGroupLagSummary_nonExistingConsumerGroupLagSummary_throwsNotFound() {
    expect(consumerGroupLagSummaryManager.getConsumerGroupLagSummary(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerGroupLagSummaryManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerGroupLagSummariesResource.getConsumerGroupLagSummary(
        response, CLUSTER_ID, CONSUMER_GROUP_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
