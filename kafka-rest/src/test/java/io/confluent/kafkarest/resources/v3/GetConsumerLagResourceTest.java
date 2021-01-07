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
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ConsumerLagManager;
import io.confluent.kafkarest.entities.ConsumerLag;
import io.confluent.kafkarest.entities.v3.ConsumerLagData;
import io.confluent.kafkarest.entities.v3.GetConsumerLagResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Optional;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GetConsumerLagResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String CONSUMER_GROUP_ID = "consumer-group-1";
  private static final String TOPIC = "topic-1";
  private static final String CONSUMER_ID = "consumer-1";
  private static final String INSTANCE_ID = "instance-1";
  private static final String CLIENT_ID = "client-1";

  private static final ConsumerLag CONSUMER_LAG =
      ConsumerLag.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setTopicName(TOPIC)
          .setPartitionId(1)
          .setConsumerId(CONSUMER_ID)
          .setInstanceId(INSTANCE_ID)
          .setClientId(CLIENT_ID)
          .setCurrentOffset(100L)
          .setLogEndOffset(101L)
          .build();

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ConsumerLagManager consumerLagManager;

  private GetConsumerLagResource consumerLagResource;

  @Before
  public void setUp() {
    consumerLagResource =
        new GetConsumerLagResource(
            () -> consumerLagManager, new CrnFactoryImpl(""), new FakeUrlFactory());
  }

  @Test
  public void getConsumerLag_returnsConsumerLag() {
    expect(consumerLagManager.getConsumerLag(CLUSTER_ID, TOPIC, 1, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(Optional.of(CONSUMER_LAG)));
    replay(consumerLagManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerLagResource.getConsumerLag(response, CLUSTER_ID, TOPIC, 1, CONSUMER_GROUP_ID);

    GetConsumerLagResponse expected =
        GetConsumerLagResponse.create(
            ConsumerLagData.fromConsumerLag(CONSUMER_LAG)
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions/1/lags/consumer-group-1")
                        .setResourceName("crn:///kafka=cluster-1/topic=topic-1/partition=1/lag=consumer-group-1")
                        .build())
                .build());

    assertEquals(expected, response.getValue());

  }

  @Test
  public void getConsumerLag_nonExistingConsumerLag_throwsNotFound() {
    expect(consumerLagManager.getConsumerLag(
        CLUSTER_ID, TOPIC, 1, CONSUMER_GROUP_ID))
            .andReturn(completedFuture(Optional.empty()));
    replay(consumerLagManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerLagResource.getConsumerLag(response, CLUSTER_ID, TOPIC, 1, CONSUMER_GROUP_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
