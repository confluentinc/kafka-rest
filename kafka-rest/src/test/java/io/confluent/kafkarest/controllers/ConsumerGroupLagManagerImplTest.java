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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.confluent.kafkarest.entities.ConsumerGroupLag;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.IsolationLevel;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerGroupLagManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String CONSUMER_GROUP_ID = "consumer_group_1";

  private static final ConsumerGroupLag CONSUMER_GROUP_LAG =
      ConsumerGroupLag.builder()
      .setClusterId(CLUSTER_ID)
      .setConsumerGroupId(CONSUMER_GROUP_ID)
      .setMaxLag(100L)
      .setTotalLag(101L)
      .setMaxLagConsumerId("consumer-1")
      .setMaxLagClientId("client-1")
      .setMaxLagTopicName("topic-1")
      .setMaxLagPartitionId(1)
      .build();

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ConsumerGroupManager consumerGroupManager;

  @Mock
  private Admin kafkaAdminClient;

  private ConsumerGroupLagManagerImpl consumerGroupLagManager;

  @Before
  public void setUp() {
    consumerGroupLagManager = new ConsumerGroupLagManagerImpl(kafkaAdminClient, consumerGroupManager);
  }

//  @Test
//  public void getConsumerGroupLag_returnsConsumerGroupLag() throws Exception {
//    expect(consumerOffsetsDao.getConsumerGroupOffsets(
//        CLUSTER_ID, CONSUMER_GROUP_ID, IsolationLevel.READ_COMMITTED))
//        .andReturn(CONSUMER_GROUP_LAG);
//    replay(consumerOffsetsDao);
//
//    ConsumerGroupLag consumerGroupLag =
//        consumerGroupLagManager.getConsumerGroupLag(CLUSTER_ID, CONSUMER_GROUP_ID).get().get();
//    assertEquals(CONSUMER_GROUP_LAG, consumerGroupLag);
//  }
//
//  @Test
//  public void getConsumerGroupLag_nonExistingConsumerGroupLag_returnsEmpty() throws Exception {
//    expect(consumerOffsetsDao.getConsumerGroupOffsets(
//        CLUSTER_ID, CONSUMER_GROUP_ID, IsolationLevel.READ_COMMITTED))
//        .andReturn(null);
//    replay(consumerOffsetsDao);
//
//    Optional<ConsumerGroupLag> consumerGroupLag = consumerGroupLagManager.getConsumerGroupLag(
//        CLUSTER_ID, CONSUMER_GROUP_ID).get();
//
//    assertFalse(consumerGroupLag.isPresent());
//  }
}
