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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.confluent.kafkarest.entities.ConsumerLag;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.IsolationLevel;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerLagManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String CONSUMER_GROUP_ID = "consumer_group_1";

  private static final ConsumerLag CONSUMER_LAG_1 =
      ConsumerLag.builder()
      .setClusterId(CLUSTER_ID)
      .setConsumerGroupId(CONSUMER_GROUP_ID)
      .setTopicName("topic-1")
      .setPartitionId(1)
      .setConsumerId("consumer-1")
      .setInstanceId("instance-1")
      .setClientId("client-1")
      .setCurrentOffset(100L)
      .setLogEndOffset(101L)
      .build();

  private static final ConsumerLag CONSUMER_LAG_2 =
      ConsumerLag.builder()
      .setClusterId(CLUSTER_ID)
      .setConsumerGroupId(CONSUMER_GROUP_ID)
      .setTopicName("topic-1")
      .setPartitionId(2)
      .setConsumerId("consumer-2")
      .setInstanceId("instance-2")
      .setClientId("client-2")
      .setCurrentOffset(100L)
      .setLogEndOffset(200L)
      .build();

  private static final List<ConsumerLag> CONSUMER_LAG_LIST = Arrays
      .asList(CONSUMER_LAG_1, CONSUMER_LAG_2);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ConsumerOffsetsDao consumerOffsetsDao;

  private ConsumerLagManagerImpl consumerLagManager;

  @Before
  public void setUp() {
    consumerLagManager = new ConsumerLagManagerImpl(consumerOffsetsDao);
  }

  @Test
  public void listConsumerLags_returnsConsumerLags() throws Exception {
    expect(consumerOffsetsDao.getConsumerLags(
        CLUSTER_ID, CONSUMER_GROUP_ID, IsolationLevel.READ_COMMITTED))
        .andReturn(CONSUMER_LAG_LIST);
    replay(consumerOffsetsDao);

    List<ConsumerLag> consumerLagList =
        consumerLagManager.listConsumerLags(CLUSTER_ID, CONSUMER_GROUP_ID).get();
    assertEquals(CONSUMER_LAG_LIST, consumerLagList);
  }

  @Test
  public void getConsumerLag_returnsConsumerLag() throws Exception {
    expect(consumerOffsetsDao.getConsumerLags(
        CLUSTER_ID, CONSUMER_GROUP_ID, IsolationLevel.READ_COMMITTED))
        .andReturn(CONSUMER_LAG_LIST);
    replay(consumerOffsetsDao);

    ConsumerLag consumerLag =
        consumerLagManager.getConsumerLag(
            CLUSTER_ID, "topic-1", 2, CONSUMER_GROUP_ID)
            .get()
            .get();

    assertEquals(CONSUMER_LAG_2, consumerLag);
  }

  @Test
  public void getConsumerLag_nonExistingConsumerLag_returnsEmpty() throws Exception {
    expect(consumerOffsetsDao.getConsumerLags(
       CLUSTER_ID, CONSUMER_GROUP_ID, IsolationLevel.READ_COMMITTED))
        .andReturn(new ArrayList<>());
    replay(consumerOffsetsDao);

    Optional<ConsumerLag> consumerLag =
        consumerLagManager.getConsumerLag(
            CLUSTER_ID, "topic-1", 2, CONSUMER_GROUP_ID)
            .get();

    assertFalse(consumerLag.isPresent());
  }
}
