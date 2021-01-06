package io.confluent.kafkarest.controllers;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.entities.ConsumerLag;
import java.util.Arrays;
import java.util.List;
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

  private List<ConsumerLag> consumerLagList;

  private ConsumerLag consumerLag2;

  @Before
  public void setUp() {
    consumerLagManager = new ConsumerLagManagerImpl(consumerOffsetsDao);
    consumerLag2 = createMock(ConsumerLag.class);
    consumerLagList = Arrays.asList(
        createMock(ConsumerLag.class),
        consumerLag2
    );
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
        .andReturn(consumerLagList);
    replay(consumerOffsetsDao);

    for (int i = 0; i < CONSUMER_LAG_LIST.size(); i++) {
      expect(consumerLagList.get(i).getTopicName()).andReturn(CONSUMER_LAG_LIST.get(i).getTopicName());
      expect(consumerLagList.get(i).getPartitionId()).andReturn(CONSUMER_LAG_LIST.get(i).getPartitionId());
      expect(consumerLagList.get(i).getConsumerGroupId()).andReturn(CONSUMER_GROUP_ID);
      replay(consumerLagList.get(i));
    }

    expect(consumerLag2.getClusterId()).andReturn(CLUSTER_ID);
    expect(consumerLag2.getConsumerGroupId()).andReturn(CONSUMER_GROUP_ID);
    expect(consumerLag2.getTopicName()).andReturn(CONSUMER_LAG_2.getTopicName());
    expect(consumerLag2.getPartitionId()).andReturn(CONSUMER_LAG_2.getPartitionId());
    expect(consumerLag2.getConsumerId()).andReturn(CONSUMER_LAG_2.getConsumerId());
    expect(consumerLag2.getInstanceId()).andReturn(CONSUMER_LAG_2.getInstanceId());
    expect(consumerLag2.getClientId()).andReturn(CONSUMER_LAG_2.getClientId());
    expect(consumerLag2.getCurrentOffset()).andReturn(CONSUMER_LAG_2.getCurrentOffset());
    expect(consumerLag2.getCurrentOffset()).andReturn(CONSUMER_LAG_2.getLogEndOffset());
    replay(consumerLag2);

    ConsumerLag consumerLag =
        consumerLagManager.getConsumerLag(
            CLUSTER_ID, "topic-1", 2, CONSUMER_GROUP_ID).get().get();
    assertEquals(CONSUMER_LAG_2, consumerLag);
  }
}
