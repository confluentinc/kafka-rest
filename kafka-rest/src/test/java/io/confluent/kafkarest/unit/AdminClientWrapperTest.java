package io.confluent.kafkarest.unit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.AdminClientWrapper;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.Topic;
import java.util.HashSet;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AdminClientWrapperTest {

  private static final String TOPIC_NAME = "topic";

  private static final Node NODE = new Node(1, "broker-1", 9091);

  private static final TopicDescription TOPIC_WITH_NULL_LEADER =
      new TopicDescription(
          TOPIC_NAME,
          /* internal= */ true,
          singletonList(
              new TopicPartitionInfo(
                  /* partition= */ 0,
                  /* leader= */ null,
                  /* replicas= */ singletonList(NODE),
                  /* isr= */ singletonList(NODE))),
          /* authorizedOperations= */ new HashSet<>());

  private static final ConfigResource CONFIG_RESOURCE =
      new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private AdminClient adminClient;

  @Mock
  private ListTopicsResult listTopicsResult;

  @Mock
  private DescribeTopicsResult describeTopicResult;

  @Mock
  private DescribeConfigsResult describeConfigsResult;

  @Test
  public void testGetTopicOnNullLeaderDoesntThrowNPE() throws Exception {
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(listTopicsResult.names()).andReturn(KafkaFuture.completedFuture(singleton(TOPIC_NAME)));
    expect(adminClient.describeTopics(singletonList(TOPIC_NAME))).andReturn(describeTopicResult);
    expect(describeTopicResult.values())
        .andReturn(singletonMap(TOPIC_NAME, KafkaFuture.completedFuture(TOPIC_WITH_NULL_LEADER)));
    expect(adminClient.describeConfigs(singletonList(CONFIG_RESOURCE)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(CONFIG_RESOURCE, KafkaFuture.completedFuture(new Config(emptyList()))));
    replay(adminClient, listTopicsResult, describeTopicResult, describeConfigsResult);

    Properties props = new Properties();
    props.put(KafkaRestConfig.KAFKACLIENT_INIT_TIMEOUT_CONFIG, "100");
    KafkaRestConfig config = new KafkaRestConfig(props);

    Topic topic = new AdminClientWrapper(config, adminClient).getTopic(TOPIC_NAME);

    assertEquals(topic.getName(), TOPIC_NAME);
  }
}
