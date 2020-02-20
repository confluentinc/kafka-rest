package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.AdminClientWrapper;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.Topic;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;


import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class AdminClientWrapperTest {
  @Test
  public void testGetTopicOnNullLeaderDoesntThrowNPE() throws Exception {
    Properties props = new Properties();
    props.put(KafkaRestConfig.KAFKACLIENT_INIT_TIMEOUT_CONFIG, "100");
    KafkaRestConfig config = new KafkaRestConfig(props);
    Node controller = new Node(1, "a", 1);

    MockAdminClient adminClient = new MockAdminClient(Arrays.asList(controller, null), controller);
    TopicPartitionInfo partition = new TopicPartitionInfo(1, null, Collections.singletonList(controller), Collections.singletonList(controller));
    adminClient.addTopic(false, "topic", Collections.singletonList(partition), new HashMap<String, String>());

    Topic topic = new AdminClientWrapper(config, adminClient).getTopic("topic");
    assertEquals(topic.getName(), "topic");
  }
}
