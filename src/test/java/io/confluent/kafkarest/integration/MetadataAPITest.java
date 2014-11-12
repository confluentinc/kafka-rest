package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.entities.BrokerList;
import io.confluent.kafkarest.entities.Topic;
import kafka.utils.TestUtils;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Tests metadata access against a real cluster. This isn't exhaustive since the unit tests cover corner cases; rather
 * it verifies the basic functionality works against a real cluster.
 */
public class MetadataAPITest extends ClusterTestHarness {
    private static final String topic1Name = "topic1";
    private static final int topic1Partitions = 1;
    private static final Topic topic1 = new Topic(topic1Name, topic1Partitions);
    private static final String topic2Name = "topic2";
    private static final int topic2Partitions = 2;
    private static final Topic topic2 = new Topic(topic2Name, topic2Partitions);

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.createTopic(zkClient, topic1Name, topic1Partitions, 2, JavaConversions.asScalaIterable(this.servers).toSeq(), new Properties());
        TestUtils.createTopic(zkClient, topic2Name, topic2Partitions, 2, JavaConversions.asScalaIterable(this.servers).toSeq(), new Properties());
    }

    @Test
    public void testBrokers() throws InterruptedException {
        // Listing
        final BrokerList brokers = request("/brokers").get(BrokerList.class);
        assertEquals(new BrokerList(Arrays.asList(0, 1, 2)), brokers);
    }

    @Test
    public void testTopicsList() throws InterruptedException {
        // Listing
        final List<Topic> topicsResponse = request("/topics").get(new GenericType<List<Topic>>() {});
        assertEquals(Arrays.asList(topic1, topic2), topicsResponse);

        // Get topic
        final Topic topic1Response = request("/topics/{topic}", "topic", topic1Name).get(Topic.class);
        assertEquals(topic1, topic1Response);

        // Get invalid topic
        final Response invalidResponse = request("/topics/{topic}", "topic", "topicdoesntexist").get();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), invalidResponse.getStatus());
    }
}
