package io.confluent.kafkarest.integration.v3;

import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Test;

public class TopicsResourceIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC = "topic-1";

  public TopicsResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listTopics_existingCluster_returnsTopics() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic(TOPIC, 1, (short) 1);

//    String expected = "{"
//        + "\"links\":{"
//        + "\"self\":\"" + baseUrl + "/v3/clusters/" + clusterId + "/topics" + "\","
//        + "\"next\":null"
//        + "},"
//        + "\"data\":["
//        +   "{"
//        +   "\"links\":{"
//        +   "\"self\":\"" + baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC + "\""
//        +   "},"
//        +   "\"attributes\":{"
//        +     "\"topic_name_\":\"" + TOPIC + "\","
//        +     "\"cluster_id\":\"" + clusterId + "\","
//        +     "\"is_internal\":\"" + false + "\","
//        +     "\"replication_factor\":\"" + 3 + "\","
//        +   "},"
//        + "\"relationships\":{"
//        +   "\"configs\":{"
//        +   "\"links\":{"
//        +   "\"related\":\""
//        + baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC + "/configs" + "\""
//        + "}"
//        + "},"
//        + "\"partitions\":{"
//        + "\"links\":{"
//        + "\"related\":\"" + baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC
//        + "/partitions" + "\""
//        + "}"
//        + "}"
//        + "},"
//        + "\"id\":\"" + clusterId + "\","
//        + "\"type\":\"KafkaCluster\""
//        + "}"
//        + "]}";

    Response response =
        request("/v3/clusters/" + clusterId + "/topics").accept(Versions.JSON_API).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
//    assertEquals(expected, response.readEntity(String.class));

  }

}
