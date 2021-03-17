package io.confluent.kafkarest.integration.accesslist;

import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class ResourceAccesslistTestBase extends ClusterTestHarness {

  private static final String TOPIC_1 = "topic-1";

  public ResourceAccesslistTestBase() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Override
  public Properties overrideBrokerProperties(int i, Properties props) {
    props.put("delete.topic.enable", true);
    return props;
  }

  /*
   * Topic API helper methods
   */

  Response listTopics() {
    return request("/v3/clusters/" + getClusterId() + "/topics")
        .accept(MediaType.APPLICATION_JSON)
        .get();
  }

  Response createTopic() {
    return request("/v3/clusters/" + getClusterId() + "/topics")
        .accept(MediaType.APPLICATION_JSON)
        .post(
            Entity.entity(
                "{\"topic_name\":\"" + TOPIC_1 + "\",\"partitions_count\":3,\"replication_factor\":3}",
                MediaType.APPLICATION_JSON));
  }

  Response getTopic() {
    return request("/v3/clusters/" + getClusterId() + "/topics/" + TOPIC_1)
        .accept(MediaType.APPLICATION_JSON)
        .get();
  }

  Response deleteTopic() {
    return request("/v3/clusters/" + getClusterId() + "/topics/" + TOPIC_1)
        .accept(MediaType.APPLICATION_JSON)
        .delete();
  }

  /*
   * Cluster API helper methods
   */

  Response listClusters() {
    return request("/v3/clusters").accept(MediaType.APPLICATION_JSON).get();
  }

  Response getCluster() {
    return request("/v3/clusters/" + getClusterId()).accept(MediaType.APPLICATION_JSON).get();
  }

  /*
   * Cluster config API helper methods
   */

  Response updateClusterConfig() {
    return request(
        "/v3/clusters/" + getClusterId() + "/broker-configs:alter")
        .accept(MediaType.APPLICATION_JSON)
        .post(
            Entity.entity(
                "{\"data\":["
                    + "{\"name\": \"max.connections\",\"value\":\"1000\"},"
                    + "{\"name\": \"compression.type\",\"value\":\"gzip\"}]}",
                MediaType.APPLICATION_JSON));
  }
}

