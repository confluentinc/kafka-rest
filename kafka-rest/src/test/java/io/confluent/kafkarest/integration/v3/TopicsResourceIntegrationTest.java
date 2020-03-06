package io.confluent.kafkarest.integration.v3;

import io.confluent.kafkarest.integration.ClusterTestHarness;
import org.junit.Test;

public class TopicsResourceIntegrationTest extends ClusterTestHarness {

  public TopicsResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listTopics_existingCluster_returnsTopic() {
    // todo still working on integration test
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    String expected = "{"

        + "}";

  }

}
