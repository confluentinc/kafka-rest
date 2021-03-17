package io.confluent.kafkarest.integration.accesslist;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import javax.ws.rs.core.Response.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ResourceAllowlistTest extends ResourceAccesslistTestBase {

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put("api.endpoints.allowlist", "api.v3.topics.*, api.v3.clusters.list");
    restProperties.put("api.endpoints.blocklist", "");
  }

  @Test
  public void testAllowlist() {
    // Even though the checks are not exactly independent (i.e. topic deletion should be tried
    // after topic creation), all of them are executed in a single test, as: (1) they are touching
    // different API endpoints, for which we don't need state reset (on the contrary); (2) failures
    // can easily be correlated to a check; and (3) running only one integration test method saves
    // a significant amount of time.
    allowlistEnablesResourceClass();
    allowlistEnablesResourceMethod();
    nonAllowlistResourcesDisabled();
  }

  private void allowlistEnablesResourceClass() {
    assertEquals(Status.OK.getStatusCode(), listTopics().getStatus());
    assertEquals(Status.CREATED.getStatusCode(), createTopic().getStatus());
    assertEquals(Status.OK.getStatusCode(), getTopic().getStatus());
    assertEquals(Status.NO_CONTENT.getStatusCode(), deleteTopic().getStatus());
  }

  private void allowlistEnablesResourceMethod() {
    assertEquals(Status.OK.getStatusCode(), listClusters().getStatus());
  }

  private void nonAllowlistResourcesDisabled() {
    assertEquals(Status.NOT_FOUND.getStatusCode(), getCluster().getStatus());
    assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(), updateClusterConfig().getStatus());
  }
}
