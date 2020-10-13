package io.confluent.kafkarest.integration;

import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.v3.CreateAclRequest;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ResourceBlocklistFeatureTest extends ClusterTestHarness {

  public ResourceBlocklistFeatureTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put("api.endpoints.blocklist", "api.v3.acls.*,api.v3.topics.create");
  }

  @Test
  public void blocklistDisablesResourceClass() {
    String clusterId = getClusterId();

    Response getResponse =
        request("/v3/clusters/" + clusterId + "/acls").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), getResponse.getStatus());

    Response createResponse =
        request("/v3/clusters/" + clusterId + "/acls")
            .post(
                Entity.entity(
                    CreateAclRequest.builder()
                        .setResourceType(Acl.ResourceType.TOPIC)
                        .setResourceName("*")
                        .setPatternType(Acl.PatternType.LITERAL)
                        .setPrincipal("User:alice")
                        .setHost("*")
                        .setOperation(Acl.Operation.READ)
                        .setPermission(Acl.Permission.ALLOW)
                        .build(),
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(), createResponse.getStatus());

    Response deleteResponse =
        request("/v3/clusters/" + clusterId + "/acls").accept(MediaType.APPLICATION_JSON).delete();
    assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(), deleteResponse.getStatus());
  }

  @Test
  public void blocklistDisablesResourceMethod() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"foobar\",\"partitions_count\":1,\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(), response.getStatus());
  }

  @Test
  public void otherResourceClassesStillEnabled() {
    Response response = request("/v3/clusters").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void otherResourceMethodsStillEnabled() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }
}
