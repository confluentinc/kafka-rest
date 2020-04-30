package io.confluent.kafkarest.integration.v3;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetBrokerConfigResponse;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Assert;
import org.junit.Test;

public class BrokerConfigsResourceIntegrationTest extends ClusterTestHarness {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public BrokerConfigsResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listBrokerConfigs_existingBroker_returnsConfigs() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    String expectedLinks =
        OBJECT_MAPPER.writeValueAsString(
            new CollectionLink(
                baseUrl
                    + "/v3/clusters/" + clusterId
                    + "/brokers/" + brokerId
                    + "/configs",
                /* next= */ null));
    String expectedConfig1 =
        OBJECT_MAPPER.writeValueAsString(
            new BrokerConfigData(
                "crn:///kafka=" + clusterId
                    + "/broker=" + brokerId
                    + "/config=max.connections",
                new ResourceLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/brokers/" + brokerId
                        + "/configs/max.connections"),
                clusterId,
                brokerId,
                "max.connections",
                "2147483647",
                /* isDefault= */ true,
                /* isReadOnly= */ false,
                /* isSensitive= */ false));

    String expectedConfig2 =
        OBJECT_MAPPER.writeValueAsString(
            new BrokerConfigData(
                "crn:///kafka=" + clusterId
                    + "/broker=" + brokerId
                    + "/config=compression.type",
                new ResourceLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/brokers/" + brokerId
                        + "/configs/compression.type"),
                clusterId,
                brokerId,
                "compression.type",
                "producer",
                /* isDefault= */ true,
                /* isReadOnly= */ false,
                /* isSensitive= */ false));

    String expectedConfig3 =
        OBJECT_MAPPER.writeValueAsString(
            new BrokerConfigData(
                "crn:///kafka=" + clusterId
                    + "/broker=" + brokerId
                    + "/config=log.cleaner.threads",
                new ResourceLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/brokers/" + brokerId
                        + "/configs/log.cleaner.threads"),
                clusterId,
                brokerId,
                "log.cleaner.threads",
                "1",
                /* isDefault= */ true,
                /* isReadOnly= */ false,
                /* isSensitive= */ false));

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String responseBody = response.readEntity(String.class);
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedLinks),
        responseBody.contains(expectedLinks));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedConfig1),
        responseBody.contains(expectedConfig1));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedConfig2),
        responseBody.contains(expectedConfig2));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedConfig3),
        responseBody.contains(expectedConfig3));
  }

  @Test
  public void listBrokerConfigs_nonExistingBroker_throwsNotFound() {
    String clusterId = getClusterId();
    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/foobar/configs")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listBrokerConfigs_nonExistingCluster_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs")
            .accept(Versions.JSON_API)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBrokerConfig_existingConfig_returnsConfig() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    String expected =
        OBJECT_MAPPER.writeValueAsString(
            new GetBrokerConfigResponse(
                new BrokerConfigData(
                    "crn:///kafka=" + clusterId
                        + "/broker=" + brokerId
                        + "/config=max.connections",
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/brokers/" + brokerId
                            + "/configs/max.connections"),
                    clusterId,
                    brokerId,
                    "max.connections",
                    "2147483647",
                    /* isDefault= */ true,
                    /* isReadOnly= */ false,
                    /* isSensitive= */ false)));

    Response response =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/max.connections")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(String.class));
  }

  @Test
  public void getBrokerConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs/foobar")
            .accept(Versions.JSON_API)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBrokerConfig_nonExistingBroker_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/foobar/configs/max.connections")
            .accept(Versions.JSON_API)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBrokerConfig_nonExistingCluster_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();
    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs/max.connections")
            .accept(Versions.JSON_API)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateBrokerConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs/foobar")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"producer\"}}}", Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateBrokerConfig_nonExistingBroker_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/foobar/configs/compression.type")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"producer\"}}}", Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateBrokerConfig_nonExistingCluster_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"producer\"}}}", Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateBrokerConfig_nonExistingCluster_noContentType_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs/compression.type")
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"producer\"}}}", Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
