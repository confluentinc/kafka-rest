package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ClusterConfig;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.ClusterConfigData;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.GetBrokerConfigResponse;
import io.confluent.kafkarest.entities.v3.GetClusterConfigResponse;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Assert;
import org.junit.Test;

public class ClusterConfigsResourceIntegrationTest extends ClusterTestHarness {

  public ClusterConfigsResourceIntegrationTest() {
    super(/* numClusters= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listClusterConfigs_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/broker-configs")
            .accept(Versions.JSON_API)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getClusterConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/broker-configs/foobar")
            .accept(Versions.JSON_API)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getClusterConfig_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/broker-configs/max.connections")
            .accept(Versions.JSON_API)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateAndReset_existingConfig_returnsDefaultUpdatedAndDefaultAgain() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    Response responseBeforeUpdate =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), responseBeforeUpdate.getStatus());

    GetBrokerConfigResponse expectedBrokerBeforeUpdate =
        new GetBrokerConfigResponse(
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
                /* isSensitive= */ false,
                ConfigSource.DEFAULT_CONFIG,
                singletonList(
                    new ConfigSynonymData(
                        "compression.type", "producer", ConfigSource.DEFAULT_CONFIG))));

    Response brokerResponseBeforeUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), brokerResponseBeforeUpdate.getStatus());

    GetBrokerConfigResponse actualBrokerBeforeUpdate =
        brokerResponseBeforeUpdate.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedBrokerBeforeUpdate, actualBrokerBeforeUpdate);

    Response updateResponse =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"gzip\"}}}", Versions.JSON_API));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    GetClusterConfigResponse expectedAfterUpdate =
        new GetClusterConfigResponse(
            new ClusterConfigData(
                "crn:///kafka=" + clusterId + "/broker-config=compression.type",
                new ResourceLink(
                    baseUrl + "/v3/clusters/" + clusterId + "/broker-configs/compression.type"),
                clusterId,
                ClusterConfig.Type.BROKER,
                "compression.type",
                "gzip",
                /* isDefault= */ false,
                /* isReadOnly= */ false,
                /* isSensitive= */ false,
                ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
                singletonList(
                    new ConfigSynonymData(
                        "compression.type",
                        "gzip",
                        ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG))));

    Response responseAfterUpdate =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseAfterUpdate.getStatus());

    GetClusterConfigResponse actualAfterUpdate =
        responseAfterUpdate.readEntity(GetClusterConfigResponse.class);
    assertEquals(expectedAfterUpdate, actualAfterUpdate);

    GetBrokerConfigResponse expectedBrokerAfterUpdate =
        new GetBrokerConfigResponse(
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
                "gzip",
                /* isDefault= */ false,
                /* isReadOnly= */ false,
                /* isSensitive= */ false,
                ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
                Arrays.asList(
                    new ConfigSynonymData(
                        "compression.type", "gzip", ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
                    new ConfigSynonymData(
                        "compression.type", "producer", ConfigSource.DEFAULT_CONFIG))));

    Response responseBrokerAfterUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseBrokerAfterUpdate.getStatus());

    GetBrokerConfigResponse actualBrokerAfterUpdate =
        responseBrokerAfterUpdate.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedBrokerAfterUpdate, actualBrokerAfterUpdate);

    Response resetResponse =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), resetResponse.getStatus());

    Response responseAfterReset =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), responseAfterReset.getStatus());

    GetBrokerConfigResponse expectedBrokerAfterReset =
        new GetBrokerConfigResponse(
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
                /* isSensitive= */ false,
                ConfigSource.DEFAULT_CONFIG,
                singletonList(
                    new ConfigSynonymData(
                        "compression.type", "producer", ConfigSource.DEFAULT_CONFIG))));

    Response brokerResponseAfterReset =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), brokerResponseAfterReset.getStatus());

    GetBrokerConfigResponse actualBrokerAfterReset =
        brokerResponseAfterReset.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedBrokerAfterReset, actualBrokerAfterReset);
  }

  @Test
  public void updateClusterConfig_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/broker-configs/compression.type")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"producer\"}}}", Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetClusterConfig_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/broker-configs/compression.type")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
