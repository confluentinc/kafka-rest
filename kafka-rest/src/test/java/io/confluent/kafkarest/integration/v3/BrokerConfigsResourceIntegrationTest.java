package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.*;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Assert;
import org.junit.Test;

public class BrokerConfigsResourceIntegrationTest extends ClusterTestHarness {

  public BrokerConfigsResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listBrokerConfigs_existingBroker_returnsConfigs() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    CollectionLink expectedLinks =
            new CollectionLink(
                baseUrl
                    + "/v3/clusters/" + clusterId
                    + "/brokers/" + brokerId
                    + "/configs",
                /* next= */ null);

    BrokerConfigData expectedConfig1 =
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
                /* isSensitive= */ false,
                ConfigSource.DEFAULT_CONFIG,
                singletonList(
                    new ConfigSynonymData(
                        "max.connections", "2147483647", ConfigSource.DEFAULT_CONFIG)));

    BrokerConfigData expectedConfig2 =
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
                        "compression.type", "producer", ConfigSource.DEFAULT_CONFIG)));

    BrokerConfigData expectedConfig3 =
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
                /* isSensitive= */ false,
                ConfigSource.DEFAULT_CONFIG,
                singletonList(
                    new ConfigSynonymData(
                        "log.cleaner.threads", "1", ConfigSource.DEFAULT_CONFIG)));

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs")
            .accept(Versions.JSON_API)
            .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokerConfigsResponse actual = response.readEntity(ListBrokerConfigsResponse.class);
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", actual.getLinks(), expectedLinks),
        actual.getData().contains(expectedConfig1));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", actual.getData(), expectedConfig1),
        actual.getData().contains(expectedConfig1));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", actual.getData(), expectedConfig2),
        actual.getData().contains(expectedConfig2));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", actual.getData(), expectedConfig3),
        actual.getData().contains(expectedConfig3));
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
  public void getBrokerConfig_existingConfig_returnsConfig() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    GetBrokerConfigResponse expected =
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
                    /* isSensitive= */ false,
                    ConfigSource.DEFAULT_CONFIG,
                    singletonList(
                        new ConfigSynonymData(
                            "max.connections", "2147483647", ConfigSource.DEFAULT_CONFIG))));

    Response response =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/max.connections")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetBrokerConfigResponse actual = response.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expected, actual);
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
  public void updateAndReset_existingConfig_returnsDefaultUpdatedAndDefaultAgain() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    GetBrokerConfigResponse expectedBeforeUpdate =
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

    Response responseBeforeUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseBeforeUpdate.getStatus());

    GetBrokerConfigResponse actualBeforeUpdate =
            responseBeforeUpdate.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedBeforeUpdate, actualBeforeUpdate);

    Response updateResponse =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"gzip\"}}}", Versions.JSON_API));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    GetBrokerConfigResponse expectedAfterUpdate =
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
                    ConfigSource.DYNAMIC_BROKER_CONFIG,
                    Arrays.asList(
                        new ConfigSynonymData(
                            "compression.type", "gzip", ConfigSource.DYNAMIC_BROKER_CONFIG),
                        new ConfigSynonymData(
                            "compression.type", "producer", ConfigSource.DEFAULT_CONFIG))));

    Response responseAfterUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseAfterUpdate.getStatus());

    GetBrokerConfigResponse actualAfterUpdate =
            responseAfterUpdate.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedAfterUpdate, actualAfterUpdate);

    Response resetResponse =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), resetResponse.getStatus());

    GetBrokerConfigResponse expectedAfterReset =
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

    Response responseAfterReset =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseAfterReset.getStatus());

    GetBrokerConfigResponse actualAfterReset =
            responseAfterReset.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedAfterReset, actualAfterReset);
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

  @Test
  public void resetBrokerConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();
    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs/foobar")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetBrokerConfig_nonExistingBroker_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/foobar/configs/compression.type")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetBrokerConfig_nonExistingCluster_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs/compression.type")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetBrokerConfig_nonExistingCluster_noContentType_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs/compression.type")
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
