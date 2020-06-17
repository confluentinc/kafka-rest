package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.GetBrokerConfigResponse;
import io.confluent.kafkarest.entities.v3.ListBrokerConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
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

    ResourceCollection.Metadata expectedMetadata =
        ResourceCollection.Metadata.builder()
            .setSelf(
                baseUrl
                    + "/v3/clusters/" + clusterId
                    + "/brokers/" + brokerId
                    + "/configs")
            .build();

    BrokerConfigData expectedConfig1 =
        BrokerConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/brokers/" + brokerId
                            + "/configs/max.connections")
                    .setResourceName(
                        "crn:///kafka=" + clusterId
                            + "/broker=" + brokerId
                            + "/config=max.connections")
                    .build())
            .setClusterId(clusterId)
            .setBrokerId(brokerId)
            .setName("max.connections")
            .setValue("2147483647")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(
                singletonList(
                    ConfigSynonymData.builder()
                        .setName("max.connections")
                        .setValue("2147483647")
                        .setSource(ConfigSource.DEFAULT_CONFIG)
                        .build()))
            .build();

    BrokerConfigData expectedConfig2 =
        BrokerConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(

                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/brokers/" + brokerId
                            + "/configs/compression.type")
                    .setResourceName(
                        "crn:///kafka=" + clusterId
                            + "/broker=" + brokerId
                            + "/config=compression.type")
                    .build())
            .setClusterId(clusterId)
            .setBrokerId(brokerId)
            .setName("compression.type")
            .setValue("producer")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(
                singletonList(
                    ConfigSynonymData.builder()
                        .setName("compression.type")
                        .setValue("producer")
                        .setSource(ConfigSource.DEFAULT_CONFIG)
                        .build()))
            .build();

    BrokerConfigData expectedConfig3 =
        BrokerConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/brokers/" + brokerId
                            + "/configs/log.cleaner.threads")
                    .setResourceName(
                        "crn:///kafka=" + clusterId
                            + "/broker=" + brokerId
                            + "/config=log.cleaner.threads")
                    .build())
            .setClusterId(clusterId)
            .setBrokerId(brokerId)
            .setName("log.cleaner.threads")
            .setValue("1")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(
                singletonList(
                    ConfigSynonymData.builder()
                        .setName("log.cleaner.threads")
                        .setValue("1")
                        .setSource(ConfigSource.DEFAULT_CONFIG)
                        .build()))
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokerConfigsResponse actual = response.readEntity(ListBrokerConfigsResponse.class);
    assertEquals(expectedMetadata, actual.getValue().getMetadata());
    assertTrue(
        String.format(
            "Not true that `%s' contains `%s'.", actual.getValue().getData(), expectedConfig1),
        actual.getValue().getData().contains(expectedConfig1));
    assertTrue(
        String.format(
            "Not true that `%s' contains `%s'.", actual.getValue().getData(), expectedConfig2),
        actual.getValue().getData().contains(expectedConfig2));
    assertTrue(
        String.format(
            "Not true that `%s' contains `%s'.", actual.getValue().getData(), expectedConfig3),
        actual.getValue().getData().contains(expectedConfig3));
  }

  @Test
  public void listBrokerConfigs_nonExistingBroker_throwsNotFound() {
    String clusterId = getClusterId();
    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/foobar/configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listBrokerConfigs_nonExistingCluster_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBrokerConfig_existingConfig_returnsConfig() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    GetBrokerConfigResponse expected =
        GetBrokerConfigResponse.create(
            BrokerConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + brokerId
                                + "/configs/max.connections")
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/broker=" + brokerId
                                + "/config=max.connections")
                        .build())
                .setClusterId(clusterId)
                .setBrokerId(brokerId)
                .setName("max.connections")
                .setValue("2147483647")
                .setDefault(true)
                .setReadOnly(false)
                .setSensitive(false)
                .setSource(ConfigSource.DEFAULT_CONFIG)
                .setSynonyms(
                    singletonList(
                        ConfigSynonymData.builder()
                            .setName("max.connections")
                            .setValue("2147483647")
                            .setSource(ConfigSource.DEFAULT_CONFIG)
                            .build()))
                .build());

    Response response =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/max.connections")
            .accept(MediaType.APPLICATION_JSON)
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
            .accept(MediaType.APPLICATION_JSON)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBrokerConfig_nonExistingBroker_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/foobar/configs/max.connections")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBrokerConfig_nonExistingCluster_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();
    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs/max.connections")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateAndReset_existingConfig_returnsDefaultUpdatedAndDefaultAgain() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    GetBrokerConfigResponse expectedBeforeUpdate =
        GetBrokerConfigResponse.create(
            BrokerConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + brokerId
                                + "/configs/compression.type")
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/broker=" + brokerId
                                + "/config=compression.type")
                        .build())
                .setClusterId(clusterId)
                .setBrokerId(brokerId)
                .setName("compression.type")
                .setValue("producer")
                .setDefault(true)
                .setReadOnly(false)
                .setSensitive(false)
                .setSource(ConfigSource.DEFAULT_CONFIG)
                .setSynonyms(
                    singletonList(
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("producer")
                            .setSource(ConfigSource.DEFAULT_CONFIG)
                            .build()))
                .build());

    Response responseBeforeUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseBeforeUpdate.getStatus());

    GetBrokerConfigResponse actualBeforeUpdate =
        responseBeforeUpdate.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedBeforeUpdate, actualBeforeUpdate);

    Response updateResponse =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"gzip\"}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    GetBrokerConfigResponse expectedAfterUpdate =
        GetBrokerConfigResponse.create(
            BrokerConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + brokerId
                                + "/configs/compression.type")
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/broker=" + brokerId
                                + "/config=compression.type")
                        .build())
                .setClusterId(clusterId)
                .setBrokerId(brokerId)
                .setName("compression.type")
                .setValue("gzip")
                .setDefault(false)
                .setReadOnly(false)
                .setSensitive(false)
                .setSource(ConfigSource.DYNAMIC_BROKER_CONFIG)
                .setSynonyms(
                    Arrays.asList(
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("gzip")
                            .setSource(ConfigSource.DYNAMIC_BROKER_CONFIG)
                            .build(),
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("producer")
                            .setSource(ConfigSource.DEFAULT_CONFIG)
                            .build()))
                .build());

    Response responseAfterUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseAfterUpdate.getStatus());

    GetBrokerConfigResponse actualAfterUpdate =
        responseAfterUpdate.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedAfterUpdate, actualAfterUpdate);

    Response resetResponse =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), resetResponse.getStatus());

    GetBrokerConfigResponse expectedAfterReset =
        GetBrokerConfigResponse.create(
            BrokerConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + brokerId
                                + "/configs/compression.type")
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/broker=" + brokerId
                                + "/config=compression.type")
                        .build())
                .setClusterId(clusterId)
                .setBrokerId(brokerId)
                .setName("compression.type")
                .setValue("producer")
                .setDefault(true)
                .setReadOnly(false)
                .setSensitive(false)
                .setSource(ConfigSource.DEFAULT_CONFIG)
                .setSynonyms(
                    singletonList(
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("producer")
                            .setSource(ConfigSource.DEFAULT_CONFIG)
                            .build()))
                .build());

    Response responseAfterReset =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
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
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"producer\"}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateBrokerConfig_nonExistingBroker_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/foobar/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"producer\"}}}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateBrokerConfig_nonExistingCluster_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"producer\"}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateBrokerConfig_nonExistingCluster_noContentType_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs/compression.type")
            .put(Entity.entity("{\"value\":\"producer\"}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetBrokerConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();
    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs/foobar")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetBrokerConfig_nonExistingBroker_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/foobar/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetBrokerConfig_nonExistingCluster_throwsNotFound() {
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/foobar/brokers/" + brokerId + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
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

  @Test
  public void alterConfigBatch_withExistingConfig() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    Response updateResponse =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs:alter")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"data\":["
                        + "{\"name\": \"max.connections\",\"value\":\"1000\"},"
                        + "{\"name\": \"compression.type\",\"value\":\"gzip\"}]}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    GetBrokerConfigResponse expectedAfterUpdate1 =
        GetBrokerConfigResponse.create(
            BrokerConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + brokerId
                                + "/configs/max.connections")
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/broker=" + brokerId
                                + "/config=max.connections")
                        .build())
                .setClusterId(clusterId)
                .setBrokerId(brokerId)
                .setName("max.connections")
                .setValue("1000")
                .setDefault(false)
                .setReadOnly(false)
                .setSensitive(false)
                .setSource(ConfigSource.DYNAMIC_BROKER_CONFIG)
                .setSynonyms(
                    Arrays.asList(
                        ConfigSynonymData.builder()
                            .setName("max.connections")
                            .setValue("1000")
                            .setSource(ConfigSource.DYNAMIC_BROKER_CONFIG)
                            .build(),
                        ConfigSynonymData.builder()
                            .setName("max.connections")
                            .setValue("2147483647")
                            .setSource(ConfigSource.DEFAULT_CONFIG)
                            .build()))
                .build());
    GetBrokerConfigResponse expectedAfterUpdate2 =
        GetBrokerConfigResponse.create(
            BrokerConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + brokerId
                                + "/configs/compression.type")
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/broker=" + brokerId
                                + "/config=compression.type")
                        .build())
                .setClusterId(clusterId)
                .setBrokerId(brokerId)
                .setName("compression.type")
                .setValue("gzip")
                .setDefault(false)
                .setReadOnly(false)
                .setSensitive(false)
                .setSource(ConfigSource.DYNAMIC_BROKER_CONFIG)
                .setSynonyms(
                    Arrays.asList(
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("gzip")
                            .setSource(ConfigSource.DYNAMIC_BROKER_CONFIG)
                            .build(),
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("producer")
                            .setSource(ConfigSource.DEFAULT_CONFIG)
                            .build()))
                .build());

    Response responseAfterUpdate1 =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/max.connections")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseAfterUpdate1.getStatus());
    GetBrokerConfigResponse actualResponseAfterUpdate1 =
        responseAfterUpdate1.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedAfterUpdate1, actualResponseAfterUpdate1);

    Response responseAfterUpdate2 =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseAfterUpdate2.getStatus());
    GetBrokerConfigResponse actualResponseAfterUpdate2 =
        responseAfterUpdate2.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedAfterUpdate2, actualResponseAfterUpdate2);
  }
}
