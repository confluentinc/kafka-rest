package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;

import io.confluent.kafkarest.entities.ClusterConfig;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.ClusterConfigData;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.GetBrokerConfigResponse;
import io.confluent.kafkarest.entities.v3.GetClusterConfigResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
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
            .accept(MediaType.APPLICATION_JSON)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getClusterConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/broker-configs/foobar")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getClusterConfig_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/broker-configs/max.connections")
            .accept(MediaType.APPLICATION_JSON)
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
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), responseBeforeUpdate.getStatus());

    GetBrokerConfigResponse expectedBrokerBeforeUpdate =
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

    Response brokerResponseBeforeUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), brokerResponseBeforeUpdate.getStatus());

    GetBrokerConfigResponse actualBrokerBeforeUpdate =
        brokerResponseBeforeUpdate.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedBrokerBeforeUpdate, actualBrokerBeforeUpdate);

    Response updateResponse =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"gzip\"}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    GetClusterConfigResponse expectedAfterUpdate =
        GetClusterConfigResponse.create(
            ClusterConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/broker-configs/compression.type")
                        .setResourceName(
                            "crn:///kafka=" + clusterId + "/broker-config=compression.type")
                        .build())
                .setClusterId(clusterId)
                .setConfigType(ClusterConfig.Type.BROKER)
                .setName("compression.type")
                .setValue("gzip")
                .setDefault(false)
                .setReadOnly(false)
                .setSensitive(false)
                .setSource(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
                .setSynonyms(
                    singletonList(
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("gzip")
                            .setSource(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
                            .build()))
                .build());

    Response responseAfterUpdate =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseAfterUpdate.getStatus());

    GetClusterConfigResponse actualAfterUpdate =
        responseAfterUpdate.readEntity(GetClusterConfigResponse.class);
    assertEquals(expectedAfterUpdate, actualAfterUpdate);

    GetBrokerConfigResponse expectedBrokerAfterUpdate =
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
                .setSource(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
                .setSynonyms(
                    Arrays.asList(
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("gzip")
                            .setSource(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
                            .build(),
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("producer")
                            .setSource(ConfigSource.DEFAULT_CONFIG)
                            .build()))
                .build());

    Response responseBrokerAfterUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseBrokerAfterUpdate.getStatus());

    GetBrokerConfigResponse actualBrokerAfterUpdate =
        responseBrokerAfterUpdate.readEntity(GetBrokerConfigResponse.class);
    assertEquals(expectedBrokerAfterUpdate, actualBrokerAfterUpdate);

    Response resetResponse =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), resetResponse.getStatus());

    Response responseAfterReset =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), responseAfterReset.getStatus());

    GetBrokerConfigResponse expectedBrokerAfterReset =
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

    Response brokerResponseAfterReset =
        request(
            "/v3/clusters/" + clusterId
                + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
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
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"producer\"}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetClusterConfig_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/broker-configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
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
            "/v3/clusters/" + clusterId + "/broker-configs:alter")
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
                .setSource(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
                .setSynonyms(
                    Arrays.asList(
                        ConfigSynonymData.builder()
                            .setName("max.connections")
                            .setValue("1000")
                            .setSource(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
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
                .setSource(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
                .setSynonyms(
                    Arrays.asList(
                        ConfigSynonymData.builder()
                            .setName("compression.type")
                            .setValue("gzip")
                            .setSource(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
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
