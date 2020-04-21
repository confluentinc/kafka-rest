package io.confluent.kafkarest.integration.v3;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
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
                    + "/v3/clusters" + clusterId
                    + "/brokers/" + brokerId
                    + "/configs",
                /* next= */ null));
    String expectedConfig1 =
        OBJECT_MAPPER.writeValueAsString(
            new BrokerConfigData(
                "crn:///kafka=" + clusterId + "/broker=" + brokerId + "/config=max.connections",
                new ResourceLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/brokers" + brokerId
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
                "crn:///kafka=" + clusterId + "/broker=" + brokerId + "/config=log.dir",
                new ResourceLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/brokers" + brokerId
                        + "/configs/log.dir"),
                clusterId,
                brokerId,
                "log.dir",
                "/tmp/kafka-logs",
                /* isDefault= */ true,
                /* isReadOnly= */ false,
                /* isSensitive= */ false));

    String expectedConfig3 =
        OBJECT_MAPPER.writeValueAsString(
            new BrokerConfigData(
                "crn:///kafka=" + clusterId + "/broker=" + brokerId + "/config=log.cleaner.threads",
                new ResourceLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/brokers" + brokerId
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
}
