package io.confluent.kafkarest.integration;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Test;

public class KafkaRestStartUpIntegrationTest extends ClusterTestHarness {

  public KafkaRestStartUpIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put("client.security.protocol", "SASL_PLAINTEXT");
    restProperties.put("client.sasl.mechanism", "OAUTHBEARER");
    restProperties.put("client.sasl.kerberos.service.name", "kafka");
  }

  @Test
  public void kafkaRest_withInvalidAdminConfigs_startsUp() {
    // Make sure that Admin is not created on startup. If it were, the server would fail to startup,
    // since the above security configs are incomplete. See
    // https://github.com/confluentinc/kafka-rest/pull/632 for context.

    // The server started up successfully. Now make sure doing a request that require Admin fails.
    Response response = request("/v3/clusters").accept("application/vnd.api+json").get();
    assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }
}
