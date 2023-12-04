/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.integration;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class KafkaRestStartUpIntegrationTest extends ClusterTestHarness {

  public KafkaRestStartUpIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put("client.security.protocol", "SASL_PLAINTEXT");
    restProperties.put("client.sasl.mechanism", "OAUTHBEARER");
    restProperties.put("client.sasl.kerberos.service.name", "kafka");
    restProperties.put("response.http.headers.config", "add X-XSS-Protection: 1; mode=block");
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void kafkaRest_withInvalidAdminConfigs_startsUp(String quorum) {
    // Make sure that Admin is not created on startup. If it were, the server would fail to startup,
    // since the above security configs are incomplete. See
    // https://github.com/confluentinc/kafka-rest/pull/632 for context.

    // The server started up successfully. Now make sure doing a request that require Admin fails.
    Response response = request("/v3/clusters").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testHttpResponseHeader(String quorum) {
    Response response = request("/v3/clusters").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(response.getHeaderString("X-XSS-Protection"), "1; mode=block");
  }
}
