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

package io.confluent.kafkarest.integration.v3;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.Properties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class DisableV3Test extends ClusterTestHarness {

  public DisableV3Test() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put(KafkaRestConfig.API_V3_ENABLE_CONFIG, false);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void v3ApiIsDisabled(String quorum) {
    Response response = request("/v3/clusters").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
