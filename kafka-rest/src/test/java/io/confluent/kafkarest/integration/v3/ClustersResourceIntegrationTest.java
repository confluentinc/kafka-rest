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

import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Test;

public class ClustersResourceIntegrationTest extends ClusterTestHarness {

  public ClustersResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listClusters_returnsArrayWithOwnCluster() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int controllerId = getControllerID();

    String expected = "{"
        + "\"links\":{"
        +   "\"self\":\"" + baseUrl + "/v3/clusters\","
        +   "\"next\":null"
        + "},"
        + "\"data\":["
        +   "{"
        +     "\"links\":{"
        +       "\"self\":\"" + baseUrl + "/v3/clusters/" + clusterId + "\""
        +     "},"
        +     "\"attributes\":{"
        +       "\"cluster_id\":\"" + clusterId + "\""
        +     "},"
        +     "\"relationships\":{"
        +       "\"controller\":{"
        +         "\"links\":{"
        +           "\"related\":\""
        +             baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + controllerId + "\""
        +         "}"
        +       "},"
        +       "\"brokers\":{"
        +         "\"links\":{"
        +           "\"related\":\"" + baseUrl + "/v3/clusters/" + clusterId + "/brokers\""
        +         "}"
        +       "}"
        +     "},"
        +     "\"id\":\"" + clusterId + "\","
        +     "\"type\":\"KafkaCluster\""
        +   "}"
        + "]}";

    Response response = request("/v3/clusters").accept(Versions.JSON_API).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(String.class));
  }

  @Test
  public void getCluster_ownCluster_returnsOwnCluster() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int controllerId = getControllerID();

    String expected = "{"
        + "\"data\":{"
        +   "\"links\":{"
        +     "\"self\":\"" + baseUrl + "/v3/clusters/" + clusterId + "\""
        +   "},"
        +   "\"attributes\":{"
        +     "\"cluster_id\":\"" + clusterId + "\""
        +   "},"
        +   "\"relationships\":{"
        +     "\"controller\":{"
        +       "\"links\":{"
        +         "\"related\":\""
        +           baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + controllerId + "\""
        +       "}"
        +     "},"
        +     "\"brokers\":{"
        +       "\"links\":{"
        +         "\"related\":\"" + baseUrl + "/v3/clusters/" + clusterId + "/brokers\""
        +       "}"
        +     "}"
        +   "},"
        +   "\"id\":\"" + clusterId + "\","
        +   "\"type\":\"KafkaCluster\""
        + "}}";

    Response response =
        request(String.format("/v3/clusters/%s", clusterId))
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(String.class));
  }

  @Test
  public void getCluster_differentCluster_returnsNotFound() {
    Response response = request("/v3/clusters/foobar").accept(Versions.JSON_API).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
