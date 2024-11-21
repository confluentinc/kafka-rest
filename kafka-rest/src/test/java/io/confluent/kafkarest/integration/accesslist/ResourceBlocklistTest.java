/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.integration.accesslist;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.Test;

public class ResourceBlocklistTest extends ResourceAccesslistTestBase {

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put("api.endpoints.allowlist", "");
    restProperties.put("api.endpoints.blocklist", "api.v3.topics.*, api.v3.clusters.list");
  }

  @Test
  public void testBlocklist() {
    // Even though the checks are not exactly independent (i.e. topic deletion should be tried
    // after topic creation), all of them are executed in a single test, as: (1) they are touching
    // different API endpoints, for which we don't need state reset (on the contrary); (2) failures
    // can easily be correlated to a check; and (3) running only one integration test method saves
    // a significant amount of time.
    blocklistDisablesResourceClass();
    blocklistDisablesResourceMethod();
    nonBlocklistResourcesEnabled();
  }

  private void blocklistDisablesResourceClass() {
    assertEquals(Status.NOT_FOUND.getStatusCode(), listTopics().getStatus());
    assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(), createTopic().getStatus());
    assertEquals(Status.NOT_FOUND.getStatusCode(), getTopic().getStatus());
    assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(), deleteTopic().getStatus());
  }

  private void blocklistDisablesResourceMethod() {
    assertEquals(Status.NOT_FOUND.getStatusCode(), listClusters().getStatus());
  }

  private void nonBlocklistResourcesEnabled() {
    assertEquals(Status.OK.getStatusCode(), getCluster().getStatus());
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateClusterConfig().getStatus());
  }
}
