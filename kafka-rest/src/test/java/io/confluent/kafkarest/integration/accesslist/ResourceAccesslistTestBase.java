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

import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class ResourceAccesslistTestBase extends ClusterTestHarness {

  private static final String TOPIC_1 = "topic-1";

  public ResourceAccesslistTestBase() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Override
  public Properties overrideBrokerProperties(int i, Properties props) {
    props.put("delete.topic.enable", true);
    return props;
  }

  /*
   * Topic API helper methods
   */

  Response listTopics() {
    return request("/v3/clusters/" + getClusterId() + "/topics")
        .accept(MediaType.APPLICATION_JSON)
        .get();
  }

  Response topicsOptions() {
    return request("/v3/clusters/" + getClusterId() + "/topics").options();
  }

  Response createTopic() {
    return request("/v3/clusters/" + getClusterId() + "/topics")
        .accept(MediaType.APPLICATION_JSON)
        .post(
            Entity.entity(
                "{\"topic_name\":\""
                    + TOPIC_1
                    + "\",\"partitions_count\":3,\"replication_factor\":3}",
                MediaType.APPLICATION_JSON));
  }

  Response getTopic() {
    return request("/v3/clusters/" + getClusterId() + "/topics/" + TOPIC_1)
        .accept(MediaType.APPLICATION_JSON)
        .get();
  }

  Response deleteTopic() {
    return request("/v3/clusters/" + getClusterId() + "/topics/" + TOPIC_1)
        .accept(MediaType.APPLICATION_JSON)
        .delete();
  }

  /*
   * Cluster API helper methods
   */

  Response listClusters() {
    return request("/v3/clusters").accept(MediaType.APPLICATION_JSON).get();
  }

  Response clustersOptions() {
    return request("/v3/clusters/").options();
  }

  Response getCluster() {
    return request("/v3/clusters/" + getClusterId()).accept(MediaType.APPLICATION_JSON).get();
  }

  /*
   * Cluster config API helper methods
   */

  Response updateClusterConfig() {
    return request("/v3/clusters/" + getClusterId() + "/broker-configs:alter")
        .accept(MediaType.APPLICATION_JSON)
        .post(
            Entity.entity(
                "{\"data\":["
                    + "{\"name\": \"max.connections\",\"value\":\"1000\"},"
                    + "{\"name\": \"compression.type\",\"value\":\"gzip\"}]}",
                MediaType.APPLICATION_JSON));
  }
}
