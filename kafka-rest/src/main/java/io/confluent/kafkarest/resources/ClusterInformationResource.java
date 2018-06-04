/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafkarest.resources;

import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.BrokerEntity;
import io.confluent.rest.annotations.PerformanceMetric;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/cluster")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED,
        Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
        Versions.JSON_WEIGHTED,
        Versions.JSON})
public class ClusterInformationResource {

  private final KafkaRestContext ctx;

  /**
   * Provides information about cluster metadata
   * <p>Create cluster information resource</p>
   *
   * @param ctx - context of rest application
   */
  public ClusterInformationResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  /**
   * <p>Get cluster controller address</p>
   * <p>Example: http://127.0.0.1:2081/cluster/controller/</p>
   *
   * @return BrokerEntity -
   *     {"brokerId":0,"endPointList":[{"protocol":"SSL","host":"127.0.0.1","port":9092}]}
   */
  @GET
  @Path("/controller")
  @PerformanceMetric("controller.address")
  public BrokerEntity controllerInformation() {
    return ctx.getClusterInformationObserver().getCurrentClusterController();
  }
}
