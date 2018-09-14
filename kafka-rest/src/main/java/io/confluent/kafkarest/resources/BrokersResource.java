/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.resources;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.BrokerEntity;
import io.confluent.kafkarest.entities.BrokerList;
import io.confluent.kafkarest.entities.NodeState;
import io.confluent.rest.annotations.PerformanceMetric;

/**
 * Resource representing the collection of all available brokers.
 */
@Path("/brokers")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED, Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes()
public class BrokersResource {

  private final KafkaRestContext ctx;

  public BrokersResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @Valid
  @PerformanceMetric("brokers.list")
  public BrokerList list() throws Exception {
    return new BrokerList(ctx.getAdminClientWrapper().getBrokerIds());
  }

  /**
   * <p>Get broker metadata</p>
   *
   * @param brokerId - broker ID
   * @return metadata about broker or null if broker not found
   */
  @GET
  @Path("/{brokerId}")
  @PerformanceMetric("brokers.get")
  public BrokerEntity get(@PathParam("brokerId")Integer brokerId) {
    return ctx.getMetadataObserver().getBroker(brokerId);
  }

  /**
   * <p>Get broker state</p>
   * <p>Example: http://127.0.0.1:2081/0/state</p>
   *
   * @return state of broker
   */
  @GET
  @Path("/{brokerId}/state")
  @PerformanceMetric("brokers.get.state")
  public NodeState state(@PathParam("brokerId")Integer brokerId) {
    return ctx.getAdminClientWrapper().getBrokerState(brokerId);
  }
}
