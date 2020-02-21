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

package io.confluent.kafkarest.resources.v3;

import io.confluent.kafkarest.Versions;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

@Path("/v3/clusters/{clusterId}/brokers")
public class BrokersResource {

  @GET
  @Produces(Versions.JSON_API)
  public void listBrokers(@PathParam("clusterId") String clusterId) {
    // This placeholder is here because the ClustersResource will return URL links to this API. We
    // need to signal HTTP 501 if someone follows the URL, otherwise the server would return HTTP
    // 404.
    throw new WebApplicationException(Status.NOT_IMPLEMENTED);
  }

  @GET
  @Path("/{brokerId}")
  @Produces(Versions.JSON_API)
  public void getBroker(
      @PathParam("clusterId") String clusterId, @PathParam("brokerId") Integer brokerId) {
    // This placeholder is here because the ClustersResource will return URL links to this API. We
    // need to signal HTTP 501 if someone follows the URL, otherwise the server would return HTTP
    // 404.
    throw new WebApplicationException(Status.NOT_IMPLEMENTED);
  }
}
