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

package io.confluent.kafkarest.resources.v2;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import java.util.HashMap;
import java.util.Map;

@Path("/")
@Produces({Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V2_JSON})
@ResourceName("api.v2.root.*")
public final class RootResource {

  @GET
  @PerformanceMetric("root.get+v2")
  @ResourceName("api.v2.root.get")
  public Map<String, String> get() {
    // Currently this just provides an endpoint that's a nop and can be used to check for liveness
    // and can be used for tests that need to test the server setup rather than the functionality
    // of a specific resource. Some APIs provide a listing of endpoints as their root resource; it
    // might be nice to provide that.
    return new HashMap<>();
  }

  @POST
  @PerformanceMetric("root.post+v2")
  @ResourceName("api.v2.root.post")
  public Map<String, String> post(@Valid Map<String, String> request) {
    // This version allows testing with posted entities
    return new HashMap<>();
  }
}
