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
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.HashMap;
import java.util.Map;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/")
@Produces({Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V2_JSON})
public final class RootResource {

  @GET
  @PerformanceMetric("root.get+v2")
  public Map<String, String> get() {
    // Currently this just provides an endpoint that's a nop and can be used to check for liveness
    // and can be used for tests that need to test the server setup rather than the functionality
    // of a specific resource. Some APIs provide a listing of endpoints as their root resource; it
    // might be nice to provide that.
    return new HashMap<String, String>();
  }

  @POST
  @PerformanceMetric("root.post+v2")
  public Map<String, String> post(@Valid Map<String, String> request) {
    // This version allows testing with posted entities
    return new HashMap<String, String>();
  }

}
