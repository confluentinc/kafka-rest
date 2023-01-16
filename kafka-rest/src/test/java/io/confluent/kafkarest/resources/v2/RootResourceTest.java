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

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.junit.Test;

public class RootResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private DefaultKafkaRestContext ctx;

  public RootResourceTest() throws RestConfigException {
    ctx = new DefaultKafkaRestContext(config, null, /* kafkaConsumerManager= */ null);
    addResource(RootResource.class);
  }

  @Test
  public void testRootResource() {
    Response response = request("/", Versions.KAFKA_V2_JSON).get();
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    Map<String, String> decoded = TestUtils.tryReadEntityOrLog(response, new GenericType<Map<String, String>>() {
    });
  }

  @Test
  public void testInvalidAcceptMediatype() {
    Response response = request("/", "text/plain").get();
    // We would like to check for a normal error response, but Jersey/JAX-RS and possibly the
    // underlying servlet API spec specify that 406 Not Acceptable responses will not attach
    // entities. See https://java.net/jira/browse/JAX_RS_SPEC-363 for the corresponding JAX-RS
    // bug. We verify the little bit we can (status code) here.
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    // These verify that we're seeing the *expected* but *incorrect* behavior.
    assertNull(response.getMediaType());
  }

  @Test
  public void testInvalidEntityContentType() {
    Response.Status UNSUPPORTED_MEDIA_TYPE = Response.Status.UNSUPPORTED_MEDIA_TYPE;
    Response
        response =
        request("/", Versions.KAFKA_V2_JSON + ", " + Versions.GENERIC_REQUEST)
            .post(Entity.entity("", "text/plain"));
    assertErrorResponse(
        UNSUPPORTED_MEDIA_TYPE, response,
        UNSUPPORTED_MEDIA_TYPE.getStatusCode(),
        "HTTP " + UNSUPPORTED_MEDIA_TYPE.getStatusCode() + " " + UNSUPPORTED_MEDIA_TYPE
            .getReasonPhrase(),
        Versions.KAFKA_V2_JSON
    );
  }
}
