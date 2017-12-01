/**
 * Copyright 2015 Confluent Inc.
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
 **/
package io.confluent.kafkarest.unit;

import org.junit.Test;

import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.resources.RootResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RootResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private DefaultKafkaRestContext ctx;

  public RootResourceTest() throws RestConfigException {
    ctx = new DefaultKafkaRestContext(config, null, null, null, null, null, null);
    addResource(RootResource.class);
  }

  @Test
  public void testRootResource() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      Response response = request("/", mediatype.header).get();
      assertOKResponse(response, mediatype.expected);
      Map<String, String> decoded = TestUtils.tryReadEntityOrLog(response, new GenericType<Map<String, String>>() {
      });
    }
  }

  @Test
  public void testInvalidAcceptMediatype() {
    for (String mediatype : TestUtils.V1_INVALID_MEDIATYPES) {
      Response response = request("/", mediatype).get();
      // We would like to check for a normal error response, but Jersey/JAX-RS and possibly the
      // underlying servlet API spec specify that 406 Not Acceptable responses will not attach
      // entities. See https://java.net/jira/browse/JAX_RS_SPEC-363 for the corresponding JAX-RS
      // bug. We verify the little bit we can (status code) here.
      assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
      // These verify that we're seeing the *expected* but *incorrect* behavior.
      assertNull(response.getMediaType());
    }
  }

  @Test
  public void testInvalidEntityContentType() {
    Response.Status UNSUPPORTED_MEDIA_TYPE = Response.Status.UNSUPPORTED_MEDIA_TYPE;
    for (String mediatype : TestUtils.V1_INVALID_REQUEST_MEDIATYPES) {
      Response
          response =
          request("/", Versions.KAFKA_MOST_SPECIFIC_DEFAULT + ", " + Versions.GENERIC_REQUEST)
              .post(Entity.entity("", mediatype));
      assertErrorResponse(
          UNSUPPORTED_MEDIA_TYPE, response,
          UNSUPPORTED_MEDIA_TYPE.getStatusCode(),
          "HTTP " + UNSUPPORTED_MEDIA_TYPE.getStatusCode() + " " + UNSUPPORTED_MEDIA_TYPE
              .getReasonPhrase(),
          Versions.KAFKA_MOST_SPECIFIC_DEFAULT
      );
    }
  }
}
