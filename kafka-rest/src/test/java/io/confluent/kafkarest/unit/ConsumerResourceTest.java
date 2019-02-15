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

import org.easymock.EasyMock;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.BinaryConsumerState;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.RestNotFoundException;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

// This test evaluates non-consume functionality for ConsumerResource, exercising functionality
// that isn't really dependent on the EmbeddedFormat, e.g. invalid requests, commit offsets,
// delete consumer. It ends up using the BINARY default since it is sometimes required to make
// those operations possible, but the operations shouldn't be affected by it. See
// ConsumerResourceBinaryTest and ConsumerResourceAvroTest for format-specific tests.
public class ConsumerResourceTest extends AbstractConsumerResourceTest {

  public ConsumerResourceTest() throws RestConfigException {
    super();
  }

  @Test
  public void testCreateInstanceRequestsNewInstance() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        expectCreateGroup(new ConsumerInstanceConfig());
        EasyMock.replay(consumerManager);

        Response response = request("/consumers/" + groupName, mediatype.header)
            .post(Entity.entity(null, requestMediatype));
        assertOKResponse(response, mediatype.expected);
        final CreateConsumerInstanceResponse ciResponse =
                TestUtils.tryReadEntityOrLog(response, CreateConsumerInstanceResponse.class);
        assertEquals(instanceId, ciResponse.getInstanceId());
        assertThat(ciResponse.getBaseUri(),
                   allOf(startsWith("http://"), containsString(instancePath)));

        EasyMock.verify(consumerManager);
        EasyMock.reset(consumerManager);
      }
    }
  }

  @Test
  public void testCreateInstanceWithConfig() {
    ConsumerInstanceConfig config = new ConsumerInstanceConfig();
    config.setId("testid");

    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        expectCreateGroup(config);
        EasyMock.replay(consumerManager);

        Response response = request("/consumers/" + groupName, mediatype.header)
            .post(Entity.entity(config, requestMediatype));
        assertOKResponse(response, mediatype.expected);
        final CreateConsumerInstanceResponse ciResponse =
                TestUtils.tryReadEntityOrLog(response, CreateConsumerInstanceResponse.class);
        assertEquals(instanceId, ciResponse.getInstanceId());
        assertThat(ciResponse.getBaseUri(),
                   allOf(startsWith("http://"), containsString(instancePath)));

        EasyMock.verify(consumerManager);
        EasyMock.reset(consumerManager);
      }
    }
  }

  @Test
  public void testInvalidInstanceOrTopic() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        // Trying to access either an invalid consumer instance or a missing topic should trigger
        // an error
        expectCreateGroup(new ConsumerInstanceConfig());
        expectReadTopic(topicName, BinaryConsumerState.class, null,
                        new RestNotFoundException(not_found_message, 1000));
        EasyMock.replay(consumerManager);

        Response response = request("/consumers/" + groupName, mediatype.header)
            .post(Entity.entity(null, requestMediatype));
        assertOKResponse(response, mediatype.expected);
        final CreateConsumerInstanceResponse createResponse =
                TestUtils.tryReadEntityOrLog(response, CreateConsumerInstanceResponse.class);

        final Response
            readResponse =
            request(instanceBasePath(createResponse) + "/topics/" + topicName, mediatype.header)
                .get();
        // Most specific default is different when retrieving embedded data
        String expectedMediatype
            = mediatype.header != null ? mediatype.expected : Versions.KAFKA_V1_JSON_BINARY;
        assertErrorResponse(Response.Status.NOT_FOUND, readResponse,
                            1000, not_found_message, expectedMediatype);

        EasyMock.verify(consumerManager);
        EasyMock.reset(consumerManager);
      }
    }
  }

  @Test
  public void testDeleteInstance() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        expectCreateGroup(new ConsumerInstanceConfig());
        expectDeleteGroup(false);
        EasyMock.replay(consumerManager);

        Response response = request("/consumers/" + groupName, mediatype.header)
            .post(Entity.entity(null, requestMediatype));
        assertOKResponse(response, mediatype.expected);
        final CreateConsumerInstanceResponse createResponse =
                TestUtils.tryReadEntityOrLog(response, CreateConsumerInstanceResponse.class);

        final Response
            deleteResponse =
            request(instanceBasePath(createResponse), mediatype.header).delete();
        assertErrorResponse(Response.Status.NO_CONTENT, deleteResponse,
                            0, null, mediatype.expected);

        EasyMock.verify(consumerManager);
        EasyMock.reset(consumerManager);
      }
    }
  }

  @Test
  public void testDeleteInvalidInstance() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      expectDeleteGroup(true);
      EasyMock.replay(consumerManager);

      final Response
          deleteResponse =
          request("/consumers/" + groupName + "/instances/" + instanceId, mediatype.header)
              .delete();
      assertErrorResponse(Response.Status.NOT_FOUND, deleteResponse,
                          1000, not_found_message, mediatype.expected);

      EasyMock.verify(consumerManager);
      EasyMock.reset(consumerManager);
    }
  }
}
