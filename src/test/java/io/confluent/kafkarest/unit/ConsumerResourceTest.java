/**
 * Copyright 2014 Confluent Inc.
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
package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.*;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.junit.EmbeddedServerTestHarness;
import io.confluent.kafkarest.resources.ConsumersResource;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class ConsumerResourceTest extends EmbeddedServerTestHarness {
    private Config config;
    private MetadataObserver mdObserver;
    private ConsumerManager consumerManager;
    private Context ctx;

    private static final String groupName = "testgroup";
    private static final String topicName = "testtopic";
    private static final String instanceId = "uniqueid";
    private static final String instancePath = "/consumers/" + groupName + "/instances/" + instanceId;

    private static final String not_found_message = "not found";

    public ConsumerResourceTest() throws ConfigurationException {
        config = new Config();
        mdObserver = EasyMock.createMock(MetadataObserver.class);
        consumerManager = EasyMock.createMock(ConsumerManager.class);
        ctx = new Context(config, mdObserver, null, consumerManager);

        addResource(new ConsumersResource(ctx));
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        EasyMock.reset(mdObserver, consumerManager);
    }

    @Test
    public void testCreateInstanceRequestsNewInstance() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            expectCreateGroup();
            EasyMock.replay(consumerManager);

            Response response = request("/consumers/" + groupName, mediatype.header)
                    .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
            assertOKResponse(response, mediatype.expected);
            final CreateConsumerInstanceResponse ciResponse = response.readEntity(CreateConsumerInstanceResponse.class);
            assertEquals(instanceId, ciResponse.getInstanceId());
            assertThat(ciResponse.getBaseUri(), allOf(startsWith("http://"), containsString(instancePath)));

            EasyMock.verify(consumerManager);
            EasyMock.reset(consumerManager);
        }
    }

    @Test
    public void testInvalidInstanceOrTopic() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            // Trying to access either an invalid consumer instance or a missing topic should trigger an error
            expectCreateGroup();
            expectReadTopic(topicName, null, new NotFoundException(not_found_message));
            EasyMock.replay(consumerManager);

            Response response = request("/consumers/" + groupName, mediatype.header)
                    .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
            assertOKResponse(response, mediatype.expected);
            final CreateConsumerInstanceResponse createResponse = response.readEntity(CreateConsumerInstanceResponse.class);

            final Response readResponse = request(instanceBasePath(createResponse) + "/topics/" + topicName, mediatype.header)
                    .get();
            assertErrorResponse(Response.Status.NOT_FOUND, readResponse, not_found_message, mediatype.expected);

            EasyMock.verify(consumerManager);
            EasyMock.reset(consumerManager);
        }
    }

    @Test
    public void testRead() {
        List<ConsumerRecord> expected = Arrays.asList(
                new ConsumerRecord("key1".getBytes(), "value1".getBytes(), 0),
                new ConsumerRecord("key2".getBytes(), "value2".getBytes(), 1),
                new ConsumerRecord("key3".getBytes(), "value3".getBytes(), 2)
        );

        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            expectCreateGroup();
            expectReadTopic(topicName, expected, null);
            EasyMock.replay(consumerManager);

            Response response = request("/consumers/" + groupName, mediatype.header)
                    .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
            assertOKResponse(response, mediatype.expected);
            final CreateConsumerInstanceResponse createResponse = response.readEntity(CreateConsumerInstanceResponse.class);

            Response readResponse = request(instanceBasePath(createResponse) + "/topics/" + topicName, mediatype.header).get();
            assertOKResponse(readResponse, mediatype.expected);
            final List<ConsumerRecord> readResponseRecords = readResponse.readEntity(new GenericType<List<ConsumerRecord>>() {});
            assertEquals(expected, readResponseRecords);

            EasyMock.verify(consumerManager);
            EasyMock.reset(consumerManager);
        }
    }

    @Test public void testDeleteInstance() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            expectCreateGroup();
            expectDeleteGroup(false);
            EasyMock.replay(consumerManager);

            Response response = request("/consumers/" + groupName, mediatype.header)
                    .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
            assertOKResponse(response, mediatype.expected);
            final CreateConsumerInstanceResponse createResponse = response.readEntity(CreateConsumerInstanceResponse.class);

            final Response deleteResponse = request(instanceBasePath(createResponse), mediatype.header).delete();
            assertErrorResponse(Response.Status.NO_CONTENT, deleteResponse, null, mediatype.expected);

            EasyMock.verify(consumerManager);
            EasyMock.reset(consumerManager);
        }
    }

    @Test public void testDeleteInvalidInstance() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            expectDeleteGroup(true);
            EasyMock.replay(consumerManager);

            final Response deleteResponse = request("/consumers/" + groupName + "/instances/" + instanceId, mediatype.header)
                    .delete();
            assertErrorResponse(Response.Status.NOT_FOUND, deleteResponse, not_found_message, mediatype.expected);

            EasyMock.verify(consumerManager);
            EasyMock.reset(consumerManager);
        }
    }



    private void expectCreateGroup() {
        EasyMock.expect(consumerManager.createConsumer(groupName)).andReturn(instanceId);
    }

    private void expectReadTopic(String topicName, final List<ConsumerRecord> readResult, final Exception readException) {
        final Capture<ConsumerManager.ReadCallback> readCallback = new Capture<>();
        consumerManager.readTopic(EasyMock.eq(groupName), EasyMock.eq(instanceId), EasyMock.eq(topicName), EasyMock.capture(readCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                readCallback.getValue().onCompletion(readResult, readException);
                return null;
            }
        });
    }

    private String instanceBasePath(CreateConsumerInstanceResponse createResponse) {
        try {
            return new URI(createResponse.getBaseUri()).getPath();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI in CreateConsumerInstanceResponse: \"" + createResponse.getBaseUri() + "\"");
        }
    }

    private void expectDeleteGroup(boolean invalid) {
        consumerManager.deleteConsumer(groupName, instanceId);
        IExpectationSetters expectation = EasyMock.expectLastCall();
        if (invalid) {
            expectation.andThrow(new NotFoundException(not_found_message));
        }
    }
}
