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
        expectCreateGroup();
        EasyMock.replay(consumerManager);

        final CreateConsumerInstanceResponse response = getJerseyTest().target("/consumers/" + groupName)
                .request().post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE), CreateConsumerInstanceResponse.class);
        assertEquals(instanceId, response.getInstanceId());
        assertThat(response.getBaseUri(), allOf(startsWith("http://"), containsString(instancePath)));

        EasyMock.verify(consumerManager);
    }

    @Test
    public void testInvalidInstanceOrTopic() {
        // Trying to access either an invalid consumer instance or a missing topic should trigger an error
        expectCreateGroup();
        expectReadTopic(topicName, null, new NotFoundException(not_found_message));
        EasyMock.replay(consumerManager);

        final CreateConsumerInstanceResponse createResponse = getJerseyTest().target("/consumers/" + groupName)
                .request().post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE), CreateConsumerInstanceResponse.class);
        final Response readResponse = getJerseyTest()
                .target(instanceBasePath(createResponse) + "/topics/" + topicName)
                .request().get();
        assertErrorResponse(Response.Status.NOT_FOUND, readResponse, not_found_message);

        EasyMock.verify(consumerManager);
    }

    @Test
    public void testRead() {
        List<ConsumerRecord> expected = Arrays.asList(
                new ConsumerRecord("key1".getBytes(), "value1".getBytes(), 0),
                new ConsumerRecord("key2".getBytes(), "value2".getBytes(), 1),
                new ConsumerRecord("key3".getBytes(), "value3".getBytes(), 2)
        );
        expectCreateGroup();
        expectReadTopic(topicName, expected, null);
        EasyMock.replay(consumerManager);

        final CreateConsumerInstanceResponse createResponse = getJerseyTest().target("/consumers/" + groupName)
                .request().post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE), CreateConsumerInstanceResponse.class);
        final List<ConsumerRecord> readResponse = getJerseyTest()
                .target(instanceBasePath(createResponse) + "/topics/" + topicName)
                .request().get(new GenericType<List<ConsumerRecord>>() {
                });
        assertEquals(expected, readResponse);

        EasyMock.verify(consumerManager);
    }

    @Test public void testDeleteInstance() {
        expectCreateGroup();
        expectDeleteGroup(false);
        EasyMock.replay(consumerManager);

        final CreateConsumerInstanceResponse createResponse = getJerseyTest().target("/consumers/" + groupName)
                .request().post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE), CreateConsumerInstanceResponse.class);
        final Response deleteResponse = getJerseyTest()
                .target(instanceBasePath(createResponse))
                .request().delete();
        assertErrorResponse(Response.Status.NO_CONTENT, deleteResponse, null);

        EasyMock.verify(consumerManager);
    }

    @Test public void testDeleteInvalidInstance() {
        expectDeleteGroup(true);
        EasyMock.replay(consumerManager);

        final Response deleteResponse = getJerseyTest()
                .target("/consumers/" + groupName + "/instances/" + instanceId)
                .request().delete();
        assertErrorResponse(Response.Status.NOT_FOUND, deleteResponse, not_found_message);

        EasyMock.verify(consumerManager);
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
