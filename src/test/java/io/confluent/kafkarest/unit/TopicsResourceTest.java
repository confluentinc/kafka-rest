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
import io.confluent.kafkarest.entities.*;
import io.confluent.kafkarest.junit.EmbeddedServerTestHarness;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.kafkarest.validation.ConstraintViolationExceptionMapper;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static io.confluent.kafkarest.TestUtils.assertErrorResponse;

public class TopicsResourceTest extends EmbeddedServerTestHarness {
    private Config config;
    private MetadataObserver mdObserver;
    private ProducerPool producerPool;
    private Context ctx;

    private List<TopicProduceRecord> produceRecordsOnlyValues;
    private List<TopicProduceRecord> produceRecordsWithKeys;
    private List<TopicProduceRecord> produceRecordsWithPartitions;
    private List<TopicProduceRecord> produceRecordsWithPartitionsAndKeys;
    // Partition -> New offset
    private Map<Integer,Long> produceOffsets;

    public TopicsResourceTest() throws ConfigurationException {
        config = new Config();
        mdObserver = EasyMock.createMock(MetadataObserver.class);
        producerPool = EasyMock.createMock(ProducerPool.class);
        ctx = new Context(config, mdObserver, producerPool, null);

        addResource(new TopicsResource(ctx));

        produceRecordsOnlyValues = Arrays.asList(
                new TopicProduceRecord("value".getBytes()),
                new TopicProduceRecord("value2".getBytes())
        );
        produceRecordsWithKeys = Arrays.asList(
                new TopicProduceRecord("key".getBytes(), "value".getBytes()),
                new TopicProduceRecord("key2".getBytes(), "value2".getBytes())
        );
        produceRecordsWithPartitions = Arrays.asList(
                new TopicProduceRecord("value".getBytes(), 0),
                new TopicProduceRecord("value2".getBytes(), 0)
        );
        produceRecordsWithPartitionsAndKeys = Arrays.asList(
                new TopicProduceRecord("key".getBytes(), "value".getBytes(), 0),
                new TopicProduceRecord("key2".getBytes(), "value2".getBytes(), 0)
        );
        produceOffsets = new HashMap<>();
        produceOffsets.put(0, 1L);
        produceOffsets.put(1, 2L);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        EasyMock.reset(mdObserver, producerPool);
    }

    @Test
    public void testList() {
        final List<Topic> topics = Arrays.asList(
                new Topic("test1", 10),
                new Topic("test2", 1),
                new Topic("test3", 10)
        );
        EasyMock.expect(mdObserver.getTopics()).andReturn(topics);
        EasyMock.replay(mdObserver);

        final List<Topic> response = getJerseyTest().target("/topics")
                .request().get(new GenericType<List<Topic>>() {});
        assertEquals(topics, response);

        EasyMock.verify(mdObserver);
    }

    @Test
    public void testGetTopic() {
        Topic topic1 = new Topic("topic1", 2);
        Topic topic2 = new Topic("topic2", 5);
        EasyMock.expect(mdObserver.getTopic("topic1"))
                .andReturn(topic1);
        EasyMock.expect(mdObserver.getTopic("topic2"))
                .andReturn(topic2);
        EasyMock.replay(mdObserver);

        final Topic response1 = getJerseyTest().target("/topics/topic1")
                .request().get(new GenericType<Topic>(){});
        assertEquals(topic1, response1);

        final Topic response2 = getJerseyTest().target("/topics/topic2")
                .request().get(new GenericType<Topic>() {
                });
        assertEquals(topic2, response2);

        EasyMock.verify(mdObserver);
    }

    @Test
    public void testGetInvalidTopic() {
        EasyMock.expect(mdObserver.getTopic("nonexistanttopic"))
                .andReturn(null);
        EasyMock.replay(mdObserver);

        Response response = getJerseyTest().target("/topics/nonexistanttopic")
                .request().get();
        assertErrorResponse(Response.Status.NOT_FOUND, response, TopicsResource.MESSAGE_TOPIC_NOT_FOUND);

        EasyMock.verify(mdObserver);
    }

    @Test
    public void testGetPartitionsResourceInvalidTopic() {
        EasyMock.expect(mdObserver.topicExists("nonexistanttopic")).andReturn(false);
        EasyMock.replay(mdObserver);

        Response response = getJerseyTest().target("/topics/nonexistanttopic/partitions")
                .request().get();
        assertErrorResponse(Response.Status.NOT_FOUND, response, TopicsResource.MESSAGE_TOPIC_NOT_FOUND);

        EasyMock.verify(mdObserver);
    }

    private Response produceToTopic(String topic, List<TopicProduceRecord> records, final Map<Integer, Long> resultOffsets) {
        final TopicProduceRequest request = new TopicProduceRequest();
        request.setRecords(records);
        final Capture<ProducerPool.ProduceRequestCallback> produceCallback = new Capture<>();
        EasyMock.expect(mdObserver.topicExists(topic)).andReturn(true);
        producerPool.produce(EasyMock.<ProducerRecordProxyCollection>anyObject(), EasyMock.capture(produceCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                if (resultOffsets == null)
                    produceCallback.getValue().onException(new Exception());
                else
                    produceCallback.getValue().onCompletion(resultOffsets);
                return null;
            }
        });
        EasyMock.replay(mdObserver, producerPool);

        Response response = getJerseyTest().target("/topics/" + topic)
                .request().post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

        EasyMock.verify(mdObserver, producerPool);

        return response;
    }

    @Test
    public void testProduceToTopicOnlyValues() {
        Response rawResponse = produceToTopic("topic1", produceRecordsOnlyValues, produceOffsets);
        ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

        assertEquals(
                Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
                response.getOffsets()
        );
    }

    @Test
    public void testProduceToTopicByKey() {
        Response rawResponse = produceToTopic("topic1", produceRecordsWithKeys, produceOffsets);
        ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

        assertEquals(
                Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
                response.getOffsets()
        );
    }

    @Test
    public void testProduceToTopicByPartition() {
        Response rawResponse = produceToTopic("topic1", produceRecordsWithPartitions, produceOffsets);
        ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

        assertEquals(
                Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
                response.getOffsets()
        );
    }

    @Test
    public void testProduceToTopicWithPartitionAndKey() {
        Response rawResponse = produceToTopic("topic1", produceRecordsWithPartitionsAndKeys, produceOffsets);
        ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

        assertEquals(
                Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
                response.getOffsets()
        );
    }

    @Test
    public void testProduceToTopicFailure() {
        // null offsets triggers a generic exception
        Response rawResponse = produceToTopic("topic1", produceRecordsWithKeys, null);
        assertErrorResponse(Response.Status.INTERNAL_SERVER_ERROR, rawResponse, Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase());
    }

    @Test
    public void testProduceInvalidRequest() {
        Response response = getJerseyTest().target("/topics/topic1")
                .request().post(Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE));
        assertEquals(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY, response.getStatus());

        // Invalid base64 encoding
        response = getJerseyTest().target("/topics/topic1")
                .request().post(Entity.entity("{\"records\":[{\"value\":\"aGVsbG8==\"}]}", MediaType.APPLICATION_JSON_TYPE));
        assertEquals(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY, response.getStatus());
    }
}
