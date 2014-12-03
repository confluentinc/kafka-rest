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
import io.confluent.kafkarest.resources.PartitionsResource;
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

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
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
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            EasyMock.expect(mdObserver.getTopics()).andReturn(topics);
            EasyMock.replay(mdObserver);

            Response response = request("/topics", mediatype.expected).get();
            assertOKResponse(response, mediatype.expected);
            final List<Topic> topicsResponse = response.readEntity(new GenericType<List<Topic>>() {});
            assertEquals(topics, topicsResponse);

            EasyMock.verify(mdObserver);
            EasyMock.reset(mdObserver);
        }
    }

    @Test
    public void testGetTopic() {
        Topic topic1 = new Topic("topic1", 2);
        Topic topic2 = new Topic("topic2", 5);

        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            EasyMock.expect(mdObserver.getTopic("topic1"))
                    .andReturn(topic1);
            EasyMock.expect(mdObserver.getTopic("topic2"))
                    .andReturn(topic2);
            EasyMock.replay(mdObserver);

            Response response1 = request("/topics/topic1", mediatype.header).get();
            assertOKResponse(response1, mediatype.expected);
            final Topic topicResponse1 = response1.readEntity(new GenericType<Topic>() {});
            assertEquals(topic1, topicResponse1);

            Response response2 = request("/topics/topic2", mediatype.header).get();
            final Topic topicResponse2 = response2.readEntity(new GenericType<Topic>() {});
            assertEquals(topic2, topicResponse2);

            EasyMock.verify(mdObserver);
            EasyMock.reset(mdObserver);
        }
    }

    @Test
    public void testGetInvalidTopic() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            EasyMock.expect(mdObserver.getTopic("nonexistanttopic"))
                    .andReturn(null);
            EasyMock.replay(mdObserver);

            Response response = request("/topics/nonexistanttopic", mediatype.header).get();
            assertErrorResponse(Response.Status.NOT_FOUND, response, TopicsResource.MESSAGE_TOPIC_NOT_FOUND, mediatype.expected);

            EasyMock.verify(mdObserver);
            EasyMock.reset(mdObserver);
        }
    }

    @Test
    public void testGetPartitionsResourceInvalidTopic() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            EasyMock.expect(mdObserver.topicExists("nonexistanttopic")).andReturn(false);
            EasyMock.replay(mdObserver);

            Response response = request("/topics/nonexistanttopic/partitions", mediatype.header).get();
            assertErrorResponse(Response.Status.NOT_FOUND, response, TopicsResource.MESSAGE_TOPIC_NOT_FOUND, mediatype.expected);

            EasyMock.verify(mdObserver);
            EasyMock.reset(mdObserver);
        }
    }

    private Response produceToTopic(String topic, String acceptHeader, String requestMediatype, List<TopicProduceRecord> records, final Map<Integer, Long> resultOffsets) {
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

        Response response = request("/topics/" + topic, acceptHeader)
                .post(Entity.entity(request, requestMediatype));

        EasyMock.verify(mdObserver, producerPool);

        return response;
    }

    @Test
    public void testProduceToTopicOnlyValues() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            for(String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
                Response rawResponse = produceToTopic("topic1", mediatype.header, requestMediatype, produceRecordsOnlyValues, produceOffsets);
                assertOKResponse(rawResponse, mediatype.expected);
                ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

                assertEquals(
                        Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
                        response.getOffsets()
                );

                EasyMock.reset(mdObserver, producerPool);
            }
        }
    }

    @Test
    public void testProduceToTopicByKey() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            for(String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
                Response rawResponse = produceToTopic("topic1", mediatype.header, requestMediatype, produceRecordsWithKeys, produceOffsets);
                assertOKResponse(rawResponse, mediatype.expected);
                ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

                assertEquals(
                        Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
                        response.getOffsets()
                );

                EasyMock.reset(mdObserver, producerPool);
            }
        }
    }

    @Test
    public void testProduceToTopicByPartition() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            for(String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
                Response rawResponse = produceToTopic("topic1", mediatype.header, requestMediatype, produceRecordsWithPartitions, produceOffsets);
                assertOKResponse(rawResponse, mediatype.expected);
                ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

                assertEquals(
                        Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
                        response.getOffsets()
                );

                EasyMock.reset(mdObserver, producerPool);
            }
        }
    }

    @Test
    public void testProduceToTopicWithPartitionAndKey() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            for(String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
                Response rawResponse = produceToTopic("topic1", mediatype.header, requestMediatype, produceRecordsWithPartitionsAndKeys, produceOffsets);
                assertOKResponse(rawResponse, mediatype.expected);
                ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

                assertEquals(
                        Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
                        response.getOffsets()
                );

                EasyMock.reset(mdObserver, producerPool);
            }
        }
    }

    @Test
    public void testProduceToTopicFailure() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            for(String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
                // null offsets triggers a generic exception
                Response rawResponse = produceToTopic("topic1", mediatype.header, requestMediatype, produceRecordsWithKeys, null);
                assertErrorResponse(Response.Status.INTERNAL_SERVER_ERROR, rawResponse, Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase(), mediatype.expected);

                EasyMock.reset(mdObserver, producerPool);
            }
        }
    }

    @Test
    public void testProduceInvalidRequest() {
        for(TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            for(String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
                Response response = request("/topics/topic1", mediatype.header)
                        .post(Entity.entity("{}", requestMediatype));
                assertEquals(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE, response.getStatus());
                assertEquals(mediatype.expected, response.getMediaType().toString());

                // Invalid base64 encoding
                response = request("/topics/topic1", mediatype.header)
                        .post(Entity.entity("{\"records\":[{\"value\":\"aGVsbG8==\"}]}", requestMediatype));
                assertEquals(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE, response.getStatus());
                assertEquals(mediatype.expected, response.getMediaType().toString());
            }
        }
    }
}
