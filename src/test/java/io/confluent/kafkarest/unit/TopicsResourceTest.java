package io.confluent.kafkarest.unit;

import com.sun.xml.internal.ws.developer.MemberSubmissionAddressing;
import io.confluent.kafkarest.Config;
import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.entities.*;
import io.confluent.kafkarest.junit.EmbeddedServerTestHarness;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.kafkarest.validation.ConstraintViolationExceptionMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
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

public class TopicsResourceTest extends EmbeddedServerTestHarness {
    private Config config = new Config();
    private MetadataObserver mdObserver = EasyMock.createMock(MetadataObserver.class);
    private ProducerPool producerPool = EasyMock.createMock(ProducerPool.class);
    private Context ctx = new Context(config, mdObserver, producerPool);

    private List<ProduceRequest.ProduceRecord> produceRecords;
    private Map<Integer,Long> produceOffsets;

    public TopicsResourceTest() {
        addResource(new TopicsResource(ctx));

        produceRecords = Arrays.asList(
                new ProduceRequest.ProduceRecord("key".getBytes(), "value".getBytes()),
                new ProduceRequest.ProduceRecord("key2".getBytes(), "value2".getBytes())
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
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

        EasyMock.verify(mdObserver);
    }

    private Response produceToTopic(String topic, List<ProduceRequest.ProduceRecord> records, final Map<Integer, Long> resultOffsets) {
        final ProduceRequest request = new ProduceRequest();
        request.setRecords(records);
        final Capture<ProducerPool.ProduceRequestCallback> produceCallback = new Capture<>();
        EasyMock.expect(mdObserver.topicExists(topic)).andReturn(true);
        producerPool.produce(EasyMock.<List<ProducerRecord>>anyObject(), EasyMock.capture(produceCallback));
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

        Response response = getJerseyTest().target("/topics/topic1")
                .request().post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

        EasyMock.verify(mdObserver, producerPool);

        return response;
    }

    @Test
    public void testProduceToTopic() {
        Response rawResponse = produceToTopic("topic1", produceRecords, produceOffsets);
        ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

        assertEquals(
                Arrays.asList(new ProduceResponse.PartitionOffset(0, 1L), new ProduceResponse.PartitionOffset(1, 2L)),
                response.getOffsets()
        );
    }

    @Test
    public void testProduceToTopicFailure() {
        Response rawResponse = produceToTopic("topic1", produceRecords, null);
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), rawResponse.getStatus());
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
