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

import static org.junit.Assert.assertEquals;

public class PartitionsResourceTest extends EmbeddedServerTestHarness {
    private Config config = new Config();
    private MetadataObserver mdObserver = EasyMock.createMock(MetadataObserver.class);
    private ProducerPool producerPool = EasyMock.createMock(ProducerPool.class);
    private Context ctx = new Context(config, mdObserver, producerPool);

    private final String topicName = "topic1";
    Topic topic = new Topic("topic1", 2);
    private final List<Partition> partitions = Arrays.asList(
            new Partition(0, 0, Arrays.asList(
                    new PartitionReplica(0, true, true),
                    new PartitionReplica(1, false, false)
            )),
            new Partition(1, 1, Arrays.asList(
                    new PartitionReplica(0, false, true),
                    new PartitionReplica(1, true, true)
            ))
    );

    private List<ProduceRecord> produceRecordsOnlyValues;
    private List<ProduceRecord> produceRecordsWithKeys;
    // Partition -> New offset
    private Map<Integer,Long> produceOffsets;


    public PartitionsResourceTest() {
        addResource(new TopicsResource(ctx));
        addResource(PartitionsResource.class);

        produceRecordsOnlyValues = Arrays.asList(
                new ProduceRecord("value".getBytes()),
                new ProduceRecord("value2".getBytes())
        );
        produceRecordsWithKeys = Arrays.asList(
                new ProduceRecord("key".getBytes(), "value".getBytes()),
                new ProduceRecord("key2".getBytes(), "value2".getBytes())
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
    public void testGetPartitions() {
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.expect(mdObserver.getTopicPartitions(topicName))
                .andReturn(partitions);

        EasyMock.replay(mdObserver);

        List<Partition> response = getJerseyTest().target("/topics/topic1/partitions")
                .request().get(new GenericType<List<Partition>>(){});
        assertEquals(partitions, response);

        EasyMock.verify(mdObserver);
    }

    @Test
    public void testGetPartition() {
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.expect(mdObserver.getTopicPartition(topicName, 0))
                .andReturn(partitions.get(0));
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.expect(mdObserver.getTopicPartition(topicName, 1))
                .andReturn(partitions.get(1));

        EasyMock.replay(mdObserver);

        Partition response = getJerseyTest().target("/topics/topic1/partitions/0")
                .request().get(new GenericType<Partition>(){});
        assertEquals(partitions.get(0), response);

        response = getJerseyTest().target("/topics/topic1/partitions/1")
                .request().get(new GenericType<Partition>(){});
        assertEquals(partitions.get(1), response);

        EasyMock.verify(mdObserver);
    }

    @Test
    public void testGetInvalidPartition() {
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.expect(mdObserver.getTopicPartition(topicName, 1000))
                .andReturn(null);
        EasyMock.replay(mdObserver);

        Response response = getJerseyTest().target("/topics/topic1/partitions/1000")
                .request().get();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

        EasyMock.verify(mdObserver);
    }



    private Response produceToPartition(String topic, int partition, List<ProduceRecord> records, final Map<Integer, Long> resultOffsets) {
        final PartitionProduceRequest request = new PartitionProduceRequest();
        request.setRecords(records);
        final Capture<ProducerPool.ProduceRequestCallback> produceCallback = new Capture<>();
        EasyMock.expect(mdObserver.topicExists(topic)).andReturn(true);
        EasyMock.expect(mdObserver.partitionExists(topic, partition)).andReturn(true);
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

        Response response = getJerseyTest().target("/topics/" + topic + "/partitions/" + ((Integer)partition).toString())
                .request().post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

        EasyMock.verify(mdObserver, producerPool);

        return response;
    }

    @Test
    public void testProduceToPartitionOnlyValues() {
        Response rawResponse = produceToPartition(topicName, 0, produceRecordsOnlyValues, produceOffsets);
        ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

        assertEquals(
                Arrays.asList(new ProduceResponse.PartitionOffset(0, 1L), new ProduceResponse.PartitionOffset(1, 2L)),
                response.getOffsets()
        );
    }

    @Test
    public void testProduceToPartitionByKey() {
        Response rawResponse = produceToPartition(topicName, 0, produceRecordsWithKeys, produceOffsets);
        ProduceResponse response = rawResponse.readEntity(ProduceResponse.class);

        assertEquals(
                Arrays.asList(new ProduceResponse.PartitionOffset(0, 1L), new ProduceResponse.PartitionOffset(1, 2L)),
                response.getOffsets()
        );
    }

    @Test
    public void testProduceToPartitionFailure() {
        // null offsets triggers a generic exception
        Response rawResponse = produceToPartition(topicName, 0, produceRecordsWithKeys, null);
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), rawResponse.getStatus());
    }

    @Test
    public void testProduceInvalidRequest() {
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.replay(mdObserver);
        Response response = getJerseyTest().target("/topics/" + topicName + "/partitions/0")
                .request().post(Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE));
        assertEquals(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY, response.getStatus());
        EasyMock.verify();

        // Invalid base64 encoding
        EasyMock.reset(mdObserver);
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.replay(mdObserver);
        response = getJerseyTest().target("/topics/" + topicName + "/partitions/0")
                .request().post(Entity.entity("{\"records\":[{\"value\":\"aGVsbG8==\"}]}", MediaType.APPLICATION_JSON_TYPE));
        assertEquals(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY, response.getStatus());
        EasyMock.verify();

        // Invalid data -- include partition in request
        EasyMock.reset(mdObserver);
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.replay(mdObserver);
        TopicProduceRequest topicRequest = new TopicProduceRequest();
        topicRequest.setRecords(Arrays.asList(new TopicProduceRecord("key".getBytes(), "value".getBytes(), 0)));
        response = getJerseyTest().target("/topics/" + topicName + "/partitions/0")
                .request().post(Entity.entity(topicRequest, MediaType.APPLICATION_JSON_TYPE));
        assertEquals(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY, response.getStatus());
        EasyMock.verify();
    }
}
