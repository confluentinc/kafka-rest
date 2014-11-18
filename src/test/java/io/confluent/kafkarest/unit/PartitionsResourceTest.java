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

import io.confluent.kafkarest.Config;
import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.junit.EmbeddedServerTestHarness;
import io.confluent.kafkarest.resources.PartitionsResource;
import io.confluent.kafkarest.resources.TopicsResource;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

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

    public PartitionsResourceTest() {
        addResource(new TopicsResource(ctx));
        addResource(PartitionsResource.class);
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
}
