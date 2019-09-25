package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.*;
import io.confluent.kafkarest.entities.*;
import io.confluent.kafkarest.resources.ConsumerGroupsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import scala.Option;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.*;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class ConsumerGroupsTest extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

    private final GroupMetadataObserver groupMetadataObserver;
    private final ProducerPool producerPool;
    private final DefaultKafkaRestContext ctx;

    public ConsumerGroupsTest() throws RestConfigException {
        groupMetadataObserver = EasyMock.createMock(GroupMetadataObserver.class);
        producerPool = EasyMock.createMock(ProducerPool.class);
        ctx = new DefaultKafkaRestContext(config, producerPool,
                 null, null, groupMetadataObserver, null);

        addResource(new ConsumerGroupsResource(ctx));
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        EasyMock.reset(groupMetadataObserver, producerPool);
    }

    @Test
    public void testListGroups() throws Exception {
        for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            final List<ConsumerGroup> groups =
                    Arrays.asList(new ConsumerGroup("foo", new ConsumerGroupCoordinator("127.0.0.1", 9092)),
                            new ConsumerGroup("bar", new ConsumerGroupCoordinator("127.0.0.1", 9093)));
            EasyMock.expect(groupMetadataObserver.getConsumerGroupList(Option.<Integer>empty(), Option.<Integer>empty()))
              .andReturn(groups);
            EasyMock.replay(groupMetadataObserver);

            Response response = request("/groups", mediatype.header).get();
            assertOKResponse(response, mediatype.expected);
            final List<ConsumerGroup> consumerGroups = TestUtils.tryReadEntityOrLog(response,
                    new GenericType<List<ConsumerGroup>>() {});
            assertEquals(groups.size(), consumerGroups.size());
            assertEquals(groups, consumerGroups);
            EasyMock.verify(groupMetadataObserver);
            EasyMock.reset(groupMetadataObserver, producerPool);
        }
    }

    @Test
    public void testListTopicsByGroup() throws Exception {
        for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            final Set<TopicName> groups = new HashSet<>();
            groups.add(new TopicName("foo"));
            groups.add(new TopicName("bar"));
            EasyMock.expect(groupMetadataObserver.getConsumerGroupTopicInformation("foo", Option.<Integer>empty(), Option.<Integer>empty()))
              .andReturn(groups);
            EasyMock.replay(groupMetadataObserver);

            Response response = request("/groups/foo/topics", mediatype.header).get();
            assertOKResponse(response, mediatype.expected);
            final Set<TopicName> consumerGroups = TestUtils.tryReadEntityOrLog(response,
                    new GenericType<Set<TopicName>>() {});
            assertEquals(groups.size(), consumerGroups.size());
            assertEquals(groups, consumerGroups);
            EasyMock.verify(groupMetadataObserver);
            EasyMock.reset(groupMetadataObserver, producerPool);
        }
    }

    @Test
    public void testTopicGroupOffsets() throws Exception {
        for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            final ConsumerEntity consumerEntity = new ConsumerEntity(Collections.singletonList(new TopicPartitionEntity("cons1", "127.0.0.1", "topic", 0, 2L, 0L, 2L)), 1, new ConsumerGroupCoordinator("127.0.0.1", 9092));
            EasyMock.expect(groupMetadataObserver.getConsumerGroupInformation("foo", Option.apply("topic"), Option.<Integer>empty(), Option.<Integer>empty()))
              .andReturn(consumerEntity);
            EasyMock.replay(groupMetadataObserver);

            Response response = request("/groups/foo/topics/topic", mediatype.header).get();
            assertOKResponse(response, mediatype.expected);
            final ConsumerEntity consumerGroupOffsets = TestUtils.tryReadEntityOrLog(response,
                    new GenericType<ConsumerEntity>() {});
            assertEquals(consumerEntity, consumerGroupOffsets);
            assertEquals(consumerEntity, consumerGroupOffsets);
            EasyMock.verify(groupMetadataObserver);
            EasyMock.reset(groupMetadataObserver, producerPool);
        }
    }

    @Test
    public void testAllTopicsGroupOffsets() throws Exception {
        for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
            final ConsumerEntity consumerEntity = new ConsumerEntity(Arrays.asList(new TopicPartitionEntity("cons1", "127.0.0.1", "topic", 0, 2L, 0L, 2L),
                    new TopicPartitionEntity("cons1", "127.0.0.1", "topic1", 0, 2L, 0L, 2L)), 1, new ConsumerGroupCoordinator("127.0.0.1", 9092));
            EasyMock.expect(groupMetadataObserver.getConsumerGroupInformation("foo"))
              .andReturn(consumerEntity);
            EasyMock.replay(groupMetadataObserver);

            Response response = request("/groups/foo/partitions", mediatype.header).get();
            assertOKResponse(response, mediatype.expected);
            final ConsumerEntity consumerGroupOffsets = TestUtils.tryReadEntityOrLog(response,
                    new GenericType<ConsumerEntity>() {});
            assertEquals(consumerEntity, consumerGroupOffsets);
            assertEquals(consumerEntity, consumerGroupOffsets);
            EasyMock.verify(groupMetadataObserver);
            EasyMock.reset(groupMetadataObserver, producerPool);
        }
    }
}
