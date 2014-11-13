package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.Config;
import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.entities.BrokerList;
import io.confluent.kafkarest.junit.EmbeddedServerTestHarness;
import io.confluent.kafkarest.resources.BrokersResource;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.core.GenericType;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BrokersResourceTest extends EmbeddedServerTestHarness {
    private Config config = new Config();
    private MetadataObserver mdObserver = EasyMock.createMock(MetadataObserver.class);
    private ProducerPool producerPool = EasyMock.createMock(ProducerPool.class);
    private Context ctx = new Context(config, mdObserver, producerPool);

    public BrokersResourceTest() {
        addResource(new BrokersResource(ctx));
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        EasyMock.reset(mdObserver, producerPool);
    }

    @Test
    public void testList() {
        final List<Integer> brokerIds = Arrays.asList(1, 2, 3);
        EasyMock.expect(mdObserver.getBrokerIds()).andReturn(brokerIds);
        EasyMock.replay(mdObserver);

        final BrokerList returnedBrokerIds = getJerseyTest().target("/brokers")
                .request().get(new GenericType<BrokerList>(){});
        assertEquals(brokerIds, returnedBrokerIds.getBrokers());
        EasyMock.verify(mdObserver);
    }
}
