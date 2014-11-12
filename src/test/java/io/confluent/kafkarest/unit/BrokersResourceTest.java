package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.Config;
import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.entities.BrokerList;
import io.confluent.kafkarest.junit.ResourceTestRule;
import io.confluent.kafkarest.resources.BrokersResource;
import org.easymock.EasyMock;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.core.GenericType;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BrokersResourceTest {
    private static Config config = new Config();
    private static MetadataObserver mdObserver = EasyMock.createMock(MetadataObserver.class);
    private static Context ctx = new Context(config, mdObserver);

    @ClassRule
    public static final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new BrokersResource(ctx))
            .build();

    @Test
    public void testList() {
        final List<Integer> brokerIds = Arrays.asList(1, 2, 3);
        EasyMock.expect(mdObserver.getBrokerIds()).andReturn(brokerIds);
        EasyMock.replay(mdObserver);

        final BrokerList returnedBrokerIds = resources.client().target("/brokers")
                .request().get(new GenericType<BrokerList>(){});
        assertEquals(brokerIds, returnedBrokerIds.getBrokers());
        EasyMock.verify(mdObserver);
    }
}
