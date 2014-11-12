package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.entities.BrokerList;
import org.junit.Test;

import javax.ws.rs.client.ClientBuilder;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BrokersAPITest extends ClusterTestHarness {
    @Test
    public void testBrokers() throws InterruptedException {
        // Listing
        final BrokerList brokers = ClientBuilder.newClient()
                .target(restConnect).path("/brokers")
                .request().get(BrokerList.class);
        assertEquals(new BrokerList(Arrays.asList(0, 1, 2)), brokers);
    }
}
