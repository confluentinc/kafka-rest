package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import java.util.concurrent.CopyOnWriteArraySet;
import org.junit.Before;
import org.junit.Test;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class DefaultKafkaRestContextTest {

    private KafkaRestContext context;

    @Before
    public void setUp() {
        Properties props = new Properties();
        // Required to satisfy config definition
        props.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5234");
        KafkaRestConfig restConfig = new KafkaRestConfig(props);

        context = new DefaultKafkaRestContext(restConfig);
    }

    @Test
    public void testGetProducerThreadSafety() throws InterruptedException {
        Set<Object> refs = new CopyOnWriteArraySet<>();

        ExecutorService executor = Executors.newFixedThreadPool(100);
        // Captures reference as it's invoked.
        for (int i = 0; i < 100; i++) {
            executor.submit(() -> refs.add(context.getProducer()));
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

        assertEquals(1, refs.size());
    }

    @Test
    public void testGetKafkaConsumerManagerThreadSafety() throws InterruptedException {
        Set<Object> refs = new CopyOnWriteArraySet<>();

        ExecutorService executor = Executors.newFixedThreadPool(100);
        // Captures reference as it's invoked.
        for (int i = 0; i < 100; i++) {
            executor.submit(() -> refs.add(context.getKafkaConsumerManager()));
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

        assertEquals(1, refs.size());
    }

    @Test
    public void testGetAdminThreadSafety() throws InterruptedException {
        Set<Object> refs = new CopyOnWriteArraySet<>();

        ExecutorService executor = Executors.newFixedThreadPool(100);
        // Captures reference as it's invoked.
        for (int i = 0; i < 100; i++) {
            executor.submit(() -> refs.add(context.getAdmin()));
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

        assertEquals(1, refs.size());
    }
}
