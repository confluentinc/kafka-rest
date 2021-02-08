package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.rest.RestConfigException;
import org.junit.Test;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultKafkaRestContextTest {
    KafkaRestConfig restConfig;

    public DefaultKafkaRestContextTest() throws RestConfigException {
        Properties props = new Properties();
        // Required to satisfy config definition
        props.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5234");
        restConfig = new KafkaRestConfig(props);
    }

    private KafkaRestContext newContext(KafkaRestConfig restConfig) {
        return new DefaultKafkaRestContext(restConfig, null, null, null, null);
    }

    @Test
    public void testGetProducerPoolThreadSafety() throws InterruptedException {
        Set<Object> refs = new CopyOnWriteArraySet<>();
        KafkaRestContext ctx = newContext(restConfig);

        ExecutorService executor = Executors.newFixedThreadPool(100);
        // Captures reference as it's invoked.
        for( int i = 0; i < 100; i++) {
            executor.submit(() ->
                    refs.add(ctx.getProducerPool()));
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

        assertEquals(1, refs.size());
    }

    @Test
    public void testGetAdminClientWrapperThreadSafety() throws InterruptedException {
        Set<Object> refs = new CopyOnWriteArraySet<>();
        KafkaRestContext ctx = newContext(restConfig);

        ExecutorService executor = Executors.newFixedThreadPool(100);
        // Captures reference as it's invoked.
        for( int i = 0; i < 100; i++) {
            executor.submit(() ->
                    refs.add(ctx.getAdminClientWrapper()));
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

        assertEquals(1, refs.size());
    }

    @Test
    public void testGetKafkaConsumerManagerThreadSafety() throws InterruptedException {
        Set<Object> refs = new CopyOnWriteArraySet<>();
        KafkaRestContext ctx = newContext(restConfig);

        ExecutorService executor = Executors.newFixedThreadPool(100);
        // Captures reference as it's invoked.
        for( int i = 0; i < 100; i++) {
            executor.submit(() ->
                    refs.add(ctx.getKafkaConsumerManager()));
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

        assertEquals(1, refs.size());
    }

    @Test
    public void testGetAdminThreadSafety() throws InterruptedException {
        Set<Object> refs = new CopyOnWriteArraySet<>();
        KafkaRestContext ctx = newContext(restConfig);

        ExecutorService executor = Executors.newFixedThreadPool(100);
        // Captures reference as it's invoked.
        for( int i = 0; i < 100; i++) {
            executor.submit(() ->
                    refs.add(ctx.getAdmin()));
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

        assertEquals(1, refs.size());
    }
}
