package io.confluent.kafkarest;

/**
 * Shared, global state for the REST proxy server, including configuration and connection pools.
 */
public class Context {
    private final Config config;
    private final MetadataObserver metadataObserver;
    private final ProducerPool producerPool;

    public Context(Config config, MetadataObserver metadataObserver, ProducerPool producerPool) {
        this.config = config;
        this.metadataObserver = metadataObserver;
        this.producerPool = producerPool;
    }

    public Config getConfig() {
        return config;
    }

    public MetadataObserver getMetadataObserver() {
        return metadataObserver;
    }

    public ProducerPool getProducerPool() {
        return producerPool;
    }
}
