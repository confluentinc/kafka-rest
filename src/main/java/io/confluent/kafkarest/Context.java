package io.confluent.kafkarest;

/**
 * Shared, global state for the REST proxy server, including configuration and connection pools.
 */
public class Context {
    private final Config config;
    private final MetadataObserver metadataObserver;

    public Context(Config config, MetadataObserver metadataObserver) {
        this.config = config;
        this.metadataObserver = metadataObserver;
    }

    public Config getConfig() {
        return config;
    }

    public MetadataObserver getMetadataObserver() {
        return metadataObserver;
    }
}
