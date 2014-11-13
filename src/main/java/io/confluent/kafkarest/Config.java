package io.confluent.kafkarest;

import java.util.Properties;

/**
 * Settings for the REST proxy server.
 */
public class Config {
    public int port;
    public static final String DEFAULT_PORT = "8080";

    public String zookeeperConnect;
    public static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";

    public String bootstrapServers;
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    public int producerThreads;
    public static final String DEFAULT_PRODUCER_THREADS = "5";

    public Config() {
        this(new Properties());
    }

    public Config(Properties props) {
        port = Integer.parseInt(props.getProperty("port", DEFAULT_PORT));
        zookeeperConnect = props.getProperty("zookeeper.connect", DEFAULT_ZOOKEEPER_CONNECT);
        bootstrapServers = props.getProperty("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
        producerThreads = Integer.parseInt(props.getProperty("producer.threads", DEFAULT_PRODUCER_THREADS));
    }
}
