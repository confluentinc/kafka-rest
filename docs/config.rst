.. _kafkarest_config:

Configuration Options
=====================

In addition to the settings specified here, the Kafka REST Proxy accepts the settings for the
Java producer and consumer (currently the new producer and old consumer). Use these to override
the default settings of producers and consumers in the REST Proxy. When configuration options are
exposed in the REST API, priority is given to settings in the user request, then to overrides
provided as configuration options, and finally falls back to the default values provided by the
Java Kafka clients.


``id``
  Unique ID for this REST server instance. This is used in generating unique IDs for consumers that do not specify their ID. The ID is empty by default, which makes a single server setup easier to get up and running, but is not safe for multi-server deployments where automatic consumer IDs are used.

  * Type: string
  * Default: ""
  * Importance: high

``listeners``
  Comma-separated list of listeners that listen for API requests over either http or https. If a listener uses https, the appropriate SSL configuration parameters need to be set as well.

  * Type: list
  * Default: "http://0.0.0.0:8082"
  * Importance: high

``schema.registry.url``
  The base URL for the schema registry that should be used by the Avro serializer.

  * Type: string
  * Default: "http://localhost:8081"
  * Importance: high

``zookeeper.connect``
  Specifies the ZooKeeper connection string in the form hostname:port where host and port are the host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is down you can also specify multiple hosts in the form hostname1:port1,hostname2:port2,hostname3:port3.

  The server may also have a ZooKeeper chroot path as part of it's ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace. If so the consumer should use the same chroot path in its connection string. For example to give a chroot path of /chroot/path you would give the connection string as hostname1:port1,hostname2:port2,hostname3:port3/chroot/path.

  * Type: string
  * Default: "localhost:2181"
  * Importance: high

``ssl.keystore.location``
  Used for https. Location of the keystore file to use for SSL. IMPORTANT: Jetty requires that the key's CN, stored in the keystore, must match the FQDN.

  * Type: string
  * Default: ""
  * Importance: high

``ssl.keystore.password``
  Used for https. The store password for the keystore file.

  * Type: password
  * Default: ""
  * Importance: high

``ssl.key.password``
  Used for https. The password of the private key in the keystore file.

  * Type: password
  * Default: ""
  * Importance: high

``ssl.truststore.location``
  Used for https. Location of the trust store. Required only to authenticate https clients.

  * Type: string
  * Default: ""
  * Importance: high

``ssl.truststore.password``
  Used for https. The store password for the trust store file.

  * Type: password
  * Default: ""
  * Importance: high

``consumer.request.max.bytes``
  Maximum number of bytes in unencoded message keys and values returned by a single request. This can be used by administrators to limit the memory used by a single consumer and to control the memory usage required to decode responses on clients that cannot perform a streaming decode. Note that the actual payload will be larger due to overhead from base64 encoding the response data and from JSON encoding the entire response.

  * Type: long
  * Default: 67108864
  * Importance: medium

``consumer.request.timeout.ms``
  The maximum total time to wait for messages for a request if the maximum number of messages has not yet been reached.

  * Type: int
  * Default: 1000
  * Importance: medium

``consumer.threads``
  Number of threads to run consumer requests on.

  * Type: int
  * Default: 1
  * Importance: medium

``host.name``
  The host name used to generate absolute URLs in responses. If empty, the default canonical hostname is used

  * Type: string
  * Default: ""
  * Importance: medium

``simpleconsumer.pool.size.max``
  Maximum number of SimpleConsumers that can be instantiated per broker. If 0, then the pool size is not limited.

  * Type: int
  * Default: 25
  * Importance: medium

``ssl.keystore.type``
  Used for https. The type of keystore file.

  * Type: string
  * Default: "JKS"
  * Importance: medium

``ssl.truststore.type``
  Used for https. The type of trust store file.

  * Type: string
  * Default: "JKS"
  * Importance: medium

``ssl.protocol``
  Used for https. The SSL protocol used to generate the SslContextFactory.

  * Type: string
  * Default: "TLS"
  * Importance: medium

``ssl.provider``
  Used for https. The SSL security provider name. Leave blank to use Jetty's default.

  * Type: string
  * Default: "" (Jetty's default)
  * Importance: medium

``ssl.client.auth``
  Used for https. Whether or not to require the https client to authenticate via the server's trust store.

  * Type: boolean
  * Default: false
  * Importance: medium

``ssl.enabled.protocols``
  Used for https. The list of protocols enabled for SSL connections. Comma-separated list. Leave blank to use Jetty's defaults.

  * Type: list
  * Default: "" (Jetty's default)
  * Importance: medium

``access.control.allow.methods``
  Set value to Jetty Access-Control-Allow-Origin header for specified methods

  * Type: string
  * Default: ""
  * Importance: low

``access.control.allow.origin``
  Set value for Jetty Access-Control-Allow-Origin header

  * Type: string
  * Default: ""
  * Importance: low

``consumer.instance.timeout.ms``
  Amount of idle time before a consumer instance is automatically destroyed.

  * Type: int
  * Default: 300000
  * Importance: low

``consumer.iterator.backoff.ms``
  Amount of time to backoff when an iterator runs out of data. If a consumer has a dedicated worker thread, this is effectively the maximum error for the entire request timeout. It should be small enough to closely target the timeout, but large enough to avoid busy waiting.

  * Type: int
  * Default: 50
  * Importance: low

``consumer.iterator.timeout.ms``
  Timeout for blocking consumer iterator operations. This should be set to a small enough value that it is possible to effectively peek() on the iterator.

  * Type: int
  * Default: 1
  * Importance: low

``debug``
  Boolean indicating whether extra debugging information is generated in some error response entities.

  * Type: boolean
  * Default: false
  * Importance: low

``metric.reporters``
  A list of classes to use as metrics reporters. Implementing the <code>MetricReporter</code> interface allows plugging in classes that will be notified of new metric creation. The JmxReporter is always included to register JMX statistics.

  * Type: list
  * Default: []
  * Importance: low

``metrics.jmx.prefix``
  Prefix to apply to metric names for the default JMX reporter.

  * Type: string
  * Default: "kafka.rest"
  * Importance: low

``metrics.num.samples``
  The number of samples maintained to compute metrics.

  * Type: int
  * Default: 2
  * Importance: low

``metrics.sample.window.ms``
  The metrics system maintains a configurable number of samples over a fixed window size. This configuration controls the size of the window. For example we might maintain two samples each measured over a 30 second period. When a window expires we erase and overwrite the oldest window.

  * Type: long
  * Default: 30000
  * Importance: low

``port``
  DEPRECATED: port to listen on for new connections. Use `listeners` instead.

  * Type: int
  * Default: 8082
  * Importance: low

``producer.threads``
  Number of threads to run produce requests on.

  * Type: int
  * Default: 5
  * Importance: low

``request.logger.name``
  Name of the SLF4J logger to write the NCSA Common Log Format request log.

  * Type: string
  * Default: "io.confluent.rest-utils.requests"
  * Importance: low

``response.mediatype.default``
  The default response media type that should be used if no specify types are requested in an Accept header.

  * Type: string
  * Default: "application/vnd.kafka.v1+json"
  * Importance: low

``response.mediatype.preferred``
  An ordered list of the server's preferred media types used for responses, from most preferred to least.

  * Type: list
  * Default: [application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json]
  * Importance: low

``shutdown.graceful.ms``
  Amount of time to wait after a shutdown request for outstanding requests to complete.

  * Type: int
  * Default: 1000
  * Importance: low

``simpleconsumer.pool.timeout.ms``
  Amount of time to wait for an available SimpleConsumer from the pool before failing. Use 0 for no timeout

  * Type: int
  * Default: 1000
  * Importance: low

``ssl.keymanager.algorithm``
  Used for https. The algorithm used by the key manager factory for SSL connections. Leave blank to use Jetty's default.

  * Type: string
  * Default: "" (Jetty's default)
  * Importance: low

``ssl.trustmanager.algorithm``
  Used for https. The algorithm used by the trust manager factory for SSL connections. Leave blank to use Jetty's default.

  * Type: string
  * Default: "" (Jetty's default)
  * Importance: low

``ssl.cipher.suites``
  Used for https. A list of SSL cipher suites. Comma-separated list. Leave blank to use Jetty's defaults.

  * Type: list
  * Default: "" (Jetty's default)
  * Importance: low

``ssl.endpoint.identification.algorithm``
  Used for https. The endpoint identification algorithm to validate the server hostname using the server certificate. Leave blank to use Jetty's default.

  * Type: string
  * Default: "" (Jetty's default)
  * Importance: low
