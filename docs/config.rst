.. _kafkarest_config:

|crest| Configuration Options
=============================

In addition to the settings specified here, the |crest-long| accepts the settings for the
Java producer and consumer (currently the new producer and old/new consumers). Use these to override
the default settings of producers and consumers in the |crest|. When configuration options are
exposed in the |crest-long| API, priority is given to settings in the user request, then to overrides
provided as configuration options, and finally falls back to the default values provided by the
Java Kafka clients.

General
-------

``id``
  Unique ID for the |crest-long| server instance. This is used in generating unique IDs for consumers that do not specify their ID. The ID is empty by default, which makes a single server setup easier to get up and running, but is not safe for multi-server deployments where automatic consumer IDs are used.

  * Type: string
  * Default: ""
  * Importance: high

``bootstrap.servers``
  A list of Kafka brokers to connect to. For example, ``PLAINTEXT://hostname:9092,SSL://hostname2:9092``. This configuration is particularly important when Kafka security is enabled, because Kafka may expose multiple endpoints that all will be stored in |zk|, but |crest|  may need to be configured with just one of those endpoints. The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this list only impacts the initial hosts used to discover the full set of servers. Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), this list need not contain the full set of servers (you may want more than one, though, in case a server is down).


``listeners``
  Comma-separated list of listeners that listen for API requests over either HTTP or HTTPS. If a listener uses HTTPS, the appropriate SSL configuration parameters need to be set as well.

  * Type: list
  * Default: "http://0.0.0.0:8082"
  * Importance: high

``schema.registry.url``
  The base URL for |sr| that should be used by the Avro serializer.

  * Type: string
  * Default: "http://localhost:8081"
  * Importance: high

``zookeeper.connect``
  Specifies the |zk| connection string in the form hostname:port where host and port are the host and port of a |zk| server. To allow connecting through other |zk| nodes when that |zk| machine is down you can also specify multiple hosts in the form hostname1:port1,hostname2:port2,hostname3:port3.

  The server may also have a |zk| chroot path as part of it's |zk| connection string which puts its data under some path in the global |zk| namespace. If so the consumer should use the same chroot path in its connection string. For example to give a chroot path of /chroot/path you would give the connection string as hostname1:port1,hostname2:port2,hostname3:port3/chroot/path.

  * Type: string
  * Default: "localhost:2181"
  * Importance: high

``consumer.request.max.bytes``
  Maximum number of bytes in unencoded message keys and values returned by a single request. This can be used by administrators to limit the memory used by a single consumer and to control the memory usage required to decode responses on clients that cannot perform a streaming decode. Note that the actual payload will be larger due to overhead from base64 encoding the response data and from JSON encoding the entire response.

  * Type: long
  * Default: 67108864
  * Importance: medium

``consumer.threads``
  The maximum number of threads to run consumer requests on. Note that this must be greater than the maximum number of consumers in a single consumer group.
  The sentinel value of -1 allows the number of threads to grow as needed to fulfill active consumer requests. Inactive threads will ultimately be stopped and cleaned up.
  * Type: int
  * Default: 50
  * Importance: medium

``consumer.request.timeout.ms``
  The maximum total time to wait for messages for a request if the maximum number of messages has not yet been reached.

  * Type: int
  * Default: 1000
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

``kafka.rest.resource.extension.class``
  A list of classes to use as RestResourceExtension. Implementing the interface <code>RestResourceExtension</code> allows you to inject user defined resources like filters to |crest|. Typically used to add custom capability like logging, security, etc.

  * Type: list
  * Default: ""
  * Importance: low


Security Configuration Options
------------------------------

|crest| supports SSL for securing communication between REST clients and the |crest| (HTTPS), and both SSL and SASL to secure communication between |crest| and Apache Kafka.

.. _kafka-rest-https-config:

-------------------------------
Configuration Options for HTTPS
-------------------------------

``ssl.keystore.location``
  Used for HTTPS. Location of the keystore file to use for SSL. IMPORTANT: Jetty requires that the key's CN, stored in the keystore, must match the FQDN.

  * Type: string
  * Default: ""
  * Importance: high

``ssl.keystore.password``
  Used for HTTPS. The store password for the keystore file.

  * Type: password
  * Default: ""
  * Importance: high

``ssl.key.password``
  Used for HTTPS. The password of the private key in the keystore file.

  * Type: password
  * Default: ""
  * Importance: high

``ssl.truststore.location``
  Used for HTTPS. Location of the trust store. Required only to authenticate HTTPS clients.

  * Type: string
  * Default: ""
  * Importance: high

``ssl.truststore.password``
  Used for HTTPS. The store password for the trust store file.

  * Type: password
  * Default: ""
  * Importance: high

``ssl.keystore.type``
  Used for HTTPS. The type of keystore file.

  * Type: string
  * Default: "JKS"
  * Importance: medium

``ssl.truststore.type``
  Used for HTTPS. The type of trust store file.

  * Type: string
  * Default: "JKS"
  * Importance: medium

``ssl.protocol``
  Used for HTTPS. The SSL protocol used to generate the SslContextFactory.

  * Type: string
  * Default: "TLS"
  * Importance: medium

``ssl.provider``
  Used for HTTPS. The SSL security provider name. Leave blank to use Jetty's default.

  * Type: string
  * Default: "" (Jetty's default)
  * Importance: medium

``ssl.client.auth``
  Used for HTTPS. Whether or not to require the HTTPS client to authenticate via the server's trust store.

  * Type: boolean
  * Default: false
  * Importance: medium

``ssl.enabled.protocols``
  Used for HTTPS. The list of protocols enabled for SSL connections. Comma-separated list. Leave blank to use Jetty's defaults.

  * Type: list
  * Default: "" (Jetty's default)
  * Importance: medium

``ssl.keymanager.algorithm``
  Used for HTTPS. The algorithm used by the key manager factory for SSL connections. Leave blank to use Jetty's default.

  * Type: string
  * Default: "" (Jetty's default)
  * Importance: low

``ssl.trustmanager.algorithm``
  Used for HTTPS. The algorithm used by the trust manager factory for SSL connections. Leave blank to use Jetty's default.

  * Type: string
  * Default: "" (Jetty's default)
  * Importance: low

``ssl.cipher.suites``
  Used for HTTPS. A list of SSL cipher suites. Comma-separated list. Leave blank to use Jetty's defaults.

  * Type: list
  * Default: "" (Jetty's default)
  * Importance: low

``ssl.endpoint.identification.algorithm``
  Used for HTTPS. The endpoint identification algorithm to validate the server hostname using the server certificate. Leave blank to use Jetty's default.

  * Type: string
  * Default: "" (Jetty's default)
  * Importance: low

------------------------------------------------------------------------------------
Configuration Options for SSL Encryption between |crest| and Apache Kafka Brokers
------------------------------------------------------------------------------------

Note that all the SSL configurations (for |crest| to Broker communication) are prefixed with "client". If you want the configuration to apply just to consumers or just to producers, you can replace the prefix with "consumer" or "producer" respectively.

In addition to these configurations, make sure ``bootstrap.servers`` configuration is set with SSL://host:port end-points, or you'll accidentally open an SSL connection to a non-SSL port.

``client.security.protocol``
Protocol used to communicate with brokers. Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.

  * Type: string
  * Default: PLAINTEXT
  * Importance: high

``client.ssl.key.password``
  The password of the private key in the key store file. This is optional for client.

  * Type: password
  * Default: null
  * Importance: high

``client.ssl.keystore.location``
  The location of the key store file. This is optional for client and can be used for two-way authentication for client.

  * Type: string
  * Default: null
  * Importance: high

``client.ssl.keystore.password``
  The store password for the key store file. This is optional for client and only needed if ssl.keystore.location is configured.

  * Type: password
  * Default: null
  * Importance: high

``client.ssl.truststore.location``
  The location of the trust store file.

  * Type: string
  * Default: null
  * Importance: high

``client.ssl.truststore.password``
  The password for the trust store file.

  * Type: string
  * Default: null
  * Importance: high

``client.ssl.enabled.protocols``
  The list of protocols enabled for SSL connections.

  * Type: list
  * Default: TLSv1.2,TLSv1.1,TLSv1
  * Importance: medium

``client.ssl.keystore.type``
  The file format of the key store file. This is optional for client.

  * Type: string
  * Default: JKS
  * Importance: medium

``client.ssl.protocol``
  The SSL protocol used to generate the SSLContext. Default setting is TLS, which is fine for most cases. Allowed values in recent JVMs are TLS, TLSv1.1 and TLSv1.2. SSL, SSLv2 and SSLv3 may be supported in older JVMs, but their usage is discouraged due to known security vulnerabilities.

  * Type: string
  * Default: TLS
  * Importance: medium

``client.ssl.provider``
  The name of the security provider used for SSL connections. Default value is the default security provider of the JVM.

  * Type: string
  * Default: null
  * Importance: medium

``client.ssl.truststore.type``
  The file format of the trust store file.

  * Type: string
  * Default: JKS
  * Importance: medium

``client.ssl.cipher.suites``
  A list of cipher suites. This is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. By default all the available cipher suites are supported.

  * Type: list
  * Default: null
  * Importance: low

``client.ssl.endpoint.identification.algorithm``
The endpoint identification algorithm to validate server hostname using server certificate.

  * Type: string
  * Default: null
  * Importance: low

``client.ssl.keymanager.algorithm``
  The algorithm used by key manager factory for SSL connections. Default value is the key manager factory algorithm configured for the Java Virtual Machine.

  * Type: string
  * Default: SunX509
  * Importance: low

``client.ssl.secure.random.implementation``
The SecureRandom PRNG implementation to use for SSL cryptography operations.

  * Type: string
  * Default: null
  * Importance: low

``client.ssl.trustmanager.algorithm``
  The algorithm used by trust manager factory for SSL connections. Default value is the trust manager factory algorithm configured for the Java Virtual Machine.

  * Type: string
  * Default: PKIX
  * Importance: low

-----------------------------------------------------------------------------------------
Configuration Options for SASL Authentication between |crest| and Apache Kafka Brokers
-----------------------------------------------------------------------------------------

Kafka SASL configurations are described :ref:`here <kafka_sasl_auth>`.

Note that all the SASL configurations (for |crest| to Broker communication) are prefixed with "client". If you want the configuration to apply just to consumers or just to producers, you can replace the prefix with "consumer" or "producer" respectively.

In addition to these configurations:

* Make sure ``bootstrap.servers`` configuration is set with SASL_PLAINTEXT://host:port (or SASL_SSL://host:port) end-points, or you'll accidentally open an SASL connection to a non-SASL port.
* Pass the name of the JAAS file and the name of Kerberos config file via environment variables to the |crest|. For example:

  .. sourcecode:: bash

    $ export KAFKAREST_OPTS="-Djava.security.auth.login.config=/mnt/security/jaas.conf -Djava.security.krb5.conf=/mnt/security/krb5.conf"; \
    /opt/kafka-rest/bin/kafka-rest-start /mnt/rest.properties 1>> /mnt/rest.log 2>> /mnt/rest.log &


* If you need to access |sr| via https protocol, one would need additional javax.net.ssl.trustStore and javax.net.ssl.trustStorePassword parameters, as shown below:

  .. sourcecode:: bash

    $ export KAFKAREST_OPTS='-Djava.security.auth.login.config=/mnt/security/jaas.conf -Djava.security.krb5.conf=/mnt/security/krb5.conf -Djavax.net.ssl.trustStore=/mnt/security/test.truststore.jks -Djavax.net.ssl.trustStorePassword=test-ts-passwd'; \
   /opt/kafka-rest/bin/kafka-rest-start /mnt/rest.properties 1>> /mnt/rest.log 2>> /mnt/rest.log &

* For more details about krb5.conf file please see `JDK’s Kerberos Requirements <https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html>`_.
* Keep in mind that authenticated and encrypted connection to Apache Kafka will only work when Kafka brokers (and |sr|, if used) are running with appropriate security configuration. Check out the documentation on `Kafka Security </kafka/security.html>`_ and `Schema Registry </schema-registry/docs/security.html>`_.




``client.security.protocol``
  Protocol used to communicate with brokers. Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.

  * Type: string
  * Default: PLAINTEXT
  * Importance: high

``client.sasl.jaas.config``
  JAAS login context parameters for SASL connections in the format used by JAAS configuration files. JAAS configuration file format is described `in Oracle's documentation <http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html>`_. The format for the value is: ' (=)*;'

  * Type: string
  * Default: null
  * Importance: medium


``client.sasl.kerberos.service.name``
  The Kerberos principal name that Kafka runs as. This can be defined either in Kafka's JAAS config or in Kafka's config.

  * Type: string
  * Default: null
  * Importance: medium

``client.sasl.mechanism``
  SASL mechanism used for client connections. This may be any mechanism for which a security provider is available. GSSAPI is the default mechanism.

  * Type: string
  * Default: GSSAPI
  * Importance: medium

``client.sasl.kerberos.kinit.cmd``
  Kerberos kinit command path.

  * Type: string
  * Default: /usr/bin/kinit
  * Importance: low

``client.sasl.kerberos.min.time.before.relogin``
  Login thread sleep time between refresh attempts.

  * Type: long
  * Default: 60000
  * Importance: low

``client.sasl.kerberos.ticket.renew.jitter``
  Percentage of random jitter added to the renewal time.

  * Type: double
  * Default: 0.05
  * Importance: low

``client.sasl.kerberos.ticket.renew.window.factor``
  Login thread will sleep until the specified window factor of time from last refresh to ticket's expiry has been reached, at which time it will try to renew the ticket.

  * Type: double
  * Default: 0.8
  * Importance: low


Interceptor Configuration Options
---------------------------------
|crest| supports interceptor configurations as part of Java new producer and consumer settings.

``producer.interceptor.classes``
  Producer interceptor classes.

  * Type: string
  * Default: ""
  * Importance: low

``consumer.interceptor.classes``
  Consumer interceptor classes.

  * Type: string
  * Default: ""
  * Importance: low
    
For example to enable Confluent Control Center monitoring interceptors:

``consumer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor``
``producer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor``

For more details about the monitoring inteceptors, please see :ref:`Interceptor Configuration <controlcenter_clients>`.
