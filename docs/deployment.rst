.. _kafka-rest-deployment:

Production Deployment
---------------------

This section is not meant to be an exhaustive guide to running the |crest-long|, but it
covers the key things to consider before you should consider the proxy production ready.

Three main areas are covered:

* Logistical considerations, such as hardware recommendations and deployment strategies
* Configuration changes that are more suited to a production environment
* Post-deployment considerations

.. toctree::
   :maxdepth: 3

Hardware
~~~~~~~~

If you’ve been following the normal development path, you’ve probably been playing with the |crest| on your laptop.
But when it comes time to deploying to production, there are a few
recommendations that you should consider.

Memory
^^^^^^

The |crest|'s memory usage is primarily tied to the number of consumers because these are the
only stateful resources managed by the proxy. The consumer buffers messages in two ways that
can affect total memory usage. First, the underlying Java consumer buffers up to
``fetch.max.message.bytes x queued.max.message.chunks`` bytes of data, with default values
resulting in 2 MB per consumer. Second, during each consumer request, up to
``consumer.request.max.bytes`` bytes may be buffered before the response is returned; the default
value is 64 MB. In practice, the average memory usage per consumer is closer to the first value
because most consumers will either have a steady stream of data, in which case requests return
quickly instead of buffering up to ``consumer.request.max.bytes`` byte or they have little data
coming through and therefore use little buffer space.

All produce requests are processed by a single set of producers, one per data format. Each has a
buffer of records waiting to be sent, by default 32 MB each. With the current default producer
settings and two data formats (binary and Avro), this requires only 64 MB. If you are using Avro,
the serializer in the producer and deserializers in consumers also maintain a cache of schemas.
However, schemas are relatively small and so should not significantly affect memory usage.

If you plan to use the |crest| mainly for administrative actions or producing data to Kafka,
the memory requirements are modest, and a heap size of 1GB would suffice. If you plan to use many
consumers, you can do a back of the envelope calculation to determine a reasonable heap size
based on the maximum number of consumers you expect and average memory usage of ~16 MB per
consumer when using the default configuration.

CPUs
^^^^

The CPU requirements for the |crest| mirror those of normal clients: the major computational
costs come from compression and serialization of messages. The |crest| can process many
requests concurrently and can take advantage of more cores if available. We recommend at
least 16 cores, which provides sufficient resources to handle HTTP requests in parallel and
background threads for the producers and consumers. However, this should be adjusted for your
workload. Low throughput deployments may use fewer cores, while a proxy that runs many consumers
should use more because each consumer has a dedicated thread.

Disks
^^^^^

The |crest| does not store any state on disk. The only disk usage comes from log4j logs.

Network
^^^^^^^

A fast and reliable network will likely have the biggest impact on the |crest|'s performance.
It should only be used as a proxy for Kafka clusters in the same data center to ensure low
latency access to both |zk| and the Kafka brokers. Standard data center networking (1 GbE,
10 GbE) is sufficient for most applications.

JVM
~~~

We recommend running the latest version of JDK 1.8 with the G1 collector (older freely available
versions have disclosed security vulnerabilities).

If you are still on JDK 1.7 (which is also supported) and you are planning to use G1 (the current
default), make sure you're on u51. We tried out u21 in testing, but we had a number of problems with
the GC implementation in that version.

Our recommended GC tuning looks like this:

.. sourcecode:: bash

   -Xms1g -Xmx1g -XX:MetaspaceSize=96m -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
          -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M \
          -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80

The heap size setting of 1 GB should be increased for proxies that will use many consumers. However,
instead of heap sizes larger than 8 GB we recommend running multiple instances of the |crest|
to avoid long GC pauses that can cause request timeout and consumer disconnections.

.. _kafkarest_deployment:

Deployment
~~~~~~~~~~

The |crest| does not require any coordination between instances, so you can easily scale your
deployment up or down. The only requirement for multiple instances is that you set a unique
``id`` for each instance.

If you run more than one instance of the proxy you should provide some load balancing mechanism.
The simplest approaches use round-robin DNS or a discovery service to select one instance per
application process at startup, sending all traffic to that instance. You can also use an HTTP load
balancer, but individual instances must still be addressable to support the absolute URLs
returned for use in consumer read and offset commit operations.


Important Configuration Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The full set of configuration options are documented :ref:`here<kafkarest_config>` .

However, some configurations should be changed for production. Some **must** be changed
because they depend on your cluster layout:

   ``zookeeper.connect``
     Specifies the |zk| connection string in the form hostname:port where host and port are
     the host and port of a |zk| server. To allow connecting through other |zk| nodes
     when that |zk| machine is down you can also specify multiple hosts in the form
     hostname1:port1,hostname2:port2,hostname3:port3.

     The server may also have a |zk| chroot path as part of its |zk| connection string
     which puts its data under some path in the global |zk| namespace. If so the consumer
     should use the same chroot path in its connection string. For example to give a chroot path
     of /chroot/path you would give the connection string as hostname1:port1,hostname2:port2,
     hostname3:port3/chroot/path.

     * Type: string
     * Default: "localhost:2181"
     * Importance: high

   ``schema.registry.url``
     The base URL for |sr| that should be used by the Avro serializer.

     * Type: string
     * Default: "http://localhost:8081"
     * Importance: high

   ``id``
     Unique ID for this REST server instance. This is used in generating unique IDs for consumers
     that do not specify their ID. The ID is empty by default, which makes a single server setup
     easier to get up and running, but is not safe for multi-server deployments where automatic
     consumer IDs are used.

     * Type: string
     * Default: ""
     * Importance: high


Other settings are important to the health and performance of the proxy. You should consider
changing these based on your specific use case.

   ``consumer.request.max.bytes``
     Maximum number of bytes in message keys and values returned by a single request.
     Smaller values reduce the maximum memory used by a single consumer and may be helpful to
     clients that cannot perform a streaming decode of responses, limiting the maximum memory
     used to decode and process a single JSON payload.

     Conversely, larger values are may be more efficient since many messages can be batched into
     a single request, reducing the number of HTTP requests (and network round trips) required to
     consume the same set of messages.

     Note that this can also be overridden by clients on a per-request basis using the
     ``max_bytes`` query parameter. However, this setting controls the absolute maximum;
     ``max_bytes`` settings exceeding this value will be ignored.

     * Type: long
     * Default: 67108864
     * Importance: medium

   ``consumer.request.timeout.ms``
     The maximum total time to wait for messages for a request if the maximum request size has
     not yet been reached. The consumer uses a timeout to enable batching. A larger value will
     allow the consumer to wait longer, possibly including more messages in the response.
     However, this value is also a lower bound on the latency of consuming a message from Kafka.
     If consumers need low latency message delivery, this setting should be reduced.

     * Type: int
     * Default: 1000
     * Importance: medium

   ``consumer.threads``
     The maximum number of threads to run consumer requests on. Consumers requests are
     ran one per thread in a synchronous manner. It is a must to have this value
     be set higher than the maximum number of consumers in a single consumer group,
     otherwise rebalances will deadlock.

     * Type: int
     * Default: 50
     * Importance: medium

   ``host.name``
     The host name used to generate absolute URLs for consumers. If empty, the default canonical
     hostname is used. You may need to set this value if the FQDN of your host cannot be
     automatically determined.

     * Type: string
     * Default: ""
     * Importance: medium


Don't Touch These Settings!
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Changing the following settings may lead to very poor performance. They have been selected
carefully to balance important performance tradeoffs. If you do need to change them, test the
configuration very thoroughly before putting it into production!

   ``consumer.iterator.backoff.ms``
     Amount of time to backoff when an iterator runs out of data. If a consumer has a dedicated
     worker thread, this is effectively the maximum error for the entire request timeout. It
     should be small enough to closely target the timeout, but large enough to avoid busy waiting.

     * Type: int
     * Default: 50
     * Importance: low

   ``consumer.iterator.timeout.ms``
     Timeout for blocking consumer iterator operations. This should be set to a small enough value
     that it is possible to effectively peek() on the iterator.

     * Type: int
     * Default: 1
     * Importance: low

Post Deployment
~~~~~~~~~~~~~~~

Although the proxy does not have any persistent state, it is stateful because consumer
instances are associated with specific proxy instances. If a proxy process has consumers that are
part of a consumer group, shutting down or restarting that proxy will cause a rebalance operation
for the remaining consumers. This event is expected and isolated instances, for example due to a
hardware failure or network outage, will not cause problems. However, operators should be aware
that this rebalance is not instantaneous and needs to be accounted for in site-wide updates, such
as rolling restarts of all REST proxies for updates.

Upgrades to newer versions are simple because there is no persistent state. A rolling restart of
all servers, leaving sufficient time for rebalance operations as describe above, is a safe way to
perform a zero-downtime upgrade.
