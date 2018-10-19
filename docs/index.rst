.. _kafkarest_intro:

|crest-long|
============

The |crest-long| is part of `Confluent Open Source <https://www.confluent.io/product/confluent-open-source/>`_ and `Confluent Enterprise <https://www.confluent.io/product/confluent-enterprise/>`_ distributions. The proxy provides a RESTful interface to a Kafka cluster, making it easy to produce and consume messages, view the state of the cluster, and perform administrative actions without using the native Kafka protocol or clients.

Some example use cases are:

* Reporting data to Kafka from any frontend app built in any language not supported by official `Confluent clients <https://www.confluent.io/clients/>`_
* Ingesting messages into a stream processing framework that doesn’t yet support Kafka
* Scripting administrative actions

Features
--------

Eventually, the REST Proxy should be able to expose all of the functionality
of the Java producers, consumers, and command-line tools. Here is the list of
what is currently supported:

* **Metadata** - Most metadata about the cluster -- brokers, topics,
  partitions, and configs -- can be read using ``GET`` requests for the
  corresponding URLs.

* **Producers** - Instead of exposing producer objects, the API accepts produce
  requests targeted at specific topics or partitions and routes them all through
  a small pool of producers.

  * Producer configuration - Producer instances are shared, so configs cannot
    be set on a per-request basis. However, you can adjust settings globally by
    passing new producer settings in the REST Proxy configuration. For example,
    you might pass in the ``compression.type`` option to enable site-wide
    compression to reduce storage and network overhead.

* **Consumers** - The REST Proxy uses either the high level consumer (v1 api) or the
  new 0.9 consumer (v2 api) to implement consumer-groups that can read from topics.
  Consumers are stateful and therefore tied to specific REST Proxy instances. Offset
  commit can be either automatic or explicitly requested by the user. Currently limited to
  one thread per consumer; use multiple consumers for higher throughput.

  * Consumer configuration - Although consumer instances are not shared, they do
    share the underlying server resources. Therefore, limited configuration
    options are exposed via the API. However, you can adjust settings globally
    by passing consumer settings in the REST Proxy configuration.

* **Data Formats** - The REST Proxy can read and write data using JSON, raw bytes
  encoded with base64 or using JSON-encoded Avro. With Avro, schemas are
  registered and validated against |sr|.

* **REST Proxy Clusters and Load Balancing** - The |crest| is designed to
  support multiple instances running together to spread load and can safely be
  run behind various load balancing mechanisms (e.g. round robin DNS, discovery
  services, load balancers) as long as instances are
  :ref:`configured correctly<kafkarest_deployment>`.
* **Simple Consumer** - The high-level consumer should generally be
  preferred. However, it is occasionally useful to use low-level read
  operations, for example to retrieve messages at specific offsets.

Just as important, here's a list of features that *aren't* yet supported:

* **Admin operations** - We plan to expose these, but must do so carefully, with
  an eye toward security.
* **Multi-topic Produce Requests** - Currently each produce request may only
  address a single topic or topic-partition. Most use cases do not require
  multi-topic produce requests, they introduce additional complexity into the
  API, and clients can easily split data across multiple requests if necessary
* **Most Producer/Consumer Overrides in Requests** - Only a few key overrides are exposed in
  the API (but global overrides can be set by the administrator). The reason is
  two-fold. First, proxies are multi-tenant and therefore most user-requested
  overrides need additional restrictions to ensure they do not impact other
  users. Second, tying the API too much to the implementation restricts future
  API improvements; this is especially important with the new upcoming consumer
  implementation.

Installation
------------

.. ifconfig:: platform_docs

   See the :ref:`installation instructions<installation>` for the Confluent
   Platform. Before starting the |crest| you must start Kafka and |sr|. The
   :ref:`Confluent Platform quickstart<quickstart>` explains how
   to start these services locally for testing.

.. ifconfig:: not platform_docs

   You can download prebuilt versions of the Kafka REST Proxy as part of the
   `Confluent Platform <http://confluent.io/downloads/>`_. To install from
   source, follow the instructions in the `Development`_ section. Before
   starting the |crest| you must start Kafka and |sr|. You can
   find instructions for starting those services in
   `Schema Registry repository <http://github.com/confluentinc/schema-registry>`_.

Deployment
----------

Starting the Kafka REST Proxy service is simple once its dependencies are
running:

.. sourcecode:: bash

   # Start the REST Proxy. The default settings automatically work with the
   # default settings for local ZooKeeper and Kafka nodes.
   $ <path-to-confluent>/bin/kafka-rest-start etc/kafka-rest/kafka-rest.properties

If you installed Debian or RPM packages, you can simply run ``kafka-rest-start``
as it will be on your ``PATH``. The ``kafka-rest.properties`` file contains
:ref:`configuration settings<kafkarest_config>`. The default configuration
included with the REST Proxy includes convenient defaults for a local testing setup
and should be modified for a production deployment. By default the server starts bound to port
8082, does not specify a unique instance ID (required to safely run multiple
proxies concurrently), and expects |zk| to be available at
``localhost:2181`` and |sr| at ``http://localhost:8081``.

If you started the service in the background, you can use the following
command to stop it:

.. sourcecode:: bash

   $ bin/kafka-rest-stop


Development
-----------

To build a development version, you may need a development versions of
`common <https://github.com/confluentinc/common>`_,
`rest-utils <https://github.com/confluentinc/rest-utils>`_, and
`schema-registry <https://github.com/confluentinc/schema-registry>`_.  After
installing these, you can build the Kafka REST Proxy
with Maven. All the standard lifecycle phases work. During development, use

.. sourcecode:: bash

   $ mvn compile

to build,

.. sourcecode:: bash

   $ mvn test

to run the unit and integration tests, and

.. sourcecode:: bash

     $ mvn exec:java

to run an instance of the proxy against a local Kafka cluster (using the default
configuration included with Kafka).

To create a packaged version, optionally skipping the tests:

.. sourcecode:: bash

    $ mvn package [-DskipTests]

This will produce a version ready for production in
``target/kafka-rest-$VERSION-package`` containing a directory layout similar
to the packaged binary versions. You can also produce a standalone fat jar using the
``standalone`` profile:

.. sourcecode:: bash

    $ mvn package -P standalone [-DskipTests]

generating
``target/kafka-rest-$VERSION-standalone.jar``, which includes all the
dependencies as well.

.. toctree::
   :maxdepth: 1
   :hidden:

   quickstart
   api
   config
   ../../cloud/connect/connect-kafka-rest-config
   operations
   security
   changelog
      
