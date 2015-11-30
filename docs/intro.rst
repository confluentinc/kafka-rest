.. _kafkarest_intro:

Kafka REST Proxy
================

The Kafka REST Proxy provides a RESTful interface to a Kafka cluster. It makes
it easy to produce and consume messages, view the state of the cluster, and
perform administrative actions without using the native Kafka protocol or
clients. Examples of use cases include reporting data to Kafka from any
frontend app built in any language, ingesting messages into a stream processing
framework that doesn't yet support Kafka, and scripting administrative actions.

Quickstart
----------

The following assumes you have Kafka, the schema registry, and an instance of
the REST Proxy running using the default settings and some topics already created.

Inspect Topic Metadata
~~~~~~~~~~~~~~~~~~~~~~

.. sourcecode:: bash

   # Get a list of topics
   $ curl "http://localhost:8082/topics"
     [{"name":"test","num_partitions":3},{"name":"test2","num_partitions":1}]

   # Get info about one topic
   $ curl "http://localhost:8082/topics/test"
     {"name":"test","num_partitions":3}

   # Get info about a topic's partitions
   $ curl "http://localhost:8082/topics/test/partitions
     [{"partition":0,"leader":1002,"replicas":[{"broker":1002,"leader":true,"in_sync":true}]}]

Produce and Consume JSON Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. sourcecode:: bash

   # Produce a message using JSON with the value '{ "foo": "bar" }' to the topic test
   $ curl -X POST -H "Content-Type: application/vnd.kafka.json.v1+json" \
         --data '{"records":[{"value":{"foo":"bar"}}]}' "http://localhost:8082/topics/test"
     {"offsets":[{"partition":0,"offset":0,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":null}

   # Create a consumer for JSON data, starting at the beginning of the topic's
   # log. Then consume some data from a topic using the base URL in the first response.
   # Finally, close the consumer with a DELETE to make it leave the group and clean up
   # its resources.
   $ curl -X POST -H "Content-Type: application/vnd.kafka.v1+json" \
         --data '{"format": "json", "auto.offset.reset": "smallest"}' \
         http://localhost:8082/consumers/my_json_consumer
     {"instance_id":"rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6","base_uri":"http://localhost:8082/consumers/my_json_consumer/instances/rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6"}
   $ curl -X GET -H "Accept: application/vnd.kafka.json.v1+json" \
         http://localhost:8082/consumers/my_json_consumer/instances/rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6/topics/test
     [{"key":null,"value":{"foo":"bar"},"partition":0,"offset":0}]
   $ curl -X DELETE \
         http://localhost:8082/consumers/my_json_consumer/instances/rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6
     # No content in response

Produce and Consume Binary Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. sourcecode:: bash

   # Produce a message using binary embedded data with value "Kafka" to the topic test
   $ curl -X POST -H "Content-Type: application/vnd.kafka.binary.v1+json" \
         --data '{"records":[{"value":"S2Fma2E="}]}' "http://localhost:8082/topics/test"
     {"offsets":[{"partition":0,"offset":0,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":null}

   # Create a consumer for binary data, starting at the beginning of the topic's
   # log. Then consume some data from a topic using the base URL in the first response.
   # Finally, close the consumer with a DELETE to make it leave the group and clean up
   # its resources.
   $ curl -X POST -H "Content-Type: application/vnd.kafka.v1+json" \
         --data '{"format": "binary", "auto.offset.reset": "smallest"}' \
         http://localhost:8082/consumers/my_binary_consumer
     {"instance_id":"rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6","base_uri":"http://localhost:8082/consumers/my_binary_consumer/instances/rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6"}
   $ curl -X GET -H "Accept: application/vnd.kafka.binary.v1+json" \
         http://localhost:8082/consumers/my_binary_consumer/instances/rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6/topics/test
     [{"key":null,"value":"S2Fma2E=","partition":0,"offset":0}]
   $ curl -X DELETE \
         http://localhost:8082/consumers/my_binary_consumer/instances/rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6
     # No content in response

Produce and Consume Avro Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. sourcecode:: bash

   # Produce a message using Avro embedded data, including the schema which will
   # be registered with the schema registry and used to validate and serialize
   # before storing the data in Kafka
   $ curl -X POST -H "Content-Type: application/vnd.kafka.avro.v1+json" \
         --data '{"value_schema": "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "testUser"}}]}' \
         "http://localhost:8082/topics/avrotest"
     {"offsets":[{"partition":0,"offset":0,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":21}

   # Create a consumer for Avro data, starting at the beginning of the topic's
   # log. Then consume some data from a topic, which is decoded, translated to
   # JSON, and included in the response. The schema used for deserialization is
   # fetched automatically from the schema registry. Finally, clean up.
   $ curl -X POST -H "Content-Type: application/vnd.kafka.v1+json" \
         --data '{"format": "avro", "auto.offset.reset": "smallest"}' \
         http://localhost:8082/consumers/my_avro_consumer
     {"instance_id":"rest-consumer-11392f3a-efbe-4fe2-b0bf-5c85d7b25e7b","base_uri":"http://localhost:8082/consumers/my_avro_consumer/instances/rest-consumer-11392f3a-efbe-4fe2-b0bf-5c85d7b25e7b"}
   $ curl -X GET -H "Accept: application/vnd.kafka.avro.v1+json" \
         http://localhost:8082/consumers/my_avro_consumer/instances/rest-consumer-11392f3a-efbe-4fe2-b0bf-5c85d7b25e7b/topics/avrotest
     [{"key":null,"value":{"name":"testUser"},"partition":0,"offset":0}]
   $ curl -X DELETE \
         http://localhost:8082/consumers/my_avro_consumer/instances/rest-consumer-11392f3a-efbe-4fe2-b0bf-5c85d7b25e7b
     # No content in response

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

* **Consumers** - The REST Proxy uses the high level consumer to implement
  consumer-groups that can read from topics. Consumers are stateful and
  therefore tied to specific REST Proxy instances. Offset commit can be either
  automatic or explicitly requested by the user. Currently limited to one thread
  per consumer; use multiple consumers for higher throughput.

  * Consumer configuration - Although consumer instances are not shared, they do
    share the underlying server resources. Therefore, limited configuration
    options are exposed via the API. However, you can adjust settings globally
    by passing consumer settings in the REST Proxy configuration.

* **Data Formats** - The REST Proxy can read and write data using JSON, raw bytes
  encoded with base64 or using JSON-encoded Avro. With Avro, schemas are
  registered and validated against the Schema Registry.
* **REST Proxy Clusters and Load Balancing** - The REST Proxy is designed to
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
* **Multi-threaded Consumers** - Currently consumers subscribe to a single topic
  and use a single stream (and therefore a single thread). You can still
  achieve high throughput as you would with the Java clients: run multiple
  threads locally that each read from a separate consumer stream.
* **Most Producer/Consumer Overrides** - Only a few key overrides are exposed in
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
   Platform. Before starting the REST proxy you must start Kafka and the schema
   registry. The :ref:`Confluent Platform quickstart<quickstart>` explains how
   to start these services locally for testing.

.. ifconfig:: not platform_docs

   You can download prebuilt versions of the Kafka REST Proxy as part of the
   `Confluent Platform <http://confluent.io/downloads/>`_. To install from
   source, follow the instructions in the `Development`_ section. Before
   starting the REST proxy you must start Kafka and the Schema Registry. You can
   find instructions for starting those services in the
   `Schema Registry repository <http://github.com/confluentinc/schema-registry>`_.

Starting the Kafka REST proxy service is simple once its dependencies are
running:

.. sourcecode:: bash

   $ cd confluent-1.0/

   # Start the REST proxy. The default settings automatically work with the
   # default settings for local ZooKeeper and Kafka nodes.
   $ bin/kafka-rest-start

If you installed Debian or RPM packages, you can simply run ``kafka-rest-start``
as it will be on your ``PATH``. If you need to override the default
configuration, add settings to a config file and pass it as an argument when you
start the service:

.. sourcecode:: bash

   $ bin/kafka-rest-start etc/kafka-rest/kafka-rest.properties

Finally, if you started the service in the background, you can use the following
command to stop it:

.. sourcecode:: bash

   $ bin/kafka-rest-stop

Deployment
----------

The REST proxy includes a built-in Jetty server. The wrapper scripts
``bin/kafka-rest-start`` and ``bin/kafka-rest-stop`` are the recommended method of
starting and stopping the service. However, you can also start the server
directly yourself:

.. sourcecode:: bash

   $ java io.confluent.kafkarest.KafkaRestMain [server.properties]

where ``server.properties`` contains configuration settings as specified by the
``KafkaRestConfiguration`` class.
Although the properties file is not required, almost all production deployments
*should* provide one. By default the server starts bound to port
8082, does not specify a unique instance ID (required to safely run multiple
proxies concurrently), and expects Zookeeper to be available at
``localhost:2181``, a Kafka broker at ``localhost:9092``, and the schema
registry at ``http://localhost:8081``.

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

Contribute
----------

- Source Code: https://github.com/confluentinc/kafka-rest
- Issue Tracker: https://github.com/confluentinc/kafka-rest/issues

License
-------

The REST Proxy is licensed under the Apache 2 license.
