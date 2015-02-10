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

.. sourcecode:: bash

   # Get a list of topics
   $ curl "http://localhost:8082/topics"
     [{"name":"test","num_partitions":3},{"name":"test2","num_partitions":1}]

   # Get info about one partition
   $ curl "http://localhost:8082/topics/test"
     {"name":"test","num_partitions":3}

   # Produce a message using binary embedded data with value "Kafka" to the topic test
   $ curl -X POST -H "Content-Type: application/vnd.kafka.binary.v1+json" \
         --data '{"records":[{"value":"S2Fma2E="}]}' "http://localhost:8082/topics/test"
     {"offsets":[{"partition": 3, "offset": 1}]}

   # Produce a message using Avro embedded data, including the schema which will
   # be registered with the schema registry and used to validate and serialize
   # before storing the data in Kafka
   $ curl -X POST -H "Content-Type: application/vnd.kafka.avro.v1+json" \
         --data '{"value_schema": "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "testUser"}}]}' \
         "http://localhost:8082/topics/avrotest"
     {"value_schema_id":0,"offsets":[{"partition":0,"offset":0}]}

   # Create a consumer for binary data, starting at the beginning of the topic's
   # log. Then consume some data from a topic.
   $ curl -X POST -H "Content-Type: application/vnd.kafka.v1+json" \
         --data '{"id": "my_instance", "format": "binary", "auto.offset.reset": "smallest"}' \
         http://localhost:8082/consumers/my_binary_consumer
     {"instance_id":"my_instance","base_uri":"http://localhost:8082/consumers/my_binary_consumer/instances/my_instance"}
   $ curl -X GET -H "Accept: application/vnd.kafka.binary.v1+json" \
         http://localhost:8082/consumers/my_binary_consumer/instances/my_instance/topics/test
     [{"value":"S2Fma2E=","partition":0,"offset":0},{"value":"S2Fma2E=","partition":0,"offset":1}]

   # Create a consumer for Avro data, starting at the beginning of the topic's
   # log. Then consume some data from a topic, which is decoded, translated to
   # JSON, and included in the response. The schema used for deserialization is
   # fetched automatically from the schema registry.
   $ curl -X POST -H "Content-Type: application/vnd.kafka.v1+json" \
         --data '{"id": "my_instance", "format": "avro", "auto.offset.reset": "smallest"}' \
         http://localhost:8082/consumers/my_avro_consumer
     {"instance_id":"my_instance","base_uri":"http://localhost:8082/consumers/my_avro_consumer/instances/my_instance"}
   $ curl -X GET -H "Accept: application/vnd.kafka.avro.v1+json" \
         http://localhost:8082/consumers/my_avro_consumer/instances/my_instance/topics/avrotest
     [{"value":{"name":"testUser"},"partition":0,"offset":0},{"value":{"name":"testUser2"},"partition":0,"offset":1}]



Installation
------------

Release versions are available from the `Central
Repository <http://search.maven.org/#search|ga|1|g%3A%22io.confluent%22%20AND%20a%3A%22kafka-rest%22>`_.

Once installed, start the Kafka REST proxy service and it's dependencies. The
following commands assume you're working with the Confluent Platform; exact
commands may vary slightly depending on your platform or if you're working with
the source repositories.

.. sourcecode:: bash

   # Start a small local Kafka cluster (1 ZK node, 1 Kafka node, and a couple of
   # test topics)
   $ cd confluent-1.0
   $ bin/zookeeper-server-start etc/kafka/zookeeper.properties &
   $ bin/kafka-server-start etc/kafka/server.properties &
   $ bin/kafka-topics --create --zookeeper localhost:2181 \
         --topic test --partitions 1 --replication-factor 1
   $ bin/kafka-topics --create --zookeeper localhost:2181 \
         --topic test2 --partitions 1 --replication-factor 1

   # Start the schema registry. This is only required if you're working with
   # Avro data.
   $ bin/schema-registry-start etc/schema-registry/schema-registry.properties

   # Start the REST proxy. The default settings automatically work with the
   # default settings for local ZooKeeper and Kafka nodes.
   $ bin/kafka-rest-start


Deployment
----------

The REST proxy includes a built-in Jetty server. The wrapper scripts
`bin/kafka-rest-start` and `bin/kafka-rest-stop` are the recommended method of
starting and stopping the service. However, you can also start the server
directly yourself:

.. sourcecode:: bash

   $ java io.confluent.kafkarest.Main [server.properties]

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
`io.confluent.common <https://github.com/confluentinc/common>`_ and
`io.confluent.rest-utils <https://github.com/confluentinc/rest-utils>`_.  After
installing ``common`` and ``rest-utils`` and compiling with Maven, you can run an instance of the
proxy against a local Kafka cluster (using the default configuration included
with Kafka):

.. sourcecode:: bash

    $ mvn exec:java

Contribute
----------

- Source Code: https://github.com/confluentinc/kafka-rest
- Issue Tracker: https://github.com/confluentinc/kafka-rest/issues

License
-------

The project is licensed under the Apache 2 license.
