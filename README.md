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

Installation
------------

You can download prebuilt versions of the Kafka REST Proxy as part of the
[Confluent Platform](http://confluent.io/downloads/). To install from source,
follow the instructions in the Development section.


Deployment
----------

The REST proxy includes a built-in Jetty server. The wrapper scripts
``bin/kafka-rest-start`` and ``bin/kafka-rest-stop`` are the recommended method of
starting and stopping the service.

Development
-----------

To build a development version, you may need a development versions of
[common](https://github.com/confluentinc/common),
[rest-utils](https://github.com/confluentinc/rest-utils), and
[schema-registry](https://github.com/confluentinc/schema-registry).  After
installing these, you can build the Kafka REST Proxy
with Maven. All the standard lifecycle phases work.

Contribute
----------

- Source Code: https://github.com/confluentinc/kafka-rest
- Issue Tracker: https://github.com/confluentinc/kafka-rest/issues

License
-------

The project is licensed under the Apache 2 license.
