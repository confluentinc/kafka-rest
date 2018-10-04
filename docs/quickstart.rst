.. _kafkarest_quickstart:

|crest| Quick Start
======================

Start by running the |crest| and the services it depends on: |zk|, Kafka, and |sr|.
You can do this in one command with Confluent CLI.

.. include:: ../../includes/cli.rst
    :start-line: 2
    :end-line: 5

.. sourcecode:: bash

   $ confluent start kafka-rest

Each service reads its configuration from its property files under ``etc``.

.. note::

   To manually start each service in its own terminal, run instead:

   .. sourcecode:: bash

      $ bin/zookeeper-server-start ./etc/kafka/zookeeper.properties
      $ bin/kafka-server-start ./etc/kafka/server.properties
      $ bin/kafka-rest-start ./etc/kafka-rest/kafka-rest.properties

      # optional, if you want to use Avro data format
      $ bin/schema-registry-start ./etc/schema-registry/schema-registry.properties

.. ifconfig:: platform_docs

   See the :ref:`Confluent Platform quickstart<quickstart>` for a more detailed explanation of how
   to get these services up and running.

Produce and Consume JSON Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. sourcecode:: bash

   # Produce a message using JSON with the value '{ "foo": "bar" }' to the topic jsontest
   $ curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
         -H "Accept: application/vnd.kafka.v2+json" \
         --data '{"records":[{"value":{"foo":"bar"}}]}' "http://localhost:8082/topics/jsontest"
     {"offsets":[{"partition":0,"offset":0,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":null}

   # Create a consumer for JSON data, starting at the beginning of the topic's
   # log and subscribe to a topic. Then consume some data using the base URL in the first response.
   # Finally, close the consumer with a DELETE to make it leave the group and clean up
   # its resources.
   $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
         --data '{"name": "my_consumer_instance", "format": "json", "auto.offset.reset": "earliest"}' \
         http://localhost:8082/consumers/my_json_consumer
     {"instance_id":"my_consumer_instance",
     "base_uri":"http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance"}

   $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{"topics":["jsontest"]}' \
    http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/subscription
    # No content in response

   $ curl -X GET -H "Accept: application/vnd.kafka.json.v2+json" \
         http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/records
     [{"key":null,"value":{"foo":"bar"},"partition":0,"offset":0,"topic":"jsontest"}]

   $ curl -X DELETE -H "Content-Type: application/vnd.kafka.v2+json" \
         http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance
     # No content in response

Produce and Consume Avro Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. sourcecode:: bash

   # Produce a message using Avro embedded data, including the schema which will
   # be registered with schema registry and used to validate and serialize
   # before storing the data in Kafka
   $ curl -X POST -H "Content-Type: application/vnd.kafka.avro.v2+json" \
         -H "Accept: application/vnd.kafka.v2+json" \
         --data '{"value_schema": "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "testUser"}}]}' \
         "http://localhost:8082/topics/avrotest"

   # You should get the following response:
     {"offsets":[{"partition":0,"offset":0,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":21}

   # Produce a message with Avro key and value.
   # Note that if you use Avro values you must also use Avro keys, but the schemas can differ

   $ curl -X POST -H "Content-Type: application/vnd.kafka.avro.v2+json" \
         -H "Accept: application/vnd.kafka.v2+json" \
         --data '{"key_schema": "{\"name\":\"user_id\"  ,\"type\": \"int\"   }", "value_schema": "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"key" : 1 , "value": {"name": "testUser"}}]}' \
         "http://localhost:8082/topics/avrokeytest2"

   # You should get the following response:
   {"offsets":[{"partition":0,"offset":0,"error_code":null,"error":null}],"key_schema_id":2,"value_schema_id":1}

   # Create a consumer for Avro data, starting at the beginning of the topic's
   # log and subscribe to a topic. Then consume some data from a topic, which is decoded, translated to
   # JSON, and included in the response. The schema used for deserialization is
   # fetched automatically from schema registry. Finally, clean up.
   $ curl -X POST  -H "Content-Type: application/vnd.kafka.v2+json" \
         --data '{"name": "my_consumer_instance", "format": "avro", "auto.offset.reset": "earliest"}' \
         http://localhost:8082/consumers/my_avro_consumer

     {"instance_id":"my_consumer_instance","base_uri":"http://localhost:8082/consumers/my_avro_consumer/instances/my_consumer_instance"}

   $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{"topics":["avrotest"]}' \
    http://localhost:8082/consumers/my_avro_consumer/instances/my_consumer_instance/subscription
    # No content in response

   $ curl -X GET -H "Accept: application/vnd.kafka.avro.v2+json" \
         http://localhost:8082/consumers/my_avro_consumer/instances/my_consumer_instance/records
     [{"key":null,"value":{"name":"testUser"},"partition":0,"offset":1,"topic":"avrotest"}]


   $ curl -X DELETE -H "Content-Type: application/vnd.kafka.v2+json" \
         http://localhost:8082/consumers/my_avro_consumer/instances/my_consumer_instance
   # No content in response

Produce and Consume Binary Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. sourcecode:: bash

   # Produce a message using binary embedded data with value "Kafka" to the topic binarytest
   $ curl -X POST -H "Content-Type: application/vnd.kafka.binary.v2+json" \
         -H "Accept: application/vnd.kafka.v2+json" \
         --data '{"records":[{"value":"S2Fma2E="}]}' "http://localhost:8082/topics/binarytest"
     {"offsets":[{"partition":0,"offset":0,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":null}

   # Create a consumer for binary data, starting at the beginning of the topic's
   # log. Then consume some data from a topic using the base URL in the first response.
   # Finally, close the consumer with a DELETE to make it leave the group and clean up
   # its resources.
   $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
         --data '{"name": "my_consumer_instance", "format": "binary", "auto.offset.reset": "earliest"}' \
         http://localhost:8082/consumers/my_binary_consumer

     {"instance_id":"my_consumer_instance","base_uri":"http://localhost:8082/consumers/my_binary_consumer/instances/my_consumer_instance"}

   $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{"topics":["binarytest"]}' \
    http://localhost:8082/consumers/my_binary_consumer/instances/my_consumer_instance/subscription
    # No content in response

   $ curl -X GET -H "Accept: application/vnd.kafka.binary.v2+json" \
         http://localhost:8082/consumers/my_binary_consumer/instances/my_consumer_instance/records

     [{"key":null,"value":"S2Fma2E=","partition":0,"offset":0,"topic":"binarytest"}]

   $ curl -X DELETE -H "Content-Type: application/vnd.kafka.v2+json" \
         http://localhost:8082/consumers/my_binary_consumer/instances/my_consumer_instance
     # No content in response

Inspect Topic Metadata
~~~~~~~~~~~~~~~~~~~~~~

.. sourcecode:: bash

   # Get a list of topics
   $ curl "http://localhost:8082/topics"
     ["__consumer_offsets","_schemas","avrotest","binarytest","jsontest"]

   # Get info about one topic
   $ curl "http://localhost:8082/topics/avrotest"
     {"name":"avrotest","configs":{},"partitions":[{"partition":0,"leader":0,"replicas":[{"broker":0,"leader":true,"in_sync":true}]}]}

   # Get info about a topic's partitions
   $ curl "http://localhost:8082/topics/avrotest/partitions"
     [{"partition":0,"leader":0,"replicas":[{"broker":0,"leader":true,"in_sync":true}]}]

More Examples
~~~~~~~~~~~~~

For an example that uses |crest| configured with security, see the :ref:`Confluent Platform demo <cp-demo>`.
