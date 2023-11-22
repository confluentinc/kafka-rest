Kafka REST Proxy
================

The Kafka REST Proxy provides a RESTful interface to a Kafka cluster. It makes
it easy to produce and consume messages, view the state of the cluster, and
perform administrative actions without using the native Kafka protocol or
clients. Examples of use cases include reporting data to Kafka from any
frontend app built in any language, ingesting messages into a stream processing
framework that doesn't yet support Kafka, and scripting administrative actions.

Installation
------------

You can download prebuilt versions of the Kafka REST Proxy as part of the
[Confluent Platform](http://confluent.io/downloads/). 

You can read our full [installation instructions](http://docs.confluent.io/current/installation.html#installation) and the complete  [documentation](http://docs.confluent.io/current/kafka-rest/docs/)


To install from source, follow the instructions in the Development section below.

Deployment
----------

The REST proxy includes a built-in Jetty server and can be deployed after
being configured to connect to an existing Kafka cluster.

Running ``mvn clean package`` runs all 3 of its assembly targets.
- The ``development`` target assembles all necessary dependencies in a ``kafka-rest/target``
  subfolder without packaging them in a distributable format. The wrapper scripts
  ``bin/kafka-rest-start`` and ``bin/kafka-rest-stop`` can then be used to start and stop the
  service.
- The ``package`` target is meant to be used in shared dependency environments and omits some
  dependencies expected to be provided externally. It assembles the other dependencies in a
  ``kafka-rest/target`` subfolder as well as in distributable archives. The wrapper scripts
  ``bin/kafka-rest-start`` and ``bin/kafka-rest-stop`` can then be used to start and stop the
  service.
- The ``standalone`` target packages all necessary dependencies as a distributable JAR that can
  be run as standard (``java -jar $base-dir/kafka-rest/target/kafka-rest-X.Y.Z-standalone.jar``).

Quickstart
----------

The following assumes you have Kafka  and an instance of
the REST Proxy running using the default settings and some topics already created.

```bash
    # Get a list of topics
    $ curl "http://localhost:8082/topics"
      
      ["__consumer_offsets","jsontest"]

    # Get info about one topic
    $ curl "http://localhost:8082/topics/jsontest"
    
      {"name":"jsontest","configs":{},"partitions":[{"partition":0,"leader":0,"replicas":[{"broker":0,"leader":true,"in_sync":true}]}]}

    # Produce a message with JSON data
    $ curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
          --data '{"records":[{"value":{"name": "testUser"}}]}' \
          "http://localhost:8082/topics/jsontest"
          
      {"offsets":[{"partition":0,"offset":3,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":null}

    # Create a consumer for JSON data, starting at the beginning of the topic's
    # log. The consumer group is called "my_json_consumer" and the instance is "my_consumer_instance".
    
    $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
    --data '{"name": "my_consumer_instance", "format": "json", "auto.offset.reset": "earliest"}' \
    http://localhost:8082/consumers/my_json_consumer
          
      {"instance_id":"my_consumer_instance","base_uri":"http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance"}
      
    # Subscribe the consumer to a topic
    
    $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{"topics":["jsontest"]}' \
    http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/subscription
    # No content in response
      
    # Then consume some data from a topic using the base URL in the first response.

    $ curl -X GET -H "Accept: application/vnd.kafka.json.v2+json" \
    http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/records
      
      [{"key":null,"value":{"name":"testUser"},"partition":0,"offset":3,"topic":"jsontest"}]
   
    # Finally, close the consumer with a DELETE to make it leave the group and clean up
    # its resources.  
    
    $ curl -X DELETE -H "Accept: application/vnd.kafka.v2+json" \
          http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance
      # No content in response
```

Development
-----------

To build a development version, you may need development versions of
[common](https://github.com/confluentinc/common),
[rest-utils](https://github.com/confluentinc/rest-utils), and
[schema-registry](https://github.com/confluentinc/schema-registry).  After
installing these, you can build the Kafka REST Proxy
with Maven. All the standard lifecycle phases work.

You can avoid building development versions of dependencies
by building on the latest (or earlier) release tag, or `<release>-post` branch,
which will reference dependencies available pre-built from the
[public repository](http://packages.confluent.io/maven/).  For example, branch
`6.1.1-post` can be used as a base for patches for this version.

Contribute
----------

- Source Code: https://github.com/confluentinc/kafka-rest
- Issue Tracker: https://github.com/confluentinc/kafka-rest/issues

License
-------

This project is licensed under the [Confluent Community License](LICENSE).
