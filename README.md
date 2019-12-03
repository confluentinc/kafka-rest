
# Kafka REST Proxy

The Kafka REST Proxy provides a RESTful interface to a Kafka cluster. It makes it easy to produce and consume messages, view the state of the cluster, and perform administrative actions without using the native Kafka protocol or clients. Examples of use cases include reporting data to Kafka from any frontend app built in any language, ingesting messages into a stream processing framework that doesn't yet support Kafka, and scripting administrative actions.

## Quickstart

The following assumes you have Kafka, the schema registry, and an instance of the REST Proxy running using the default settings and some topics already created.

### Get a list of topics

```
$ curl "http://localhost:8082/topics"
  [{"name":"test","num_partitions":3},{"name":"test2","num_partitions":1}]
```

### Get info about one topic

```
$ curl "http://localhost:8082/topics/test"
  {"name":"test","num_partitions":3}
```

### Produce a message using binary embedded data with value "Kafka" to the topic test

```
$ curl -X POST -H "Content-Type: application/vnd.kafka.binary.v1+json" \
      --data '{"records":[{"value":"S2Fma2E="}]}' "http://localhost:8082/topics/test"
  {"offsets":[{"partition": 3, "offset": 1}]}
```

### Produce a message using Avro embedded data, including the schema which will be registered with the schema registry and used to validate and serialize before storing the data in Kafka

```
$ curl -X POST -H "Content-Type: application/vnd.kafka.avro.v1+json" \
      --data '{"value_schema": "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "testUser"}}]}' \
      "http://localhost:8082/topics/avrotest"
  {"value_schema_id":0,"offsets":[{"partition":0,"offset":0}]}
```

### Create a consumer for binary data, starting at the beginning of the topic's log.

```
$ curl -X POST -H "Content-Type: application/vnd.kafka.v1+json" \
      --data '{"format": "binary", "auto.offset.reset": "smallest"}' \
      http://localhost:8082/consumers/my_binary_consumer
  {"instance_id":"rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6","base_uri":"http://localhost:8082/consumers/my_binary_consumer/instances/rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6"}
```

Then consume some data from a topic using the base URL in the first response.

```
$ curl -X GET -H "Accept: application/vnd.kafka.binary.v1+json" \
      http://localhost:8082/consumers/my_binary_consumer/instances/rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6/topics/test
  [{"key":null,"value":"S2Fma2E=","partition":0,"offset":0}]
```

Finally, close the consumer with a DELETE to make it leave the group and clean up its resources.

```
$ curl -X DELETE \
      http://localhost:8082/consumers/my_binary_consumer/instances/rest-consumer-11561681-8ba5-4b46-bed0-905ae1769bc6
  # No content in response
```

### Create a consumer for Avro data, starting at the beginning of the topic's log.

```
$ curl -X POST -H "Content-Type: application/vnd.kafka.v1+json" \
      --data '{"format": "avro", "auto.offset.reset": "smallest"}' \
      http://localhost:8082/consumers/my_avro_consumer
  {"instance_id":"rest-consumer-11392f3a-efbe-4fe2-b0bf-5c85d7b25e7b","base_uri":"http://localhost:8082/consumers/my_avro_consumer/instances/rest-consumer-11392f3a-efbe-4fe2-b0bf-5c85d7b25e7b"}
```

Then consume some data from a topic, which is decoded, translated to JSON, and included in the response. The schema used for deserialization is fetched automatically from the schema registry.

```
$ curl -X GET -H "Accept: application/vnd.kafka.avro.v1+json" \
      http://localhost:8082/consumers/my_avro_consumer/instances/rest-consumer-11392f3a-efbe-4fe2-b0bf-5c85d7b25e7b/topics/avrotest
  [{"key":null,"value":{"name":"testUser"},"partition":0,"offset":0}]
```

Finally, clean up.

```
$ curl -X DELETE \
      http://localhost:8082/consumers/my_avro_consumer/instances/rest-consumer-11392f3a-efbe-4fe2-b0bf-5c85d7b25e7b
  # No content in response
```

## Installation

You can download prebuilt versions of the Kafka REST Proxy as part of the [Confluent Platform](http://confluent.io/downloads/). To install from source, follow the instructions in the Development section.

## Deployment

The REST proxy includes a built-in Jetty server. The wrapper scripts `bin/kafka-rest-start` and `bin/kafka-rest-stop` are the recommended method of starting and stopping the service.

## Development

To build a development version, you may need a development versions of [common](https://github.com/confluentinc/common), [rest-utils](https://github.com/confluentinc/rest-utils), and [schema-registry](https://github.com/confluentinc/schema-registry). After installing these, you can build the Kafka REST Proxy with Maven. All the standard lifecycle phases work.

## Contribute

- Source Code: <https://github.com/confluentinc/kafka-rest>
- Issue Tracker: <https://github.com/confluentinc/kafka-rest/issues>

## License
=======
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

The REST proxy includes a built-in Jetty server. The wrapper scripts
``bin/kafka-rest-start`` and ``bin/kafka-rest-stop`` are the recommended method of
starting and stopping the service.

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


This project is licensed under the [Confluent Community License](LICENSE).
