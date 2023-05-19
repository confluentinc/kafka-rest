# Kafka REST Proxy

The Kafka REST Proxy provides a RESTful interface to a Kafka cluster. It makes it easy to produce and consume data, view the state of the cluster, and perform administrative actions without using the native Kafka protocol or clients. Examples of use cases include reporting data to Kafka from any front-end app built in any language, ingesting data into a stream processing framework that doesn't yet support Kafka, and scripting administrative actions.

## Installation

You can download prebuilt versions of the Kafka REST Proxy as part of the [Confluent Platform](https://www.confluent.io/product/confluent-platform/). 

You can read our full [installation instructions](http://docs.confluent.io/current/installation.html#installation) and the complete [documentation](http://docs.confluent.io/current/kafka-rest/docs/).


To install from source, follow the instructions in the Development section below.

## Deployment

The Kafka REST Proxy includes a built-in Jetty server and can be deployed after being configured to connect to an existing Kafka cluster.

Running ``mvn clean package`` runs all 3 of its assembly targets.
- The ``development`` target assembles all necessary dependencies in a ``kafka-rest/target`` subfolder without packaging them in a distributable format. The wrapper scripts ``bin/kafka-rest-start`` and ``bin/kafka-rest-stop`` can then be used to start and stop the service.
- The ``package`` target is meant to be used in shared dependency environments and omits some dependencies expected to be provided externally. It assembles the other dependencies in a ``kafka-rest/target`` subfolder as well as in distributable archives. The wrapper scripts ``bin/kafka-rest-start`` and ``bin/kafka-rest-stop`` can then be used to start and stop the service.
- The ``standalone`` target packages all necessary dependencies as a distributable JAR that can be run as standard (``java -jar $base-dir/kafka-rest/target/kafka-rest-X.Y.Z-standalone.jar``).

## Quickstart (v3 API)

The following assumes you have Kafka and an instance of the REST Proxy running using the default settings and some topics already created.

The v3 API is the latest version of the API. The cluster ID is a path parameter to enable a REST Proxy to work with multiple Kafka clusters. API responses often contain links to related resources, such as the list of a topic's partitions. The content type is always `application/json`.

### Get the local cluster information
```bash
$ curl http://localhost:8082/v3/clusters

Response:
  {"kind":"KafkaClusterList",
   "metadata":{"self":"http://localhost:8082/v3/clusters","next":null},
   "data":[
    {"kind":"KafkaCluster",
     "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q",
     "resource_name":"crn:///kafka=xFhUvurESIeeCI87SXWR-Q"},
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "controller":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/brokers/0"},
     "acls":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/acls"},
     "brokers":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/brokers"},
     "broker_configs":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/broker-configs"},
     "consumer_groups":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/consumer-groups"},
     "topics":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics"},
     "partition_reassignments":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/-/partitions/-/reassignment"}
    }
   ]
  }
```

The cluster ID in the output is `xFhUvurESIeeCI87SXWR-Q`.

### Get a list of topics
```bash
$ curl http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics

Response:
  {"kind":"KafkaTopicList",
   "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics","next":null},
   "data":[
    {"kind":"KafkaTopic",
     "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest",
     "resource_name":"crn:///kafka=xFhUvurESIeeCI87SXWR-Q/topic=jsontest"},
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "topic_name":"jsontest",
     "is_internal":false,
     "replication_factor":1,
     "partitions_count":1,
     "partitions":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions"},
     "configs":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/configs"},
     "partition_reassignments":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions/-/reassignment"}
    }
   ]
  }
```

### Create a topic
```bash
$ curl -X POST -H "Content-Type:application/json" -d '{"topic_name":"jsontest"}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics

Response:
  {"kind":"KafkaTopic",
   "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest",
   "resource_name":"crn:///kafka=xFhUvurESIeeCI87SXWR-Q/topic=jsontest"},
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "is_internal":false,
   "replication_factor":1,
   "partitions_count":1,
   "partitions":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions"},
   "configs":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/configs"},
   "partition_reassignments":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions/-/reassignment"}
  }
```

### Produce records with JSON data
```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"value":{"type":"JSON","data":{"name":"testUser"}}}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records

Response:
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":0,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"JSON","size":19}
  }
```

In the response, the `error_code` of 200 is an HTTP status code (OK) which indicates the operation was successful. Because you can use this API to stream multiple records into a topic as part of the same request, each record produced has its own error code. To send multiple records, simply concatentate the records like this: 

```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"value":{"type":"JSON","data":"ONE"}} {"value":{"type":"JSON","data":"TWO"}}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records

Response:
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":1,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"JSON","size":5}
  }
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":2,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"JSON","size":5}
  }
```

### Produce records with string data
```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"value":{"type":"STRING","data":"REST"}}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records

Response:
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":2,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"STRING","size":4}
  }
```

The data is treated as a string in UTF-8 encoding and follows JSON rules for escaping special characters.

### Produce records in a batch

As an alternative to streaming mode, you can produce multiple records in a batch. This is not streaming, but it is easier to use with HTTP libraries that expect a straightforward request-response behavior.

Each entry in the batch has a unique identifier (a string of up to 80 characters) which can be used to correlate the responses. The identifiers of the entries in a batch must be unique.

```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"entries":[{"id":"first","value":{"type":"JSON","data":"ONE"}}, {"id":"second","value":{"type":"JSON","data":"TWO"}}]}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records:batch

Response:
  {"successes":[
    {"id":"first",
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "topic_name":"jsontest",
     "partition_id":0,
     "offset":3,
     "timestamp":"2023-03-09T14:07:23.592Z",
     "value":{"type":"JSON","size":5}
    },
    {"id":"second",
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "topic_name":"jsontest",
     "partition_id":0,
     "offset":4,
     "timestamp":"2023-03-09T14:07:23.592Z",
     "value":{"type":"JSON","size":5}
    }
   ],
   "failures":[]
  }
```

Successes and failures are returned in the response in separate arrays like this:

```json
{
  "successes": [
    {
      "id": "1",
      "cluster_id": "xFhUvurESIeeCI87SXWR-Q",
      "topic_name": "jsontest",
      "partition_id": 0,
      "offset": 5,
      "timestamp": "2023-03-09T14:07:23.592Z",
      "value": {
        "type": "STRING",
        "size": 7
      }
    }
  ],
  "failures": [
    {
      "id": "2",
      "error_code": 400,
      "message": "Bad Request: data=\"Message$\" is not a valid base64 string."
    }
  ]
}
```

## Quickstart (v2 API)
The earlier v2 API is a bit more concise.

### Get a list of topics
```bash
$ curl http://localhost:8082/topics
  
Response:
  ["__consumer_offsets","jsontest"]
```

### Get info about one topic
```bash
$ curl http://localhost:8082/topics/jsontest

Response:
  {"name":"jsontest",
   "configs":{},
   "partitions":[
    {"partition":0,
     "leader":0,
     "replicas":[
      {"broker":0,
       "leader":true,
       "in_sync":true
      }
     ]
    }
   ]
  }
```

### Produce records with JSON data
```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
       -d '{"records":[{"value":{"name": "testUser"}}]}' \
       http://localhost:8082/topics/jsontest

Response:
  {"offsets":[
    {"partition":0,
     "offset":0,
     "error_code":null,
     "error":null
    }
   ],
   "key_schema_id":null,
   "value_schema_id":null
  }
```

### Consume JSON data
First, create a consumer for JSON data, starting at the beginning of the topic. The consumer group is called `my_json_consumer` and the instance is `my_consumer_instance`.

```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
       -d '{"name": "my_consumer_instance", "format": "json", "auto.offset.reset": "earliest"}' \
       http://localhost:8082/consumers/my_json_consumer

Response:
  {"instance_id":"my_consumer_instance",
   "base_uri":"http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance"
  }
```

Subscribe the consumer to a topic.

```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
       -d '{"topics":["jsontest"]}' \
      http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/subscription

Response:
  # No content in response
```

Then consume some data from a topic using the base URL in the first response.

```bash
$ curl -X GET -H "Accept: application/vnd.kafka.json.v2+json" \
       http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/records

Response:
  [
   {"key":null,
    "value":{"name":"testUser"},
    "partition":0,
    "offset":0,
    "topic":"jsontest"
   }
  ]
```   

Finally, close the consumer with a DELETE to make it leave the group and clean up its resources.  
```bash    
$ curl -X DELETE -H "Accept: application/vnd.kafka.v2+json" \
       http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance

Response:
  # No content in response
```

## Development

To build a development version, you may need development versions of [common](https://github.com/confluentinc/common), [rest-utils](https://github.com/confluentinc/rest-utils), and [schema-registry](https://github.com/confluentinc/schema-registry).  After installing these, you can build the Kafka REST Proxy with Maven. All the standard lifecycle phases work.

You can avoid building development versions of dependencies by building on the latest (or earlier) release tag, or `<release>-post` branch, which will reference dependencies available pre-built from the [public repository](http://packages.confluent.io/maven/).  For example, branch `7.3.0-post` can be used as a base for patches for this version.

## Contribute

- Source Code: https://github.com/confluentinc/kafka-rest
- Issue Tracker: https://github.com/confluentinc/kafka-rest/issues

## License

This project is licensed under the [Confluent Community License](LICENSE).
