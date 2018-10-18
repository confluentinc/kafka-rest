.. _kafkarest_api:

|crest-long| API Reference
==========================

Content Types
^^^^^^^^^^^^^

The REST proxy uses content types for both requests and responses to indicate 3
properties of the data: the serialization format (e.g. ``json``), the version of
the API (e.g. ``v2``), and the *embedded format* (e.g. ``json``, ``binary`` or
``avro``). Currently, the only serialization format supported is ``json`` and
the versions of the API are ``v1`` and ``v2``.

The embedded format is the format of data you are producing or consuming, which
are embedded into requests or responses in the serialization format. For
example, you can provide ``binary`` data in a ``json``-serialized request; in
this case the data should be provided as a base64-encoded string and the content type will be
``application/vnd.kafka.binary.v2+json``. If your data is just JSON, you can use ``json`` as
the embedded format and embed it directly; in this case the content type will be
``application/vnd.kafka.json.v2+json``. The proxy also supports ``avro``, in which case a
JSON form of the data can be embedded directly and a schema (or schema ID) should be included
with the request. If Avro is used, the content type will be ``application/vnd.kafka.avro.v2+json``.

The format for the content type is::

    application/vnd.kafka[.embedded_format].[api_version]+[serialization_format]

The serialization format can be omitted when there are no embedded messages
(i.e. for metadata requests you can use ``application/vnd.kafka.v2+json``). The preferred content type is
``application/vnd.kafka.[embedded_format].v1+json``. However, other less
specific content types are permitted, including ``application/vnd.kafka+json``
to indicate no specific API version requirement (the most recent stable version
will be used), ``application/json``, and ``application/octet-stream``. The
latter two are only supported for compatibility and ease of use. In all cases,
if the embedded format is omitted, ``binary`` is assumed. Although using these
less specific values is permitted, to remain compatible with future versions you
*should* specify preferred content types in requests and check the content types
of responses.

Your requests *should* specify the most specific format and version information
possible via the HTTP ``Accept`` header::

      Accept: application/vnd.kafka.v2+json

The server also supports content negotiation, so you may include multiple,
weighted preferences::

      Accept: application/vnd.kafka.v2+json; q=0.9, application/json; q=0.5

which can be useful when, for example, a new version of the API is preferred but
you cannot be certain it is available yet.

Errors
^^^^^^

All API endpoints use a standard error message format for any requests that
return an HTTP status indicating an error (any 400 or 500 statuses). For
example, a request entity that omits a required field may generate the
following response:

   .. sourcecode:: http

      HTTP/1.1 422 Unprocessable Entity
      Content-Type: application/vnd.kafka.v1+json

      {
          "error_code": 422,
          "message": "records may not be empty"
      }

Although it is good practice to check the status code, you may safely parse the
response of any non-DELETE API calls and check for the presence of an
``error_code`` field to detect errors.

Some error codes are used frequently across the entire API and you will probably want to have
general purpose code to handle these, whereas most other error codes will need to be handled on a
per-request basis.

.. http:any:: *

   :statuscode 404:
          * Error code 40401 -- Topic not found.
          * Error code 40402 -- Partition not found.
   :statuscode 422: The request payload is either improperly formatted or contains semantic errors
   :statuscode 500:
          * Error code 50001 -- Zookeeper error.
          * Error code 50002 -- Kafka error.
          * Error code 50003 -- Retriable Kafka error. Although the operation failed, it's
            possible that retrying the request will be successful.
          * Error code 50101 -- Only SSL endpoints were found for the specified broker, but
            SSL is not supported for the invoked API yet.


API v2
------

Topics
^^^^^^

The topics resource provides information about the topics in your Kafka cluster and their current state. It also lets
you produce messages by making ``POST`` requests to specific topics.

.. http:get:: /topics

   Get a list of Kafka topics.

   :>json array topics: List of topic names

   **Example request**:

   .. sourcecode:: http

      GET /topics HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v2+json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      ["topic1", "topic2"]

.. http:get:: /topics/(string:topic_name)

   Get metadata about a specific topic.

   :param string topic_name: Name of the topic to get metadata about

   :>json string name: Name of the topic
   :>json map configs: Per-topic configuration overrides
   :>json array partitions: List of partitions for this topic
   :>json int partitions[i].partition: the ID of this partition
   :>json int partitions[i].leader: the broker ID of the leader for this partition
   :>json array partitions[i].replicas: list of replicas for this partition,
                                        including the leader
   :>json array partitions[i].replicas[j].broker: broker ID of the replica
   :>json boolean partitions[i].replicas[j].leader: true if this replica is the
                                                    leader for the partition
   :>json boolean partitions[i].replicas[j].in_sync: true if this replica is
                                                     currently in sync with the
                                                     leader

   :statuscode 404:
     * Error code 40401 -- Topic not found

   **Example request**:

   .. sourcecode:: http

      GET /topics/test HTTP/1.1
      Accept: application/vnd.kafka.v2+json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "name": "test",
        "configs": {
           "cleanup.policy": "compact"
        },
        "partitions": [
          {
            "partition": 1,
            "leader": 1,
            "replicas": [
              {
                "broker": 1,
                "leader": true,
                "in_sync": true,
              },
              {
                "broker": 2,
                "leader": false,
                "in_sync": true,
              }
            ]
          },
          {
            "partition": 2,
            "leader": 2,
            "replicas": [
              {
                "broker": 1,
                "leader": false,
                "in_sync": true,
              },
              {
                "broker": 2,
                "leader": true,
                "in_sync": true,
              }
            ]
          }
        ]
      }

.. http:post:: /topics/(string:topic_name)

   Produce messages to a topic, optionally specifying keys or partitions for the
   messages. If no partition is provided, one will be chosen based on the hash of
   the key. If no key is provided, the partition will be chosen for each message
   in a round-robin fashion.

   For the ``avro`` embedded format, you must provide information
   about schemas and the REST proxy must be configured with the URL to access
   |sr| (``schema.registry.url``). Schemas may be provided as
   the full schema encoded as a string, or, after the initial request may be
   provided as the schema ID returned with the first response.

   :param string topic_name: Name of the topic to produce the messages to

   :<json string key_schema: Full schema encoded as a string (e.g. JSON
                             serialized for Avro data)
   :<json int key_schema_id: ID returned by a previous request using the same
                             schema. This ID corresponds to the ID of the schema
                             in the registry.
   :<json string value_schema: Full schema encoded as a string (e.g. JSON
                               serialized for Avro data)
   :<json int value_schema_id: ID returned by a previous request using the same
                               schema. This ID corresponds to the ID of the schema
                               in the registry.
   :<jsonarr records: A list of records to produce to the topic.
   :<jsonarr object records[i].key: The message key, formatted according to the
                                    embedded format, or null to omit a key (optional)
   :<jsonarr object records[i].value: The message value, formatted according to the
                                      embedded format
   :<jsonarr int records[i].partition: Partition to store the message in (optional)

   :>json int key_schema_id: The ID for the schema used to produce keys, or null
                             if keys were not used
   :>json int value_schema_id: The ID for the schema used to produce values.
   :>jsonarr object offsets: List of partitions and offsets the messages were
                             published to
   :>jsonarr int offsets[i].partition: Partition the message was published to, or null if
                                       publishing the message failed
   :>jsonarr long offsets[i].offset: Offset of the message, or null if publishing the message failed
   :>jsonarr long offsets[i].error_code: An error code classifying the reason this operation
                                         failed, or null if it succeeded.

                                         * 1 - Non-retriable Kafka exception
                                         * 2 - Retriable Kafka exception; the message might be sent
                                           successfully if retried
   :>jsonarr string offsets[i].error: An error message describing why the operation failed, or
                                            null if it succeeded

   :statuscode 404:
      * Error code 40401 -- Topic not found
   :statuscode 422:
      * Error code 42201 -- Request includes keys and uses a format that requires schemas, but does
        not include the ``key_schema`` or ``key_schema_id`` fields
      * Error code 42202 -- Request includes values and uses a format that requires schemas, but
        does not include the ``value_schema`` or ``value_schema_id`` fields
      * Error code 42205 -- Request includes invalid schema.

   **Example binary request**:

   .. sourcecode:: http

      POST /topics/test HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.binary.v2+json
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

      {
        "records": [
          {
            "key": "a2V5",
            "value": "Y29uZmx1ZW50"
          },
          {
            "value": "a2Fma2E=",
            "partition": 1
          },
          {
            "value": "bG9ncw=="
          }
        ]
      }

   **Example binary response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "key_schema_id": null,
        "value_schema_id": null,
        "offsets": [
          {
            "partition": 2,
            "offset": 100
          },
          {
            "partition": 1,
            "offset": 101
          },
          {
            "partition": 2,
            "offset": 102
          }
        ]
      }

   **Example Avro request**:

   .. sourcecode:: http

      POST /topics/test HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.avro.v2+json
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

      {
        "value_schema": "{\"name\":\"int\",\"type\": \"int\"}"
        "records": [
          {
            "value": 12
          },
          {
            "value": 24,
            "partition": 1
          }
        ]
      }

   **Example Avro response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "key_schema_id": null,
        "value_schema_id": 32,
        "offsets": [
          {
            "partition": 2,
            "offset": 103
          },
          {
            "partition": 1,
            "offset": 104
          }
        ]
      }


   **Example JSON request**:

   .. sourcecode:: http

      POST /topics/test HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.json.v2+json
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

      {
        "records": [
          {
            "key": "somekey",
            "value": {"foo": "bar"}
          },
          {
            "value": [ "foo", "bar" ],
            "partition": 1
          },
          {
            "value": 53.5
          }
        ]
      }

   **Example JSON response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "key_schema_id": null,
        "value_schema_id": null,
        "offsets": [
          {
            "partition": 2,
            "offset": 100
          },
          {
            "partition": 1,
            "offset": 101
          },
          {
            "partition": 2,
            "offset": 102
          }
        ]
      }

Partitions
^^^^^^^^^^

The partitions resource provides per-partition metadata, including the current leaders and replicas for each partition.
It also allows you to consume and produce messages to single partition using ``GET`` and ``POST`` requests.

.. http:get:: /topics/(string:topic_name)/partitions

   Get a list of partitions for the topic.

   :param string topic_name: the name of the topic

   :>jsonarr int partition: ID of the partition
   :>jsonarr int leader: Broker ID of the leader for this partition
   :>jsonarr array replicas: List of brokers acting as replicas for this partition
   :>jsonarr int replicas[i].broker: Broker ID of the replica
   :>jsonarr boolean replicas[i].leader: true if this broker is the leader for the partition
   :>jsonarr boolean replicas[i].in_sync: true if the replica is in sync with the leader

   :statuscode 404:
      * Error code 40401 -- Topic not found

   **Example request**:

   .. sourcecode:: http

      GET /topics/test/partitions HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      [
        {
          "partition": 1,
          "leader": 1,
          "replicas": [
            {
              "broker": 1,
              "leader": true,
              "in_sync": true,
            },
            {
              "broker": 2,
              "leader": false,
              "in_sync": true,
            },
            {
              "broker": 3,
              "leader": false,
              "in_sync": false,
            }
          ]
        },
        {
          "partition": 2,
          "leader": 2,
          "replicas": [
            {
              "broker": 1,
              "leader": false,
              "in_sync": true,
            },
            {
              "broker": 2,
              "leader": true,
              "in_sync": true,
            },
            {
              "broker": 3,
              "leader": false,
              "in_sync": false,
            }
          ]
        }
      ]


.. http:get:: /topics/(string:topic_name)/partitions/(int:partition_id)

   Get metadata about a single partition in the topic.

   :param string topic_name: Name of the topic
   :param int partition_id: ID of the partition to inspect

   :>json int partition: ID of the partition
   :>json int leader: Broker ID of the leader for this partition
   :>json array replicas: List of brokers acting as replicas for this partition
   :>json int replicas[i].broker: Broker ID of the replica
   :>json boolean replicas[i].leader: true if this broker is the leader for the partition
   :>json boolean replicas[i].in_sync: true if the replica is in sync with the leader

   :statuscode 404:
      * Error code 40401 -- Topic not found
      * Error code 40402 -- Partition not found

   **Example request**:

   .. sourcecode:: http

      GET /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "partition": 1,
        "leader": 1,
        "replicas": [
          {
            "broker": 1,
            "leader": true,
            "in_sync": true,
          },
          {
            "broker": 2,
            "leader": false,
            "in_sync": true,
          },
          {
            "broker": 3,
            "leader": false,
            "in_sync": false,
          }
        ]
      }


.. http:post:: /topics/(string:topic_name)/partitions/(int:partition_id)

   Produce messages to one partition of the topic. For the ``avro`` embedded
   format, you must provide information about schemas. This may be provided as
   the full schema encoded as a string, or, after the initial request may be
   provided as the schema ID returned with the first response.

   :param string topic_name: Topic to produce the messages to
   :param int partition_id: Partition to produce the messages to
   :<json string key_schema: Full schema encoded as a string (e.g. JSON
                             serialized for Avro data)
   :<json int key_schema_id: ID returned by a previous request using the same
                             schema. This ID corresponds to the ID of the schema
                             in the registry.
   :<json string value_schema: Full schema encoded as a string (e.g. JSON
                               serialized for Avro data)
   :<json int value_schema_id: ID returned by a previous request using the same
                               schema. This ID corresponds to the ID of the schema
                               in the registry.
   :<json records: A list of records to produce to the partition.
   :<jsonarr object records[i].key: The message key, formatted according to the
                                    embedded format, or null to omit a key (optional)
   :<jsonarr object records[i].value: The message value, formatted according to the
                                      embedded format

   :>json int key_schema_id: The ID for the schema used to produce keys, or null
                             if keys were not used
   :>json int value_schema_id: The ID for the schema used to produce values.
   :>jsonarr object offsets: List of partitions and offsets the messages were
                             published to
   :>jsonarr int offsets[i].partition: Partition the message was published to. This
                                       will be the same as the ``partition_id``
                                       parameter and is provided only to maintain
                                       consistency with responses from producing to
                                       a topic
   :>jsonarr long offsets[i].offset: Offset of the message
   :>jsonarr long offsets[i].error_code: An error code classifying the reason this operation
                                         failed, or null if it succeeded.

                                         * 1 - Non-retriable Kafka exception
                                         * 2 - Retriable Kafka exception; the message might be sent
                                           successfully if retried
   :>jsonarr string offsets[i].error: An error message describing why the operation failed, or
                                      null if it succeeded

   :statuscode 404:
      * Error code 40401 -- Topic not found
      * Error code 40402 -- Partition not found
   :statuscode 422:
      * Error code 42201 -- Request includes keys and uses a format that requires schemas, but does
        not include the ``key_schema`` or ``key_schema_id`` fields
      * Error code 42202 -- Request includes values and uses a format that requires schemas, but
        does not include the ``value_schema`` or ``value_schema_id`` fields
      * Error code 42205 -- Request includes invalid schema.

   **Example binary request**:

   .. sourcecode:: http

      POST /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.binary.v2+json
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

      {
        "records": [
          {
            "key": "a2V5",
            "value": "Y29uZmx1ZW50"
          },
          {
            "value": "a2Fma2E="
          }
        ]
      }

   **Example binary response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "key_schema_id": null,
        "value_schema_id": null,
        "offsets": [
          {
            "partition": 1,
            "offset": 100,
          },
          {
            "partition": 1,
            "offset": 101,
          }
        ]
      }

   **Example Avro request**:

   .. sourcecode:: http

      POST /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.avro.v2+json
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

      {
        "value_schema": "{\"name\":\"int\",\"type\": \"int\"}"
        "records": [
          {
            "value": 25
          },
          {
            "value": 26
          }
        ]
      }

   **Example Avro response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "key_schema_id": null,
        "value_schema_id": 32,
        "offsets": [
          {
            "partition": 1,
            "offset": 100,
          },
          {
            "partition": 1,
            "offset": 101,
          }
        ]
      }

   **Example JSON request**:

   .. sourcecode:: http

      POST /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.json.v2+json
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

      {
        "records": [
          {
            "key": "somekey",
            "value": {"foo": "bar"}
          },
          {
            "value": 53.5
          }
        ]
      }

   **Example JSON response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "key_schema_id": null,
        "value_schema_id": null,
        "offsets": [
          {
            "partition": 1,
            "offset": 100,
          },
          {
            "partition": 1,
            "offset": 101,
          }
        ]
      }

Consumers
^^^^^^^^^

The consumers resource provides access to the current state of consumer groups, allows you to create a consumer in a
consumer group and consume messages from topics and partitions. The proxy can convert data stored
in Kafka in serialized form into a JSON-compatible embedded format. Currently three formats are
supported: raw binary data is encoded as base64 strings, Avro data is converted into embedded
JSON objects, and JSON is embedded directly.

Because consumers are stateful, any consumer instances created with the REST API are tied to a specific REST proxy
instance. A full URL is provided when the instance is created and it should be used to construct any subsequent
requests. Failing to use the returned URL for future consumer requests will result in `404` errors because the consumer
instance will not be found. If a REST proxy instance is shutdown, it will attempt to cleanly destroy
any consumers before it is terminated.

.. http:post:: /consumers/(string:group_name)

   Create a new consumer instance in the consumer group. The ``format`` parameter controls the
   deserialization of data from Kafka and the content type that *must* be used in the
   ``Accept`` header of subsequent read API requests performed against this consumer. For
   example, if the creation request specifies ``avro`` for the format, subsequent read requests
   should use ``Accept: application/vnd.kafka.avro.v2+json``.

   Note that the response includes a URL including the host since the consumer is stateful and tied
   to a specific REST proxy instance. Subsequent examples in this section use a ``Host`` header
   for this specific REST proxy instance.

   :param string group_name: The name of the consumer group to join
   :<json string name: Name for the consumer instance, which will be used in URLs for the
                       consumer. This must be unique, at least within the proxy process handling
                       the request. If omitted, falls back on the automatically generated ID. Using
                       automatically generated names is recommended for most use cases.
   :<json string format: The format of consumed messages, which is used to convert messages into
                         a JSON-compatible form. Valid values: "binary", "avro", "json". If unspecified,
                         defaults to "binary".
   :<json string auto.offset.reset: Sets the ``auto.offset.reset`` setting for the consumer
   :<json string auto.commit.enable: Sets the ``auto.commit.enable`` setting for the consumer

   :>json string instance_id: Unique ID for the consumer instance in this group.
   :>json string base_uri: Base URI used to construct URIs for subsequent requests against this consumer instance. This
                           will be of the form ``http://hostname:port/consumers/consumer_group/instances/instance_id``.

   :statuscode 409:
         * Error code 40902 -- Consumer instance with the specified name already exists.
   :statuscode 422:
         * Error code 42204 -- Invalid consumer configuration. One of the settings specified in
           the request contained an invalid value.

   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/ HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.v2+json


      {
        "name": "my_consumer",
        "format": "binary",
        "auto.offset.reset": "earliest",
        "auto.commit.enable": "false"
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "instance_id": "my_consumer",
        "base_uri": "http://proxy-instance.kafkaproxy.example.com/consumers/testgroup/instances/my_consumer"
      }

.. http:delete:: /consumers/(string:group_name)/instances/(string:instance)

   Destroy the consumer instance.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :statuscode 404:
     * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      DELETE /consumers/testgroup/instances/my_consumer HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Content-Type: application/vnd.kafka.v2+json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content

.. http:post:: /consumers/(string:group_name)/instances/(string:instance)/offsets

   Commit a list of offsets for the consumer. When the post body is empty, it commits
   all the records that have been fetched by the consumer instance.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance
   :<jsonarr offsets: A list of offsets to commit for partitions
   :<jsonarr string offsets[i].topic: Name of the topic
   :<jsonarr int offsets[i].partition: Partition ID
   :<jsonarr offset: the offset to commit

   :statuscode 404:
     * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/instances/my_consumer/offsets HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Content-Type: application/vnd.kafka.v2+json

      {
        "offsets": [
          {
            "topic": "test",
            "partition": 0,
	    "offset": 20
          },
          {
            "topic": "test",
            "partition": 1,
	    "offset": 30
          }
        ]
      }


.. http:get:: /consumers/(string:group_name)/instances/(string:instance)/offsets

   Get the last committed offsets for the given partitions (whether the commit happened by this process or another).

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :<jsonarr partitions: A list of partitions to find the last committed offsets for
   :<jsonarr string partitions[i].topic: Name of the topic
   :<jsonarr int partitions[i].partition: Partition ID
   :>jsonarr offsets: A list of committed offsets
   :>jsonarr string offsets[i].topic: Name of the topic for which an offset was committed
   :>jsonarr int offsets[i].partition: Partition ID for which an offset was committed
   :>jsonarr int offsets[i].offset: Committed offset
   :>jsonarr string offsets[i].metadata: Metadata for the committed offset

   :statuscode 404:
     * Error code 40402 -- Partition not found
     * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      GET /consumers/testgroup/instances/my_consumer/offsets HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

      {
        "partitions": [
          {
            "topic": "test",
            "partition": 0
          },
          {
            "topic": "test",
            "partition": 1
          }

        ]
      }


   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {"offsets":
       [
        {
          "topic": "test",
          "partition": 0,
          "offset": 21,
	  "metadata":""
        },
        {
          "topic": "test",
          "partition": 1,
          "offset": 31,
	  "metadata":""
        }
       ]
      }


.. http:post:: /consumers/(string:group_name)/instances/(string:instance)/subscription

   Subscribe to the given list of topics or a topic pattern to get dynamically assigned partitions. If a prior subscription exists, it would be replaced by the latest subscription.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance
   :<jsonarr topics: A list of topics to subscribe
   :<jsonarr string topics[i].topic: Name of the topic
   :<json string topic_pattern: A REGEX pattern. topics_pattern and topics fields are mutually exclusive.
   :statuscode 404:
     * Error code 40403 -- Consumer instance not found

   :statuscode 409:
     * Error code 40903 -- Subscription to topics, partitions and pattern are mutually exclusive.


   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/instances/my_consumer/subscription HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Content-Type: application/vnd.kafka.v2+json

      {
        "topics": [
          "test1",
	  "test2"
        ]
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content

   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/instances/my_consumer/subscription HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Content-Type: application/vnd.kafka.v2+json

      {
        "topic_pattern": "test.*"
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content


.. http:get:: /consumers/(string:group_name)/instances/(string:instance)/subscription

   Get the current subscribed list of topics.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance
   :>jsonarr topics: A list of subscribed topics
   :>jsonarr string topics[i]: Name of the topic

   :statuscode 404:
      * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      GET /consumers/testgroup/instances/my_consumer/subscription HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.v2+json


   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "topics": [
          "test1",
	  "test2"
        ]
      }

.. http:delete:: /consumers/(string:group_name)/instances/(string:instance)/subscription

   Unsubscribe from topics currently subscribed.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :statuscode 404:
     * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      DELETE /consumers/testgroup/instances/my_consumer/subscription HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content


.. http:post:: /consumers/(string:group_name)/instances/(string:instance)/assignments

   Manually assign a list of partitions to this consumer.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :<jsonarr partitions: A list of partitions to assign to this consumer
   :<jsonarr string partitions[i].topic: Name of the topic
   :<jsonarr int partitions[i].partition: Partition ID

   :statuscode 404:
     * Error code 40403 -- Consumer instance not found

   :statuscode 409:
     * Error code 40903 -- Subscription to topics, partitions and pattern are mutually exclusive.


   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/instances/my_consumer/assignments HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Content-Type: application/vnd.kafka.v2+json

      {
        "partitions": [
          {
            "topic": "test",
            "partition": 0
          },
          {
            "topic": "test",
            "partition": 1
          }

        ]
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content



.. http:get:: /consumers/(string:group_name)/instances/(string:instance)/assignments

   Get the list of partitions currently manually assigned to this consumer.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :>jsonarr partitions: A list of partitions manually to assign to this consumer
   :>jsonarr string partitions[i].topic: Name of the topic
   :>jsonarr int partitions[i].partition: Partition ID

   :statuscode 404:
     * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      GET /consumers/testgroup/instances/my_consumer/assignments HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.v2+json


   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "partitions": [
          {
            "topic": "test",
            "partition": 0
          },
          {
            "topic": "test",
            "partition": 1
          }

        ]
      }


.. http:post:: /consumers/(string:group_name)/instances/(string:instance)/positions

   Overrides the fetch offsets that the consumer will use for the next set of records to fetch.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :<jsonarr offsets: A list of offsets
   :<jsonarr string offsets[i].topic: Name of the topic for
   :<jsonarr int offsets[i].partition: Partition ID
   :<jsonarr int offsets[i].offset: Seek to offset for the next set of records to fetch

   :statuscode 404:
     * Error code 40403 -- Consumer instance not found


   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/instances/my_consumer/positions HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Content-Type: application/vnd.kafka.v2+json


      {
        "offsets": [
          {
            "topic": "test",
            "partition": 0,
	    "offset": 20
          },
          {
            "topic": "test",
            "partition": 1,
	    "offset": 30
          }
        ]
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content


.. http:post:: /consumers/(string:group_name)/instances/(string:instance)/positions/beginning

   Seek to the first offset for each of the given partitions.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :<jsonarr partitions: A list of partitions
   :<jsonarr string partitions[i].topic: Name of the topic
   :<jsonarr int partitions[i].partition: Partition ID

   :statuscode 404:
     * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/instances/my_consumer/positions/beginning HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Content-Type: application/vnd.kafka.v2+json

      {
        "partitions": [
          {
            "topic": "test",
            "partition": 0
          },
          {
            "topic": "test",
            "partition": 1
          }

        ]
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content


.. http:post:: /consumers/(string:group_name)/instances/(string:instance)/positions/end

   Seek to the last offset for each of the given partitions.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :<jsonarr partitions: A list of partitions
   :<jsonarr string partitions[i].topic: Name of the topic
   :<jsonarr int partitions[i].partition: Partition ID

   :statuscode 404:
     * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/instances/my_consumer/positions/end HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Content-Type: application/vnd.kafka.v2+json

      {
        "partitions": [
          {
            "topic": "test",
            "partition": 0
          },
          {
            "topic": "test",
            "partition": 1
          }

        ]
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content


.. http:get:: /consumers/(string:group_name)/instances/(string:instance)/records

   Fetch data for the topics or partitions specified using one of the subscribe/assign APIs.

   The format of the embedded data returned by this request is determined by the format specified
   in the initial consumer instance creation request and must match the format of the ``Accept``
   header. Mismatches will result in error code ``40601``.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :query timeout: The number of milliseconds for the underlying client library poll(timeout) request to fetch the records. Default to 5000ms.

   :query max_bytes: The maximum number of bytes of unencoded keys and values that should be
                     included in the response. This provides approximate control over the size of
                     responses and the amount of memory required to store the decoded response. The
                     actual limit will be the minimum of this setting and the server-side
                     configuration ``consumer.request.max.bytes``. Default is unlimited.

   :>jsonarr string topic: The topic
   :>jsonarr string key: The message key, formatted according to the embedded format
   :>jsonarr string value: The message value, formatted according to the embedded format
   :>jsonarr int partition: Partition of the message
   :>jsonarr long offset: Offset of the message

   :statuscode 404:
      * Error code 40403 -- Consumer instance not found
   :statuscode 406:
      * Error code 40601 -- Consumer format does not match the embedded format requested by the
        ``Accept`` header.

   **Example binary request**:

   .. sourcecode:: http

      GET /consumers/testgroup/instances/my_consumer/records?timeout=3000&max_bytes=300000 HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.binary.v2+json

   **Example binary response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.binary.v2+json

      [
        {
	  "topic": "test",
          "key": "a2V5",
          "value": "Y29uZmx1ZW50",
          "partition": 1,
          "offset": 100,
        },
        {
	  "topic": "test",
          "key": "a2V5",
          "value": "a2Fma2E=",
          "partition": 2,
          "offset": 101,
        }
      ]

   **Example Avro request**:

   .. sourcecode:: http

      GET /consumers/avrogroup/instances/my_avro_consumer/records?timeout=3000&max_bytes=300000 HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.avro.v2+json

   **Example Avro response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.avro.v2+json

      [
        {
	  "topic": "test",
          "key": 1,
          "value": {
            "id": 1,
            "name": "Bill"
          },
          "partition": 1,
          "offset": 100,
        },
        {
	  "topic": "test",
          "key": 2,
          "value": {
            "id": 2,
            "name": "Melinda"
          },
          "partition": 2,
          "offset": 101,
        }
      ]

   **Example JSON request**:

   .. sourcecode:: http

      GET /consumers/jsongroup/instances/my_json_consumer/records?timeout=3000&max_bytes=300000 HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.json.v2+json

   **Example JSON response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.json.v2+json

      [
        {
	  "topic": "test",
          "key": "somekey",
          "value": {"foo":"bar"},
          "partition": 1,
          "offset": 10,
        },
        {
	  "topic": "test",
          "key": "somekey",
          "value": ["foo", "bar"],
          "partition": 2,
          "offset": 11,
        }
      ]


Brokers
^^^^^^^

The brokers resource provides access to the current state of Kafka brokers in the cluster.

.. http:get:: /brokers

   Get a list of brokers.

   :>json array brokers: List of broker IDs

   **Example request**:

   .. sourcecode:: http

      GET /brokers HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v2+json

      {
        "brokers": [1, 2, 3]
      }

API v1
------

Topics
^^^^^^

The topics resource provides information about the topics in your Kafka cluster and their current state. It also lets
you produce messages by making ``POST`` requests to specific topics.

.. http:get:: /topics

   Get a list of Kafka topics.

   :>json array topics: List of topic names

   **Example request**:

   .. sourcecode:: http

      GET /topics HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      ["topic1", "topic2"]

.. http:get:: /topics/(string:topic_name)

   Get metadata about a specific topic.

   :param string topic_name: Name of the topic to get metadata about

   :>json string name: Name of the topic
   :>json map configs: Per-topic configuration overrides
   :>json array partitions: List of partitions for this topic
   :>json int partitions[i].partition: the ID of this partition
   :>json int partitions[i].leader: the broker ID of the leader for this partition
   :>json array partitions[i].replicas: list of replicas for this partition,
                                        including the leader
   :>json array partitions[i].replicas[j].broker: broker ID of the replica
   :>json boolean partitions[i].replicas[j].leader: true if this replica is the
                                                    leader for the partition
   :>json boolean partitions[i].replicas[j].in_sync: true if this replica is
                                                     currently in sync with the
                                                     leader

   :statuscode 404:
      * Error code 40401 -- Topic not found

   **Example request**:

   .. sourcecode:: http

      GET /topics/test HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "name": "test",
        "configs": {
           "cleanup.policy": "compact"
        },
        "partitions": [
          {
            "partition": 1,
            "leader": 1,
            "replicas": [
              {
                "broker": 1,
                "leader": true,
                "in_sync": true,
              },
              {
                "broker": 2,
                "leader": false,
                "in_sync": true,
              }
            ]
          },
          {
            "partition": 2,
            "leader": 2,
            "replicas": [
              {
                "broker": 1,
                "leader": false,
                "in_sync": true,
              },
              {
                "broker": 2,
                "leader": true,
                "in_sync": true,
              }
            ]
          }
        ]
      }

.. http:post:: /topics/(string:topic_name)

   Produce messages to a topic, optionally specifying keys or partitions for the
   messages. If no partition is provided, one will be chosen based on the hash of
   the key. If no key is provided, the partition will be chosen for each message
   in a round-robin fashion.

   We currently support Avro, JSON and binary message formats.

   For the ``avro`` embedded format, you must provide information
   about schemas and the REST proxy must be configured with the URL to access
   |sr| (``schema.registry.url``). Schemas may be provided as
   the full schema encoded as a string, or, after the initial request may be
   provided as the schema ID returned with the first response. Note that if you use Avro for value you must also use Avro for the key, but the key and value may have different schemas.

   :param string topic_name: Name of the topic to produce the messages to

   :<json string key_schema: Full schema encoded as a string (e.g. JSON
                             serialized for Avro data). This is only needed for Avro format.
   :<json int key_schema_id: ID returned by a previous request using the same
                             schema. This ID corresponds to the ID of the schema
                             in the registry.
   :<json string value_schema: Full schema encoded as a string (e.g. JSON
                               serialized for Avro data).  This is only needed for Avro format.
   :<json int value_schema_id: ID returned by a previous request using the same
                               schema. This ID corresponds to the ID of the schema
                               in the registry.
   :<jsonarr records: A list of records to produce to the topic.
   :<jsonarr object records[i].key: The message key, formatted according to the
                                    embedded format, or null to omit a key (optional)
   :<jsonarr object records[i].value: The message value, formatted according to the
                                      embedded format
   :<jsonarr int records[i].partition: Partition to store the message in (optional)

   :>json int key_schema_id: The ID for the schema used to produce keys, or null
                             if keys were not used
   :>json int value_schema_id: The ID for the schema used to produce values.
   :>jsonarr object offsets: List of partitions and offsets the messages were
                             published to
   :>jsonarr int offsets[i].partition: Partition the message was published to, or null if
                                       publishing the message failed
   :>jsonarr long offsets[i].offset: Offset of the message, or null if publishing the message failed
   :>jsonarr long offsets[i].error_code: An error code classifying the reason this operation
                                         failed, or null if it succeeded.

                                         * 1 - Non-retriable Kafka exception
                                         * 2 - Retriable Kafka exception; the message might be sent
                                           successfully if retried
   :>jsonarr string offsets[i].error: An error message describing why the operation failed, or
                                      null if it succeeded

   :statuscode 404:
      * Error code 40401 -- Topic not found
   :statuscode 422:
      * Error code 42201 -- Request includes keys and uses a format that requires schemas, but does
        not include the ``key_schema`` or ``key_schema_id`` fields
      * Error code 42202 -- Request includes values and uses a format that requires schemas, but
        does not include the ``value_schema`` or ``value_schema_id`` fields
      * Error code 42205 -- Request includes invalid schema.

   **Example binary request**:

   .. sourcecode:: http

      POST /topics/test HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.binary.v1+json
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

      {
        "records": [
          {
            "key": "a2V5",
            "value": "Y29uZmx1ZW50"
          },
          {
            "value": "a2Fma2E=",
            "partition": 1
          },
          {
            "value": "bG9ncw=="
          }
        ]
      }

   **Example binary response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "key_schema_id": null,
        "value_schema_id": null,
        "offsets": [
          {
            "partition": 2,
            "offset": 100
          },
          {
            "partition": 1,
            "offset": 101
          },
          {
            "partition": 2,
            "offset": 102
          }
        ]
      }

   **Example Avro request**:

   .. sourcecode:: http

      POST /topics/test HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.avro.v1+json
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

      {
        "value_schema": "{\"name\":\"int\",\"type\": \"int\"}"
        "records": [
          {
            "value": 12
          },
          {
            "value": 24,
            "partition": 1
          }
        ]
      }

   **Example Avro response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "key_schema_id": null,
        "value_schema_id": 32,
        "offsets": [
          {
            "partition": 2,
            "offset": 103
          },
          {
            "partition": 1,
            "offset": 104
          }
        ]
      }


   **Example JSON request**:

   .. sourcecode:: http

      POST /topics/test HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.json.v1+json
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

      {
        "records": [
          {
            "key": "somekey",
            "value": {"foo": "bar"}
          },
          {
            "value": [ "foo", "bar" ],
            "partition": 1
          },
          {
            "value": 53.5
          }
        ]
      }

   **Example JSON response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "key_schema_id": null,
        "value_schema_id": null,
        "offsets": [
          {
            "partition": 2,
            "offset": 100
          },
          {
            "partition": 1,
            "offset": 101
          },
          {
            "partition": 2,
            "offset": 102
          }
        ]
      }

Partitions
^^^^^^^^^^

The partitions resource provides per-partition metadata, including the current leaders and replicas for each partition.
It also allows you to consume and produce messages to single partition using ``GET`` and ``POST`` requests.

.. http:get:: /topics/(string:topic_name)/partitions

   Get a list of partitions for the topic.

   :param string topic_name: the name of the topic

   :>jsonarr int partition: ID of the partition
   :>jsonarr int leader: Broker ID of the leader for this partition
   :>jsonarr array replicas: List of brokers acting as replicas for this partition
   :>jsonarr int replicas[i].broker: Broker ID of the replica
   :>jsonarr boolean replicas[i].leader: true if this broker is the leader for the partition
   :>jsonarr boolean replicas[i].in_sync: true if the replica is in sync with the leader

   :statuscode 404:
      * Error code 40401 -- Topic not found

    **Example request**:

   .. sourcecode:: http

      GET /topics/test/partitions HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      [
        {
          "partition": 1,
          "leader": 1,
          "replicas": [
            {
              "broker": 1,
              "leader": true,
              "in_sync": true,
            },
            {
              "broker": 2,
              "leader": false,
              "in_sync": true,
            },
            {
              "broker": 3,
              "leader": false,
              "in_sync": false,
            }
          ]
        },
        {
          "partition": 2,
          "leader": 2,
          "replicas": [
            {
              "broker": 1,
              "leader": false,
              "in_sync": true,
            },
            {
              "broker": 2,
              "leader": true,
              "in_sync": true,
            },
            {
              "broker": 3,
              "leader": false,
              "in_sync": false,
            }
          ]
        }
      ]


.. http:get:: /topics/(string:topic_name)/partitions/(int:partition_id)

   Get metadata about a single partition in the topic.

   :param string topic_name: Name of the topic
   :param int partition_id: ID of the partition to inspect

   :>json int partition: ID of the partition
   :>json int leader: Broker ID of the leader for this partition
   :>json array replicas: List of brokers acting as replicas for this partition
   :>json int replicas[i].broker: Broker ID of the replica
   :>json boolean replicas[i].leader: true if this broker is the leader for the partition
   :>json boolean replicas[i].in_sync: true if the replica is in sync with the leader

   :statuscode 404:
      * Error code 40401 -- Topic not found
      * Error code 40402 -- Partition not found

   **Example request**:

   .. sourcecode:: http

      GET /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "partition": 1,
        "leader": 1,
        "replicas": [
          {
            "broker": 1,
            "leader": true,
            "in_sync": true,
          },
          {
            "broker": 2,
            "leader": false,
            "in_sync": true,
          },
          {
            "broker": 3,
            "leader": false,
            "in_sync": false,
          }
        ]
      }

.. http:get:: /topics/(string:topic_name)/partitions/(int:partition_id)/messages?offset=(int)[&count=(int)]

   Consume messages from one partition of the topic.

   :param string topic_name: Topic to consume the messages from
   :param int partition_id: Partition to consume the messages from
   :query int offset: Offset to start from
   :query int count: Number of messages to consume (optional). Default is 1.

   :>jsonarr string key: The message key, formatted according to the embedded format
   :>jsonarr string value: The message value, formatted according to the embedded format
   :>jsonarr int partition: Partition of the message
   :>jsonarr long offset: Offset of the message

   :statuscode 404:
      * Error code 40401 -- Topic not found
      * Error code 40402 -- Partition not found
      * Error code 40404 -- Leader not available
   :statuscode 500:
      * Error code 500 -- General consumer error response, caused by an exception during the
        operation. An error message is included in the standard format which explains the cause.
   :statuscode 503:
      * Error code 50301 -- No SimpleConsumer is available at the time in the pool. The request can be retried.
        You can increase the pool size or the pool timeout to avoid this error in the future.

   **Example binary request**:

   .. sourcecode:: http

      GET /topic/test/partitions/1/messages?offset=10&count=2 HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.binary.v1+json

   **Example binary response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.binary.v1+json

      [
        {
          "key": "a2V5",
          "value": "Y29uZmx1ZW50",
          "partition": 1,
          "offset": 10,
        },
        {
          "key": "a2V5",
          "value": "a2Fma2E=",
          "partition": 1,
          "offset": 11,
        }
      ]

   **Example Avro request**:

   .. sourcecode:: http

      GET /topic/test/partitions/1/messages?offset=1 HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.avro.v1+json

   **Example Avro response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.avro.v1+json

      [
        {
          "key": 1,
          "value": {
            "id": 1,
            "name": "Bill"
          },
          "partition": 1,
          "offset": 1,
        }
      ]

   **Example JSON request**:

   .. sourcecode:: http

      GET /topic/test/partitions/1/messages?offset=10&count=2 HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.json.v1+json

   **Example JSON response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.json.v1+json

      [
        {
          "key": "somekey",
          "value": {"foo":"bar"},
          "partition": 1,
          "offset": 10,
        },
        {
          "key": "somekey",
          "value": ["foo", "bar"],
          "partition": 1,
          "offset": 11,
        }
      ]

.. http:post:: /topics/(string:topic_name)/partitions/(int:partition_id)

   Produce messages to one partition of the topic. For the ``avro`` embedded
   format, you must provide information about schemas. This may be provided as
   the full schema encoded as a string, or, after the initial request may be
   provided as the schema ID returned with the first response.

   :param string topic_name: Topic to produce the messages to
   :param int partition_id: Partition to produce the messages to
   :<json string key_schema: Full schema encoded as a string (e.g. JSON
                             serialized for Avro data)
   :<json int key_schema_id: ID returned by a previous request using the same
                             schema. This ID corresponds to the ID of the schema
                             in the registry.
   :<json string value_schema: Full schema encoded as a string (e.g. JSON
                               serialized for Avro data)
   :<json int value_schema_id: ID returned by a previous request using the same
                               schema. This ID corresponds to the ID of the schema
                               in the registry.
   :<json records: A list of records to produce to the partition.
   :<jsonarr object records[i].key: The message key, formatted according to the
                                    embedded format, or null to omit a key (optional)
   :<jsonarr object records[i].value: The message value, formatted according to the
                                      embedded format

   :>json int key_schema_id: The ID for the schema used to produce keys, or null
                             if keys were not used
   :>json int value_schema_id: The ID for the schema used to produce values.
   :>jsonarr object offsets: List of partitions and offsets the messages were
                             published to
   :>jsonarr int offsets[i].partition: Partition the message was published to. This
                                       will be the same as the ``partition_id``
                                       parameter and is provided only to maintain
                                       consistency with responses from producing to
                                       a topic
   :>jsonarr long offsets[i].offset: Offset of the message
   :>jsonarr long offsets[i].error_code: An error code classifying the reason this operation
                                         failed, or null if it succeeded.

                                         * 1 - Non-retriable Kafka exception
                                         * 2 - Retriable Kafka exception; the message might be sent
                                           successfully if retried
   :>jsonarr string offsets[i].error: An error message describing why the operation failed, or
                                      null if it succeeded

   :statuscode 404:
      * Error code 40401 -- Topic not found
      * Error code 40402 -- Partition not found
   :statuscode 422:
      * Error code 42201 -- Request includes keys and uses a format that requires schemas, but does
        not include the ``key_schema`` or ``key_schema_id`` fields
      * Error code 42202 -- Request includes values and uses a format that requires schemas, but
        does not include the ``value_schema`` or ``value_schema_id`` fields
      * Error code 42205 -- Request includes invalid schema.

   **Example binary request**:

   .. sourcecode:: http

      POST /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.binary.v1+json
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

      {
        "records": [
          {
            "key": "a2V5",
            "value": "Y29uZmx1ZW50"
          },
          {
            "value": "a2Fma2E="
          }
        ]
      }

   **Example binary response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "key_schema_id": null,
        "value_schema_id": null,
        "offsets": [
          {
            "partition": 1,
            "offset": 100,
          },
          {
            "partition": 1,
            "offset": 101,
          }
        ]
      }

   **Example Avro request**:

   .. sourcecode:: http

      POST /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.avro.v1+json
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

      {
        "value_schema": "{\"name\":\"int\",\"type\": \"int\"}"
        "records": [
          {
            "value": 25
          },
          {
            "value": 26
          }
        ]
      }

   **Example Avro response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "key_schema_id": null,
        "value_schema_id": 32,
        "offsets": [
          {
            "partition": 1,
            "offset": 100,
          },
          {
            "partition": 1,
            "offset": 101,
          }
        ]
      }

   **Example JSON request**:

   .. sourcecode:: http

      POST /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Content-Type: application/vnd.kafka.json.v1+json
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

      {
        "records": [
          {
            "key": "somekey",
            "value": {"foo": "bar"}
          },
          {
            "value": 53.5
          }
        ]
      }

   **Example JSON response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "key_schema_id": null,
        "value_schema_id": null,
        "offsets": [
          {
            "partition": 1,
            "offset": 100,
          },
          {
            "partition": 1,
            "offset": 101,
          }
        ]
      }

Consumers
^^^^^^^^^

The consumers resource provides access to the current state of consumer groups, allows you to create a consumer in a
consumer group and consume messages from topics and partitions. The proxy can convert data stored
in Kafka in serialized form into a JSON-compatible embedded format. Currently three formats are
supported: raw binary data is encoded as base64 strings, Avro data is converted into embedded
JSON objects, and JSON is embedded directly.

Because consumers are stateful, any consumer instances created with the REST API are tied to a specific REST proxy
instance. A full URL is provided when the instance is created and it should be used to construct any subsequent
requests. Failing to use the returned URL for future consumer requests will result in `404` errors because the consumer
instance will not be found. If a REST proxy instance is shutdown, it will attempt to cleanly destroy
any consumers before it is terminated.

Consumers may not change the set of topics they are subscribed to once they have
started consuming messages. For example, if a consumer is created without
specifying topic subscriptions, the first read from a topic will subscribe the
consumer to that topic and attempting to read from another topic will cause an
error.

.. http:post:: /consumers/(string:group_name)

   Create a new consumer instance in the consumer group. The ``format`` parameter controls the
   deserialization of data from Kafka and the content type that *must* be used in the
   ``Accept`` header of subsequent read API requests performed against this consumer. For
   example, if the creation request specifies ``avro`` for the format, subsequent read requests
   should use ``Accept: application/vnd.kafka.avro.v1+json``.

   Note that the response includes a URL including the host since the consumer is stateful and tied
   to a specific REST proxy instance. Subsequent examples in this section use a ``Host`` header
   for this specific REST proxy instance.

   :param string group_name: The name of the consumer group to join
   :<json string id: **DEPRECATED** Unique ID for the consumer instance in this group. If omitted,
                     one will be automatically generated
   :<json string name: Name for the consumer instance, which will be used in URLs for the
                       consumer. This must be unique, at least within the proxy process handling
                       the request. If omitted, falls back on the automatically generated ID. Using
                       automatically generated names is recommended for most use cases.
   :<json string format: The format of consumed messages, which is used to convert messages into
                         a JSON-compatible form. Valid values: "binary", "avro", "json". If unspecified,
                         defaults to "binary".
   :<json string auto.offset.reset: Sets the ``auto.offset.reset`` setting for the consumer
   :<json string auto.commit.enable: Sets the ``auto.commit.enable`` setting for the consumer

   :>json string instance_id: Unique ID for the consumer instance in this group. If provided in the initial request,
                              this will be identical to ``id``.
   :>json string base_uri: Base URI used to construct URIs for subsequent requests against this consumer instance. This
                           will be of the form ``http://hostname:port/consumers/consumer_group/instances/instance_id``.

   :statuscode 409:
          * Error code 40902 -- Consumer instance with the specified name already exists.
   :statuscode 422:
          * Error code 42204 -- Invalid consumer configuration. One of the settings specified in
            the request contained an invalid value.

   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/ HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

      {
        "name": "my_consumer",
        "format": "binary",
        "auto.offset.reset": "smallest",
        "auto.commit.enable": "false"
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "instance_id": "my_consumer",
        "base_uri": "http://proxy-instance.kafkaproxy.example.com/consumers/testgroup/instances/my_consumer"
      }

.. http:post:: /consumers/(string:group_name)/instances/(string:instance)/offsets

   Commit offsets for the consumer. Returns a list of the partitions with the committed offsets.

   The body of this request is empty. The offsets are determined by the current state of the consumer instance on the
   proxy. The returned state includes both ``consumed`` and ``committed`` offsets. After a successful commit, these
   should be identical; however, both are included so the output format is consistent with other API calls that return
   the offsets.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :>jsonarr string topic: Name of the topic for which an offset was committed
   :>jsonarr int partition: Partition ID for which an offset was committed
   :>jsonarr long consumed: The offset of the most recently consumed message
   :>jsonarr long committed: The committed offset value. If the commit was successful, this should be identical to
                             ``consumed``.

   :statuscode 404:
      * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/instances/my_consumer/offsets HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      [
        {
          "topic": "test",
          "partition": 1,
          "consumed": 100,
          "committed": 100
        },
        {
          "topic": "test",
          "partition": 2,
          "consumed": 200,
          "committed": 200
        },
        {
          "topic": "test2",
          "partition": 1,
          "consumed": 50,
          "committed": 50
        }
      ]

.. http:delete:: /consumers/(string:group_name)/instances/(string:instance)

   Destroy the consumer instance.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :statuscode 404:
      * Error code 40403 -- Consumer instance not found

   **Example request**:

   .. sourcecode:: http

      DELETE /consumers/testgroup/instances/my_consumer HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content

.. http:get:: /consumers/(string:group_name)/instances/(string:instance)/topics/(string:topic_name)

   Consume messages from a topic. If the consumer is not yet subscribed to the topic, this adds it
   as a subscriber, possibly causing a consumer rebalance.

   The format of the embedded data returned by this request is determined by the format specified
   in the initial consumer instance creation request and must match the format of the ``Accept``
   header. Mismatches will result in error code ``40601``.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance
   :param string topic_name: The topic to consume messages from.
   :query max_bytes: The maximum number of bytes of unencoded keys and values that should be
                     included in the response. This provides approximate control over the size of
                     responses and the amount of memory required to store the decoded response. The
                     actual limit will be the minimum of this setting and the server-side
                     configuration ``consumer.request.max.bytes``. Default is unlimited.

   :>jsonarr string key: The message key, formatted according to the embedded format
   :>jsonarr string value: The message value, formatted according to the embedded format
   :>jsonarr int partition: Partition of the message
   :>jsonarr long offset: Offset of the message

   :statuscode 404:
      * Error code 40401 -- Topic not found
      * Error code 40403 -- Consumer instance not found
   :statuscode 406:
      * Error code 40601 -- Consumer format does not match the embedded format requested by the
        ``Accept`` header.
   :statuscode 409:
      * Error code 40901 -- Consumer has already initiated a subscription. Consumers may
        subscribe to multiple topics, but all subscriptions must be initiated in a single request.
   :statuscode 500:
      * Error code 500 -- General consumer error response, caused by an exception during the
        operation. An error message is included in the standard format which explains the cause.

   **Example binary request**:

   .. sourcecode:: http

      GET /consumers/testgroup/instances/my_consumer/topics/test_topic HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.binary.v1+json

   **Example binary response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.binary.v1+json

      [
        {
          "key": "a2V5",
          "value": "Y29uZmx1ZW50",
          "partition": 1,
          "offset": 100,
          "topic": "test_topic"
        },
        {
          "key": "a2V5",
          "value": "a2Fma2E=",
          "partition": 2,
          "offset": 101,
          "topic": "test_topic"
        }
      ]

   **Example Avro request**:

   .. sourcecode:: http

      GET /consumers/avrogroup/instances/my_avro_consumer/topics/test_avro_topic HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.avro.v1+json

   **Example Avro response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.avro.v1+json

      [
        {
          "key": 1,
          "value": {
            "id": 1,
            "name": "Bill"
          },
          "partition": 1,
          "offset": 100,
          "topic": "test_avro_topic"
        },
        {
          "key": 2,
          "value": {
            "id": 2,
            "name": "Melinda"
          },
          "partition": 2,
          "offset": 101,
          "topic": "test_avro_topic"
        }
      ]

   **Example JSON request**:

   .. sourcecode:: http

      GET /consumers/jsongroup/instances/my_json_consumer/topics/test_json_topic HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.kafka.json.v1+json

   **Example JSON response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.json.v1+json

      [
        {
          "key": "somekey",
          "value": {"foo":"bar"},
          "partition": 1,
          "offset": 10,
          "topic": "test_json_topic"
        },
        {
          "key": "somekey",
          "value": ["foo", "bar"],
          "partition": 2,
          "offset": 11,
          "topic": "test_json_topic"
        }
      ]


Brokers
^^^^^^^

The brokers resource provides access to the current state of Kafka brokers in the cluster.

.. http:get:: /brokers

   Get a list of brokers.

   :>json array brokers: List of broker IDs

   **Example request**:

   .. sourcecode:: http

      GET /brokers HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.kafka.v1+json

      {
        "brokers": [1, 2, 3]
      }


