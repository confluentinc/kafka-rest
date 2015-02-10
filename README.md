Kafka REST Proxy
================

The Kafka REST Proxy provides a RESTful interface to a Kafka cluster. It makes it easy to produce and consume messages,
view the state of the cluster, and perform administrative actions without using the native Kafka protocol or clients.
Examples of use cases include reporting data to Kafka from any frontend app built in any language, ingesting messages
into a stream processing framework that doesn't yet support Kafka, and scripting administrative actions.

Installation
------------

You can download prebuilt versions of the Kafka REST Proxy as part of the
[Confluent Platform](http://confluent.io/downloads/). To install from source,
follow the instructions in the Development section.

Deployment
----------

The REST proxy includes a built-in Jetty server. Assuming you've configured your classpath correctly, you can start a
server with

    java io.confluent.kafkarest.Main [server.properties]

where `server.properties` contains configuration settings as specified by the `KafkaRestConfiguration` class. Although the
properties file is not required, the default configuration is not intended for production. Production deployments *should*
specify a properties file. By default the server starts bound to port 8082, does not specify a unique instance ID (required
to safely run multiple proxies concurrently), and expects Zookeeper to be available at `localhost:2181` and a Kafka broker
at `localhost:9092`.

Development
-----------

To build a development version, you may need a development version of
[io.confluent.common](https://github.com/confluentinc/common) and [io.confluent.rest-utils](https://github.com/confluentinc/rest-utils).
After installing `common` and `rest-utils` and compiling `kafka-rest` with
Maven, you can run an instance of the proxy against a local Kafka cluster (using
the default configuration included with Kafka):

    mvn exec:java

Contribute
----------

- Source Code: https://github.com/confluentinc/kafka-rest
- Issue Tracker: https://github.com/confluentinc/kafka-rest/issues

License
-------

The project is licensed under the Apache 2 license.
