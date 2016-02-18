.. _kafkarest_changelog:

Changelog
=========

Version 2.0.1
-------------

* `PR-144 <https://github.com/confluentinc/kafka-rest/pull/144>`_ - Fix for Race condition when consuming while topic is being created (Issue #105)
* `PR-158 <https://github.com/confluentinc/kafka-rest/pull/158>`_ - Update Kafka version to 0.9.0.1-cp1

Version 2.0.0
-------------

* `PR-64 <https://github.com/confluentinc/kafka-rest/pull/64>`_ - Reduce integration test time.
* `PR-66 <https://github.com/confluentinc/kafka-rest/pull/66>`_ - Add support for SimpleConsumer-like access (Issue #26)
* `PR-67 <https://github.com/confluentinc/kafka-rest/pull/67>`_ - Handle conflicting IDs and separate IDs used in the REST proxy and by Kafka's consumer implementation.
* `PR-78 <https://github.com/confluentinc/kafka-rest/pull/78>`_ - Remove kafka from list of production directories to include in CLASSPATH.
* `PR-89 <https://github.com/confluentinc/kafka-rest/pull/89>`_ - JSON message support
* `PR-96 <https://github.com/confluentinc/kafka-rest/pull/96>`_ - Fixed log4j and daemon flag bugs in kafka-rest-run-class based on fix from schema-registry.
* `PR-99 <https://github.com/confluentinc/kafka-rest/pull/99>`_ - Require Java 7
* `PR-101 <https://github.com/confluentinc/kafka-rest/pull/101>`_ - rest-utils updates
* `PR-103 <https://github.com/confluentinc/kafka-rest/pull/103>`_ - Issue 94 rename main
* `PR-108 <https://github.com/confluentinc/kafka-rest/pull/108>`_ - Clarify partitioning behavior for produce requests
* `PR-117 <https://github.com/confluentinc/kafka-rest/pull/117>`_ - Update to Kafka 0.9.0.0-SNAPSHOT and make adjustments to work with updated ZkUtils.
* `PR-122 <https://github.com/confluentinc/kafka-rest/pull/122>`_ - Use x.y.z versioning scheme (i.e. 2.0.0-SNAPSHOT)
* `PR-123 <https://github.com/confluentinc/kafka-rest/pull/123>`_ - Updated args for JaasUtils.isZkSecurityEnabled()
* `PR-125 <https://github.com/confluentinc/kafka-rest/pull/125>`_ - Use Kafka compiled with Scala 2.11
