.. _kafkarest_changelog:

Changelog
=========

Version 5.0.0
-------------

* `PR-439 <https://github.com/confluentinc/kafka-rest/pull/439>`_ - Temporarily pin Kafka and Confluent versions
* `PR-438 <https://github.com/confluentinc/kafka-rest/pull/438>`_ - Don't use deprecated RecordMetadata constructor
* `PR-434 <https://github.com/confluentinc/kafka-rest/pull/434>`_ - Fix usage of internal Jersey utilities that no longer exist
* `PR-428 <https://github.com/confluentinc/kafka-rest/pull/428>`_ - Use ExplicitGCInvokesConcurrent instead of disabling explicit GC

Version 3.3.0
-------------

* `PR-305 <https://github.com/confluentinc/kafka-rest/pull/305>`_ - Use max.block.ms instead of metadata.fetch.timeout.ms since the latter was deprecated and now removed in KAFKA-3763
* Ability to add resources like filter through a custom extension and provide a custom context
* `PR-296 <https://github.com/confluentinc/kafka-rest/pull/296>`_ - Interceptor configuration options

Version 3.2.2
-------------

* `PR-303 <https://github.com/confluentinc/kafka-rest/pull/303>`_ - CLIENTS-304: Fix AvroConsumerRecord constructors and handling of topic for v1 consumer API.

Version 3.2.1
-------------

* `PR-285 <https://github.com/confluentinc/kafka-rest/pull/285>`_ - CLIENTS-281: Fix KafkaConsumerReadTask's locking, handling of multiple polls, invalid handling of old ConsumerTimeout exceptions, and incorrect generic type parameters.
* `PR-277 <https://github.com/confluentinc/kafka-rest/pull/277>`_ - Documentation fixes

Version 3.2.0
-------------

* `PR-256 <https://github.com/confluentinc/kafka-rest/pull/256>`_ - Update ClusterTestHarness to use o.a.k.common.utils.Time.
* `PR-255 <https://github.com/confluentinc/kafka-rest/pull/255>`_ - clarified the key/value format limitations based on user questions
* `PR-264 <https://github.com/confluentinc/kafka-rest/pull/264>`_ - Fix build to work post KIP-103
* `PR-265 <https://github.com/confluentinc/kafka-rest/pull/265>`_ - Follow up for KIP-103 changes that fixes ProducerPool's extraction of endpoints to use the security protocol in the URL instead of the listener name.
* `PR-268 <https://github.com/confluentinc/kafka-rest/pull/268>`_ - modified readme to use v2. also bumped installation up for usability
* `PR-271 <https://github.com/confluentinc/kafka-rest/pull/271>`_ - Updated quickstart and configuration docs for V2 and for security
* `PR-258 <https://github.com/confluentinc/kafka-rest/pull/258>`_ - |crest| security
* `PR-274 <https://github.com/confluentinc/kafka-rest/pull/274>`_ - added field 'topic' to tools/ConsumerPerformance.java for system test
* `PR-272 <https://github.com/confluentinc/kafka-rest/pull/272>`_ - added description of JVM parameters for SASL configuration

Version 3.1.2
-------------
No changes

Version 3.1.1
-------------
No changes

Version 3.1.0
-------------

* `PR-239 <https://github.com/confluentinc/kafka-rest/pull/239>`_ - Require bash since we use some bashisms and fix a copyright.
* `PR-235 <https://github.com/confluentinc/kafka-rest/pull/235>`_ - fix typo command line options ``-help`` check
* `PR-222 <https://github.com/confluentinc/kafka-rest/pull/222>`_ - fixing issue #91
* `PR-230 <https://github.com/confluentinc/kafka-rest/pull/230>`_ - Issue #229: Add blurb on Jetty jmx metrics
* `PR-214 <https://github.com/confluentinc/kafka-rest/pull/214>`_ - Small readme.md fix
* `PR-127 <https://github.com/confluentinc/kafka-rest/pull/127>`_ - Fix to ConsumerManager tests AND then handling of max bytes
* `PR-203 <https://github.com/confluentinc/kafka-rest/pull/203>`_ - Fix a typo that cause non-corresponding logger name
* `PR-202 <https://github.com/confluentinc/kafka-rest/pull/202>`_ - fix the implementation of topicExists

Version 3.0.0
-------------

* `PR-164 <https://github.com/confluentinc/kafka-rest/pull/164>`_ - 2.x merge to master
* `PR-166 <https://github.com/confluentinc/kafka-rest/pull/166>`_ - Bump version to 3.0.0-SNAPSHOT and Kafka dependency to 0.10.0.0-SNAPSHOT
* `PR-171 <https://github.com/confluentinc/kafka-rest/pull/171>`_ - Fix build to handle rack aware changes in Kafka.
* `PR-174 <https://github.com/confluentinc/kafka-rest/pull/174>`_ - Update CoreUtils usage to match kafka updates
* `PR-189 <https://github.com/confluentinc/kafka-rest/pull/189>`_ - Minor fixes for compatibility with newest 0.10.0 branch.
* `PR-192 <https://github.com/confluentinc/kafka-rest/pull/192>`_ - Minor fixes for compatibility with newest 0.10.0 branch.
* `PR-202 <https://github.com/confluentinc/kafka-rest/pull/202>`_ - fix the implementation of topicExists
* `PR-205 <https://github.com/confluentinc/kafka-rest/pull/205>`_ - Rearrange quickstart and use topics from earlier steps in requests for metadata so the example output will exactly match real output when starting from an empty cluster.

Version 2.0.1
-------------

* `PR-144 <https://github.com/confluentinc/kafka-rest/pull/144>`_ - Fix for Race condition when consuming while topic is being created (Issue #105)
* `PR-158 <https://github.com/confluentinc/kafka-rest/pull/158>`_ - Update Kafka version to 0.9.0.1-cp1

Version 2.0.0
-------------

* `PR-64 <https://github.com/confluentinc/kafka-rest/pull/64>`_ - Reduce integration test time.
* `PR-66 <https://github.com/confluentinc/kafka-rest/pull/66>`_ - Add support for SimpleConsumer-like access (Issue #26)
* `PR-67 <https://github.com/confluentinc/kafka-rest/pull/67>`_ - Handle conflicting IDs and separate IDs used in the |crest| and by Kafka's consumer implementation.
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
