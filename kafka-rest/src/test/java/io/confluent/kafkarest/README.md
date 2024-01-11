Kafka REST Proxy Tests
================

Unit Tests
----------

The unit tests can be run using `mvn test`

To run an individual unit test class use, for example, `mvn -Dtest=ProduceRequestTest test`

To run an individual unit test within a test class use, for example, `mvn -Dtest=ProduceRequestTest#testProduceRequestDeserializerAddsSizeFromString test`

Integration Tests
-----------------

The integration tests live in the integration folder.

They can be run using `mvn failsafe:integration-test`

Individual test classes can be run using, for example, `mvn -Dit.test=ProduceActionIntegrationTest failsafe:integration-test`

And individual tests within a test class can be run using, for example, ` mvn -Dit.test=ProduceActionIntegrationTest#produceBinary failsafe:integration-test`

Notes
-----

To run individual tests you must be within the kafka-rest project folder (eg `~/git/kafka-rest/kafka-rest`) otherwise no tests will be found.

If you make changes to the code, you will need to rebuild before running the tests for any code changes to be picked up. For example rebuild with `mvn clean package -DskipTests=true`

To run the same test multiple times, there are several options, which include:

- Replace `@Test` with `@RepeatedTest(5)` (remembering to rebuild)
- Put a simple while loop around the mvn command `while true; do mvn -Dit.test=ProduceActionIntegrationTest failsafe:integration-test; done`