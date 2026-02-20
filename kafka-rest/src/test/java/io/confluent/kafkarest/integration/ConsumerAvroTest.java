/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.integration;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.SchemaConsumerRecord;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.core.GenericType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ConsumerAvroTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";
  private static final String groupName = "testconsumergroup";

  // Primitive types
  private final List<ProducerRecord<Object, Object>> recordsOnlyValues =
      Arrays.asList(
          new ProducerRecord<>(topicName, 1),
          new ProducerRecord<>(topicName, 2),
          new ProducerRecord<>(topicName, 3),
          new ProducerRecord<>(topicName, 4));

  // And primitive keys w/ record values
  private static final String valueSchemaStr =
      "{\"type\": \"record\", "
          + "\"name\":\"test\","
          + "\"fields\":[{"
          + "  \"name\":\"field\", "
          + "  \"type\": \"int\""
          + "}]}";
  private static final Schema valueSchema = new Schema.Parser().parse(valueSchemaStr);
  private final List<ProducerRecord<Object, Object>> recordsWithKeys =
      Arrays.asList(
          new ProducerRecord<Object, Object>(
              topicName, "key", new GenericRecordBuilder(valueSchema).set("field", 72).build()),
          new ProducerRecord<Object, Object>(
              topicName, "key", new GenericRecordBuilder(valueSchema).set("field", 73).build()),
          new ProducerRecord<Object, Object>(
              topicName, "key", new GenericRecordBuilder(valueSchema).set("field", 74).build()),
          new ProducerRecord<Object, Object>(
              topicName, "key", new GenericRecordBuilder(valueSchema).set("field", 75).build()));

  private static final GenericType<List<SchemaConsumerRecord>> avroConsumerRecordType =
      new GenericType<List<SchemaConsumerRecord>>() {};
  private static final Converter converter = obj -> new AvroConverter().toJson(obj).getJson();

  public ConsumerAvroTest() {
    super(1, true);
  }

  @BeforeEach
  @Override
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp(testInfo);
    final int numPartitions = 3;
    final int replicationFactor = 1;
    createTopic(topicName, numPartitions, (short) replicationFactor);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testConsumeOnlyValues(String quorum) {
    String instanceUri =
        startConsumeMessages(
            groupName, topicName, EmbeddedFormat.AVRO, Versions.KAFKA_V2_JSON_AVRO, "earliest");
    produceAvroMessages(recordsOnlyValues);
    consumeMessages(
        instanceUri,
        recordsOnlyValues,
        Versions.KAFKA_V2_JSON_AVRO,
        Versions.KAFKA_V2_JSON_AVRO,
        avroConsumerRecordType,
        converter,
        SchemaConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testConsumeWithKeys(String quorum) {
    String instanceUri =
        startConsumeMessages(
            groupName, topicName, EmbeddedFormat.AVRO, Versions.KAFKA_V2_JSON_AVRO, "earliest");
    produceAvroMessages(recordsWithKeys);
    consumeMessages(
        instanceUri,
        recordsWithKeys,
        Versions.KAFKA_V2_JSON_AVRO,
        Versions.KAFKA_V2_JSON_AVRO,
        avroConsumerRecordType,
        converter,
        SchemaConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testConsumeTimeout(String quorum) {
    String instanceUri =
        startConsumeMessages(
            groupName, topicName, EmbeddedFormat.AVRO, Versions.KAFKA_V2_JSON_AVRO, "earliest");
    produceAvroMessages(recordsWithKeys);
    consumeMessages(
        instanceUri,
        recordsWithKeys,
        Versions.KAFKA_V2_JSON_AVRO,
        Versions.KAFKA_V2_JSON_AVRO,
        avroConsumerRecordType,
        converter,
        SchemaConsumerRecord::toConsumerRecord);
    consumeForTimeout(
        instanceUri,
        Versions.KAFKA_V2_JSON_AVRO,
        Versions.KAFKA_V2_JSON_AVRO,
        avroConsumerRecordType);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testDeleteConsumer(String quorum) {
    String instanceUri =
        startConsumeMessages(
            groupName, topicName, EmbeddedFormat.AVRO, Versions.KAFKA_V2_JSON_AVRO, "earliest");
    produceAvroMessages(recordsWithKeys);
    consumeMessages(
        instanceUri,
        recordsWithKeys,
        Versions.KAFKA_V2_JSON_AVRO,
        Versions.KAFKA_V2_JSON_AVRO,
        avroConsumerRecordType,
        converter,
        SchemaConsumerRecord::toConsumerRecord);
    deleteConsumer(instanceUri);
  }
}
