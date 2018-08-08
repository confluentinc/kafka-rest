/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import kafka.utils.TestUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;

public class SimpleConsumerAvroTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";

  // Primitive types
  private final List<ProducerRecord<Object, Object>> recordsOnlyValues = Arrays.asList(
      new ProducerRecord<Object, Object>(topicName, 1),
      new ProducerRecord<Object, Object>(topicName, 2),
      new ProducerRecord<Object, Object>(topicName, 3),
      new ProducerRecord<Object, Object>(topicName, 4)
  );

  // And primitive keys w/ record values
  private static final String valueSchemaStr = "{\"type\": \"record\", "
      + "\"name\":\"test\","
      + "\"fields\":[{"
      + "  \"name\":\"field\", "
      + "  \"type\": \"int\""
      + "}]}";
  private static final Schema valueSchema = new Schema.Parser().parse(valueSchemaStr);
  private final List<ProducerRecord<Object, Object>> recordsWithKeys = Arrays.asList(
      new ProducerRecord<Object, Object>(
          topicName, "key",
          new GenericRecordBuilder(valueSchema).set("field", 72).build()),
      new ProducerRecord<Object, Object>(
          topicName, "key",
          new GenericRecordBuilder(valueSchema).set("field", 73).build()),
      new ProducerRecord<Object, Object>(
          topicName, "key",
          new GenericRecordBuilder(valueSchema).set("field", 74).build()),
      new ProducerRecord<Object, Object>(
          topicName, "key",
          new GenericRecordBuilder(valueSchema).set("field", 75).build())
  );

  private static final GenericType<List<AvroConsumerRecord>> avroConsumerRecordType
      = new GenericType<List<AvroConsumerRecord>>() {
  };
  private static final Converter converter = new Converter() {
    @Override
    public Object convert(Object obj) {
      return AvroConverter.toJson(obj).json;
    }
  };

  public SimpleConsumerAvroTest() {
    super(1, true);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 1;
    final int replicationFactor = 1;
    TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor,
                          JavaConversions.asScalaBuffer(this.servers), new Properties());
  }

  @Test
  public void testConsumeOnlyValuesByOffset() {
    produceAvroMessages(recordsOnlyValues);

    simpleConsumeMessages(
        topicName,
        0,
        null, // No "count" parameter in the query
        recordsOnlyValues.subList(0, 1), // We expect only the first record in the response
        Versions.KAFKA_V1_JSON_AVRO,
        Versions.KAFKA_V1_JSON_AVRO,
        avroConsumerRecordType,
        converter
    );
  }

  @Test
  public void testConsumeWithKeysByOffset() {
    produceAvroMessages(recordsWithKeys);

    simpleConsumeMessages(
        topicName,
        0,
        null, // No "count" parameter in the query
        recordsWithKeys.subList(0, 1),
        Versions.KAFKA_V1_JSON_AVRO,
        Versions.KAFKA_V1_JSON_AVRO,
        avroConsumerRecordType,
        converter
    );
  }

  @Test
  public void testConsumeOnlyValuesByOffsetAndCount() {
    produceAvroMessages(recordsOnlyValues);

    simpleConsumeMessages(
        topicName,
        0,
        recordsOnlyValues.size(),
        recordsOnlyValues,
        Versions.KAFKA_V1_JSON_AVRO,
        Versions.KAFKA_V1_JSON_AVRO,
        avroConsumerRecordType,
        converter
    );
  }

  @Test
  public void testConsumeWithKeysByOffsetAndCount() {
    produceAvroMessages(recordsWithKeys);

    simpleConsumeMessages(
        topicName,
        0,
        recordsWithKeys.size(),
        recordsWithKeys,
        Versions.KAFKA_V1_JSON_AVRO,
        Versions.KAFKA_V1_JSON_AVRO,
        avroConsumerRecordType,
        converter
    );
  }

  @Test
  public void testConsumeInvalidTopic() {

    Response response = request(
        "/topics/nonexistenttopic/partitions/0/messages",
        Collections.singletonMap("offset", "0")
    ).accept(Versions.KAFKA_V1_JSON_AVRO).get();

    assertErrorResponse(Response.Status.NOT_FOUND, response,
        Errors.TOPIC_NOT_FOUND_ERROR_CODE,
        Errors.TOPIC_NOT_FOUND_MESSAGE,
        Versions.KAFKA_V1_JSON_AVRO);
  }

  @Test
  public void testConsumeInvalidPartition() {

    Response response = request(
        "/topics/topic1/partitions/1/messages",
        Collections.singletonMap("offset", "0")
    ).accept(Versions.KAFKA_V1_JSON_AVRO).get();

    assertErrorResponse(Response.Status.NOT_FOUND, response,
        Errors.PARTITION_NOT_FOUND_ERROR_CODE,
        Errors.PARTITION_NOT_FOUND_MESSAGE,
        Versions.KAFKA_V1_JSON_AVRO);
  }
}
