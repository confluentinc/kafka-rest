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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.core.GenericType;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import kafka.utils.TestUtils;
import scala.collection.JavaConversions;

public class ConsumerAvroTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";
  private static final List<Partition> partitions = Arrays.asList(
      new Partition(0, 0, Arrays.asList(
          new PartitionReplica(0, true, true),
          new PartitionReplica(1, false, false)
      ))
  );
  private static final Topic topic = new Topic(topicName, new Properties(), partitions);
  private static final String groupName = "testconsumergroup";

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

  public ConsumerAvroTest() {
    super(1, true);
  }
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor,
                          JavaConversions.asScalaBuffer(this.servers), new Properties());
  }

  @Test
  public void testConsumeOnlyValues() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.AVRO,
                                              Versions.KAFKA_V1_JSON_AVRO);
    produceAvroMessages(recordsOnlyValues);
    consumeMessages(instanceUri, topicName, recordsOnlyValues,
                    Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V1_JSON_AVRO,
                    avroConsumerRecordType, converter);
    commitOffsets(instanceUri);
  }

  @Test
  public void testConsumeWithKeys() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.AVRO,
                                              Versions.KAFKA_V1_JSON_AVRO);
    produceAvroMessages(recordsWithKeys);
    consumeMessages(instanceUri, topicName, recordsWithKeys,
                    Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V1_JSON_AVRO,
                    avroConsumerRecordType, converter);
    commitOffsets(instanceUri);
  }

  @Test
  public void testConsumeInvalidTopic() {
    startConsumeMessages(groupName, "nonexistenttopic", EmbeddedFormat.AVRO,
                         Versions.KAFKA_V1_JSON_AVRO, true);
  }

  @Test
  public void testConsumeTimeout() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.AVRO,
                                              Versions.KAFKA_V1_JSON_AVRO);
    produceAvroMessages(recordsWithKeys);
    consumeMessages(instanceUri, topicName, recordsWithKeys,
                    Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V1_JSON_AVRO,
                    avroConsumerRecordType, converter);
    consumeForTimeout(instanceUri, topicName,
                      Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V1_JSON_AVRO,
                      avroConsumerRecordType);
  }

  @Test
  public void testDeleteConsumer() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.AVRO,
                                              Versions.KAFKA_V1_JSON_AVRO);
    produceAvroMessages(recordsWithKeys);
    consumeMessages(instanceUri, topicName, recordsWithKeys,
                    Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V1_JSON_AVRO,
                    avroConsumerRecordType, converter);
    deleteConsumer(instanceUri);
  }
}
