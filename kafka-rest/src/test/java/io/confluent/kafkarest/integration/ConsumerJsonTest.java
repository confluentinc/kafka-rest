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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.JsonConsumerRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.GenericType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

public class ConsumerJsonTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";
  private static final String groupName = "testconsumergroup";

  private Map<String, Object> exampleMapValue() {
    Map<String, Object> res = new HashMap<String, Object>();
    res.put("foo", "bar");
    res.put("bar", null);
    res.put("baz", 53.4);
    res.put("taz", 45);
    return res;
  }

  private List<Object> exampleListValue() {
    List<Object> res = new ArrayList<Object>();
    res.add("foo");
    res.add(null);
    res.add(53.4);
    res.add(45);
    return res;
  }

  private final List<ProducerRecord<Object, Object>> recordsWithKeys = Arrays.asList(
      new ProducerRecord<Object, Object>(topicName, "key", "value"),
      new ProducerRecord<Object, Object>(topicName, "key", null),
      new ProducerRecord<Object, Object>(topicName, "key", 43.2),
      new ProducerRecord<Object, Object>(topicName, "key", 999),
      new ProducerRecord<Object, Object>(topicName, "key", exampleMapValue()),
      new ProducerRecord<Object, Object>(topicName, "key", exampleListValue())
  );

  private final List<ProducerRecord<Object, Object>> recordsOnlyValues = Arrays.asList(
      new ProducerRecord<Object, Object>(topicName, "value"),
      new ProducerRecord<Object, Object>(topicName, null),
      new ProducerRecord<Object, Object>(topicName, 43.2),
      new ProducerRecord<Object, Object>(topicName, 999),
      new ProducerRecord<Object, Object>(topicName, exampleMapValue()),
      new ProducerRecord<Object, Object>(topicName, exampleListValue())
  );

  private static final GenericType<List<JsonConsumerRecord>> jsonConsumerRecordType
      = new GenericType<List<JsonConsumerRecord>>() {
  };

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    createTopic(topicName, numPartitions, (short) replicationFactor);
  }

  @Test
  public void testConsumeWithKeys() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.JSON,
        Versions.KAFKA_V2_JSON_JSON);
    produceJsonMessages(recordsWithKeys);
    consumeMessages(
        instanceUri,
        recordsWithKeys,
        Versions.KAFKA_V2_JSON_JSON,
        Versions.KAFKA_V2_JSON_JSON,
        jsonConsumerRecordType,
        /* converter= */ null,
        JsonConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
  }

  @Test
  public void testConsumeOnlyValues() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.JSON,
        Versions.KAFKA_V2_JSON_JSON);
    produceJsonMessages(recordsOnlyValues);
    consumeMessages(
        instanceUri,
        recordsOnlyValues,
        Versions.KAFKA_V2_JSON_JSON,
        Versions.KAFKA_V2_JSON_JSON,
        jsonConsumerRecordType,
        /* converter= */ null,
        JsonConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
  }

}
