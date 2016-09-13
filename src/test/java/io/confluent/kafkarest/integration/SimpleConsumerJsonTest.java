/**
 * Copyright 2015 Confluent Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.JsonConsumerRecord;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;

public class SimpleConsumerJsonTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";

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
    final int numPartitions = 1;
    final int replicationFactor = 1;
    TestUtils.createTopic(zkUtils, topicName, numPartitions, replicationFactor,
        JavaConversions.asScalaBuffer(this.servers), new Properties());
  }

  @Test
  public void testConsumeOnlyValuesByOffset() {
    produceJsonMessages(recordsOnlyValues);

    simpleConsumeMessages(
        topicName,
        0,
        null, // No "count" parameter in the query
        recordsOnlyValues.subList(0, 1), // We expect only the first record in the response
        Versions.KAFKA_V1_JSON_JSON,
        Versions.KAFKA_V1_JSON_JSON,
        jsonConsumerRecordType,
        null
    );
  }

  @Test
  public void testConsumeOnlyValuesByOffsetAndCount() {
    produceJsonMessages(recordsOnlyValues);

    simpleConsumeMessages(
        topicName,
        0,
        recordsOnlyValues.size(),
        recordsOnlyValues,
        Versions.KAFKA_V1_JSON_JSON,
        Versions.KAFKA_V1_JSON_JSON,
        jsonConsumerRecordType,
        null
    );
  }

  @Test
  public void testConsumeWithKeysByOffsetAndCount() {
    produceJsonMessages(recordsWithKeys);

    simpleConsumeMessages(
        topicName,
        0,
        recordsWithKeys.size(),
        recordsWithKeys,
        Versions.KAFKA_V1_JSON_JSON,
        Versions.KAFKA_V1_JSON_JSON,
        jsonConsumerRecordType,
        null
    );
  }

  @Test(timeout = 4000)
  public void testConsumeMoreMessagesThanAvailable() {
    produceJsonMessages(recordsOnlyValues);

    simpleConsumeMessages(
        topicName,
        0,
        recordsOnlyValues.size()+1, // Ask for more than there is
        recordsOnlyValues,
        Versions.KAFKA_V1_JSON_JSON,
        Versions.KAFKA_V1_JSON_JSON,
        jsonConsumerRecordType,
        null
    );
  }

  @Test
  public void testConsumeInvalidTopic() {

    Response response = request("/topics/nonexistenttopic/partitions/0/messages",
        ImmutableMap.of("offset", "0")).accept(Versions.KAFKA_V1_JSON_JSON).get();

    assertErrorResponse(Response.Status.NOT_FOUND, response,
        Errors.TOPIC_NOT_FOUND_ERROR_CODE,
        Errors.TOPIC_NOT_FOUND_MESSAGE,
        Versions.KAFKA_V1_JSON_JSON);
  }

  @Test
  public void testConsumeInvalidPartition() {

    Response response = request("/topics/topic1/partitions/1/messages",
        ImmutableMap.of("offset", "0")).accept(Versions.KAFKA_V1_JSON_JSON).get();

    assertErrorResponse(Response.Status.NOT_FOUND, response,
        Errors.PARTITION_NOT_FOUND_ERROR_CODE,
        Errors.PARTITION_NOT_FOUND_MESSAGE,
        Versions.KAFKA_V1_JSON_JSON);
  }

}
