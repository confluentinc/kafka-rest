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
package io.confluent.kafkarest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.generic.IndexedRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.entities.EntityUtils;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.rest.entities.ErrorMessage;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUtils {

  private static final Logger log = LoggerFactory.getLogger(TestUtils.class);
  private static final ObjectMapper jsonParser = new ObjectMapper();

  // Media type collections that should be tested together (i.e. expect the same raw output). The
  // expected output format is included so these lists can include weighted Accept headers.
  public static final RequestMediaType[] V1_ACCEPT_MEDIATYPES = {
      // Single type in Accept header
      new RequestMediaType(Versions.KAFKA_V1_JSON, Versions.KAFKA_V1_JSON),
      new RequestMediaType(Versions.KAFKA_DEFAULT_JSON, Versions.KAFKA_DEFAULT_JSON),
      new RequestMediaType(Versions.JSON, Versions.JSON),
      // Weighted options in Accept header should select the highest weighted option
      new RequestMediaType(
          Versions.KAFKA_V1_JSON_WEIGHTED + ", " + Versions.KAFKA_DEFAULT_JSON_WEIGHTED + ", "
          + Versions.JSON, Versions.KAFKA_V1_JSON),
      new RequestMediaType(
          Versions.KAFKA_V1_JSON + "; q=0.8, " + Versions.KAFKA_DEFAULT_JSON + "; q=0.9, "
          + Versions.JSON + "; q=0.7", Versions.KAFKA_DEFAULT_JSON),
      new RequestMediaType(
          Versions.KAFKA_V1_JSON + "; q=0.8, " + Versions.KAFKA_DEFAULT_JSON + "; q=0.7, "
          + Versions.JSON + "; q=0.9", Versions.JSON),
      // No accept header, should use most specific default media type. Note that in cases with
      // embedded data this won't be the most specific value since the version with the embedded
      // type will be used instead
      new RequestMediaType(null, Versions.KAFKA_MOST_SPECIFIC_DEFAULT)
  };
  public static final List<RequestMediaType> V1_ACCEPT_MEDIATYPES_BINARY;

  static {
    V1_ACCEPT_MEDIATYPES_BINARY =
        new ArrayList<RequestMediaType>(Arrays.asList(V1_ACCEPT_MEDIATYPES));
    V1_ACCEPT_MEDIATYPES_BINARY.add(
        new RequestMediaType(Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY));
  }

  public static final RequestMediaType[] V1_ACCEPT_MEDIATYPES_AVRO = {
      new RequestMediaType(Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V1_JSON_AVRO)
  };

  // Response content types we should never allow to be produced
  public static final String[] V1_INVALID_MEDIATYPES = {
      "text/plain",
      "application/octet-stream"
  };

  public static final String[] V1_REQUEST_ENTITY_TYPES = {
      Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST
  };
  public static final List<String> V1_REQUEST_ENTITY_TYPES_BINARY;

  static {
    V1_REQUEST_ENTITY_TYPES_BINARY = new ArrayList<String>(Arrays.asList(V1_REQUEST_ENTITY_TYPES));
    V1_REQUEST_ENTITY_TYPES_BINARY.add(Versions.KAFKA_V1_JSON_BINARY);
  }

  public static final List<String> V1_REQUEST_ENTITY_TYPES_AVRO = Arrays.asList(
      Versions.KAFKA_V1_JSON_AVRO
  );

  // Request content types we'll always ignore
  public static final String[] V1_INVALID_REQUEST_MEDIATYPES = {
      "text/plain"
  };


  /**
   * Try to read the entity. If parsing fails, errors are rethrown, but the raw entity is also logged for debugging.
   */
  public static <T> T tryReadEntityOrLog(Response rawResponse, Class<T> entityType) {
    rawResponse.bufferEntity();
    try {
      return rawResponse.readEntity(entityType);
    } catch (Throwable t) {
      log.error("Failed to parse entity {}: ", rawResponse.readEntity(String.class), t);
      throw t;
    }
  }

  public static <T> T tryReadEntityOrLog(Response rawResponse, GenericType<T> entityType) {
    rawResponse.bufferEntity();
    try {
      return rawResponse.readEntity(entityType);
    } catch (Throwable t) {
      log.error("Failed to parse entity {}: ", rawResponse.readEntity(String.class), t);
      throw t;
    }
  }


  /**
   * Asserts that the response received an HTTP 200 status code, as well as some optional
   * requirements such as the Content-Type.
   */
  public static void assertOKResponse(Response rawResponse, String mediatype) {
    assertEquals(Response.Status.OK.getStatusCode(), rawResponse.getStatus());
    assertEquals(mediatype, rawResponse.getMediaType().toString());
  }

  /**
   * Asserts that the correct HTTP status code was set for the error and that a generic structured
   * response is returned. This requires a custom message to check against since they should always
   * be provided or explicitly specify the default.
   */
  public static void assertErrorResponse(Response.StatusType status, Response rawResponse,
                                         int code, String msg, String mediatype) {
    assertEquals(status.getStatusCode(), rawResponse.getStatus());

    // Successful deletion's return no content, so we shouldn't try to decode their entities
    if (status.equals(Response.Status.NO_CONTENT)) {
      return;
    }

    assertEquals(mediatype, rawResponse.getMediaType().toString());

    ErrorMessage response = tryReadEntityOrLog(rawResponse, ErrorMessage.class);
    assertEquals(code, response.getErrorCode());
    // This only checks that the start of the message is identical because debug mode will
    // include extra information and is enabled by default.
    if (msg != null) {
      assertTrue(response.getMessage().startsWith(msg));
    }
  }

  /**
   * Short-hand version of assertErrorResponse for the rare case that a default response is used,
   * e.g. for internal server errors.
   */
  public static void assertErrorResponse(Response.StatusType status, Response rawResponse,
                                         String mediatype) {
    assertErrorResponse(status, rawResponse, status.getStatusCode(), status.getReasonPhrase(),
                        mediatype);
  }

  public static class RequestMediaType {

    public final String header;
    public final String expected;

    public RequestMediaType(String header, String expected) {
      this.header = header;
      this.expected = expected;
    }
  }


  /**
   * Parses the given JSON string into Jackson's generic JsonNode structure. Useful for generation
   * test data that's easier to express as a JSON-encoded string.
   */
  public static JsonNode jsonTree(String jsonData) {
    try {
      return jsonParser.readTree(jsonData);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }

  public static void assertPartitionsEqual(List<PartitionOffset> a, List<PartitionOffset> b) {
    assertEquals(a.size(), b.size());
    for (int i = 0; i < a.size(); i++) {
      PartitionOffset aOffset = a.get(i), bOffset = b.get(i);
      assertEquals(aOffset.getPartition(), bOffset.getPartition());
    }
  }

  public static void assertPartitionOffsetsEqual(List<PartitionOffset> a, List<PartitionOffset> b) {
    // We can't be sure these will be exactly equal since they may be random. Instead verify that
    // exception vs. non-exception responses match up
    assertEquals(a.size(), b.size());
    for (int i = 0; i < a.size(); i++) {
      PartitionOffset aOffset = a.get(i), bOffset = b.get(i);
      assertEquals(aOffset.getError() != null, bOffset.getError() != null);
      assertEquals(aOffset.getOffset() != null, bOffset.getOffset() != null);
    }
  }

  /**
   * Given one of the standard data types used for message payloads (byte[], JsonNode), get an
   * object that can be used to compare it to other instances, e.g. convert byte[] to an encoded
   * String. For some types this may be a noop.
   */
  public static <V> Object encodeComparable(V k) {
    if (k == null) {
      return null;
    } else if (k instanceof byte[]) {
      return EntityUtils.encodeBase64Binary((byte[]) k);
    } else if (k instanceof JsonNode) {
      return k;
    } else if (k instanceof IndexedRecord) {
      return k;
    } else if (k instanceof Number || k instanceof Boolean || k instanceof Character
               || k instanceof String) {
      // Primitive types + strings are all safe for comparison
      return k;
    } else if (k instanceof Collection || k instanceof Map) {
      return k;
    } else {
      throw new RuntimeException(k.getClass().getName() + " is not handled by encodeComparable.");
    }
  }

  /**
   * Consumes messages from Kafka to verify they match the inputs. Optionally add a partition to
   * only examine that partition.
   */
  public static <K, V> void assertTopicContains(String bootstrapServers, String topicName,
                                                List<? extends ProduceRecord<K, V>> records,
                                                Integer partition,
                                                String keyDeserializer, String valueDeserializer,
                                                Properties deserializerProps,
                                                boolean validateContents) {

    KafkaConsumer<K, V> consumer = createConsumer(bootstrapServers, "testgroup", "consumer0",
        20000L, keyDeserializer, valueDeserializer, deserializerProps);

    Map<Object, Integer> msgCounts = TestUtils.topicCounts(consumer, topicName, records, partition);

    Map<Object, Integer> refMsgCounts = new HashMap<>();
    for (ProduceRecord rec : records) {
      Object msg = TestUtils.encodeComparable(rec.getValue());
      refMsgCounts.put(msg, (refMsgCounts.get(msg) == null ? 0 : refMsgCounts.get(msg)) + 1);
    }

    // We can't always easily get the data on both ends to be easily comparable, e.g. when the
    // input data is JSON but it's stored in Avro, so in some cases we use an alternative that
    // just checks the # of each count matches up, e.g. if we have (a => 3, b => 4) input and (c
    // => 4, d => 3), it would pass since both have (3 => 1, 4 => 1) counts, even though their
    // encoded values differ. This, of course, assumes we don't get collisions.
    if (validateContents) {
      assertEquals(msgCounts, refMsgCounts);
    } else {
      Map<Integer, Integer> refCountCounts = new HashMap<Integer, Integer>();
      for (Map.Entry<Object, Integer> entry : refMsgCounts.entrySet()) {
        Integer count = refCountCounts.get(entry.getValue());
        refCountCounts.put(entry.getValue(), (count == null ? 0 : count) + 1);
      }
      Map<Integer, Integer> msgCountCounts = new HashMap<Integer, Integer>();
      for (Map.Entry<Object, Integer> entry : msgCounts.entrySet()) {
        Integer count = msgCountCounts.get(entry.getValue());
        msgCountCounts.put(entry.getValue(), (count == null ? 0 : count) + 1);
      }
      assertEquals(refCountCounts, msgCountCounts);
    }
  }

  public static <K, V> void assertTopicContains(String bootstrapServers, String topicName,
                                                List<? extends ProduceRecord<K, V>> records,
                                                Integer partition,
                                                String keyDeserializer, String valueDeserializer,
                                                boolean validateContents) {
    assertTopicContains(bootstrapServers, topicName, records, partition, keyDeserializer,
        valueDeserializer, new Properties(), validateContents);
  }

  private static <V, K> Map<Object, Integer> topicCounts(final KafkaConsumer<K, V> consumer ,
                                                         final String topicName,
                                                         final List<? extends ProduceRecord<K,V>> records,
                                                         final Integer partition) {
    Map<Object, Integer> msgCounts = new HashMap<Object, Integer>();
    consumer.subscribe(Collections.singleton(topicName));

    try {
      AtomicInteger counter = new AtomicInteger(0);
      org.apache.kafka.test.TestUtils.waitForCondition(() -> {
        ConsumerRecords<K, V> consumerRecords =  consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<K, V> record: consumerRecords) {
          if (partition == null || record.partition() == partition) {
            Object msg = encodeComparable(record.value());
            msgCounts.put(msg, (msgCounts.get(msg) == null ? 0 : msgCounts.get(msg)) + 1);
            if (counter.incrementAndGet() == records.size())
              return true;
          }
        }

        return false;
      }, "Failed to consume messages");
    } catch (InterruptedException e) {
      throw new RuntimeException("InterruptedException occurred", e);
    }

    consumer.close();
    return  msgCounts;
  }


  public static <K, V> KafkaConsumer<K, V> createConsumer(String bootstrapServers, String groupId,
                                                          String consumerId, Long consumerTimeout,
                                                          String keyDeserializer,
                                                          String valueDeserializer,
                                                          Properties deserializerProps) {
    Properties consumerConfig = new Properties();
    consumerConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
    consumerConfig.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, consumerTimeout.toString());
    consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    consumerConfig.putAll(deserializerProps);
    return new KafkaConsumer<>(consumerConfig);
  }
}
