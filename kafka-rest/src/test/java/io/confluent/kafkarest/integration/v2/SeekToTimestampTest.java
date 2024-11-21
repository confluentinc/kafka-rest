package io.confluent.kafkarest.integration.v2;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryConsumerRecord;
import io.confluent.kafkarest.integration.AbstractConsumerTest;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.core.GenericType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SeekToTimestampTest extends AbstractConsumerTest {

  private static final String TOPIC_NAME = "topic-1";
  private static final String CONSUMER_GROUP_ID = "consumer-group-1";

  private final List<ProducerRecord<byte[], byte[]>> RECORDS_BEFORE_TIMESTAMP =
      Arrays.asList(
          new ProducerRecord<>(TOPIC_NAME, "value-1".getBytes()),
          new ProducerRecord<>(TOPIC_NAME, "value-2".getBytes()),
          new ProducerRecord<>(TOPIC_NAME, "value-3".getBytes()));

  private final List<ProducerRecord<byte[], byte[]>> RECORDS_AFTER_TIMESTAMP =
      Arrays.asList(
          new ProducerRecord<>(TOPIC_NAME, "value-4".getBytes()),
          new ProducerRecord<>(TOPIC_NAME, "value-5".getBytes()),
          new ProducerRecord<>(TOPIC_NAME, "value-6".getBytes()));

  private static final GenericType<List<BinaryConsumerRecord>> BINARY_CONSUMER_RECORD_TYPE =
      new GenericType<List<BinaryConsumerRecord>>() {};

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTopic(TOPIC_NAME, /* numPartitions= */ 1, /* replicationFactor= */ (short) 1);
  }

  @Test
  public void testConsumeOnlyValues() throws Exception {
    String consumerUri =
        startConsumeMessages(
            CONSUMER_GROUP_ID, TOPIC_NAME, /* format= */ null, Versions.KAFKA_V2_JSON_BINARY);

    produceBinaryMessages(RECORDS_BEFORE_TIMESTAMP);

    Instant timestamp = Instant.now();
    Thread.sleep(1000);

    produceBinaryMessages(RECORDS_AFTER_TIMESTAMP);

    seekToTimestamp(consumerUri, TOPIC_NAME, /* partitionId= */ 0, timestamp);

    consumeMessages(
        consumerUri,
        RECORDS_AFTER_TIMESTAMP,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        BINARY_CONSUMER_RECORD_TYPE,
        /* converter= */ null,
        BinaryConsumerRecord::toConsumerRecord);

    commitOffsets(consumerUri);
  }
}
