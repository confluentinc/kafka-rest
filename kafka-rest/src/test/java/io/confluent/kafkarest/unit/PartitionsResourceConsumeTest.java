package io.confluent.kafkarest.unit;

import static io.confluent.kafkarest.entities.EntityUtils.encodeBase64Binary;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.ConsumerReadCallback;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ScalaConsumersContext;
import io.confluent.kafkarest.SimpleConsumerManager;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.extension.InstantConverterProvider;
import io.confluent.kafkarest.resources.PartitionsResource;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.core.Application;
import org.easymock.Capture;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;

public class PartitionsResourceConsumeTest extends JerseyTest {

  private static final String TOPIC = "topic";
  private static final int PARTITION = 1;
  private static final long OFFSET = 30L;
  private static final Instant TIMESTAMP = Instant.ofEpochMilli(1000L);
  private static final long COUNT = 10L;

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_INSTANT;

  private static final byte[] KEY = "key".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);

  private static final List<BinaryConsumerRecord> RECORDS =
      unmodifiableList(
          singletonList(new BinaryConsumerRecord(TOPIC, KEY, VALUE, PARTITION, OFFSET)));


  private KafkaConsumerManager kafkaConsumerManager;
  private SimpleConsumerManager simpleConsumerManager;

  @Override
  protected Application configure() {
    ResourceConfig application = new ResourceConfig();
    application.register(new PartitionsResource(createKafkaRestContext()));
    application.register(InstantConverterProvider.class);
    application.register(new JacksonMessageBodyProvider());
    return application;
  }

  private KafkaRestContext createKafkaRestContext() {
    kafkaConsumerManager = createMock(KafkaConsumerManager.class);
    simpleConsumerManager = createMock(SimpleConsumerManager.class);

    try {
      return new DefaultKafkaRestContext(
          new KafkaRestConfig(),
          /* producerPool= */ null,
          kafkaConsumerManager,
          /* adminClientWrapper= */ null,
          /* groupMetadataObserver= */ null,
          new ScalaConsumersContext(
              /* metadataObserver= */ null,
              /* consumerManager= */ null,
              simpleConsumerManager));
    } catch (RestConfigException e) {
      throw new RuntimeException(e);
    }
  }

  private <K, V> void expectConsume(
      String topicName,
      int partitionId,
      long offset,
      long count,
      EmbeddedFormat embeddedFormat,
      List<? extends ConsumerRecord<K, V>> records) {
    Capture<ConsumerReadCallback<K, V>> callback = Capture.newInstance();
    simpleConsumerManager.consume(
        eq(topicName),
        eq(partitionId),
        eq(offset),
        eq(count),
        eq(embeddedFormat),
        capture(callback));
    expectLastCall().andAnswer(
        () -> {
          callback.getValue().onCompletion(records, /* e= */ null);
          return null;
        });
  }

  @Before
  public void setUpMocks() {
    reset(kafkaConsumerManager, simpleConsumerManager);
  }

  @Test
  public void getMessages_withTimestamp_returnsMessagesAtTimestamp() {
    expect(kafkaConsumerManager.getOffsetForTime(TOPIC, PARTITION, TIMESTAMP))
        .andReturn(Optional.of(OFFSET));
    expectConsume(TOPIC, PARTITION, OFFSET, COUNT, EmbeddedFormat.BINARY, RECORDS);
    replay(kafkaConsumerManager, simpleConsumerManager);

    String response =
        target("/topics/{topic}/partitions/{partition}/messages")
            .resolveTemplate("topic", TOPIC)
            .resolveTemplate("partition", PARTITION)
            .queryParam("timestamp", DATE_TIME_FORMATTER.format(TIMESTAMP))
            .queryParam("count", COUNT)
            .request(Versions.KAFKA_V1_JSON_BINARY)
            .get(String.class);

    assertEquals("[{"
            + "\"topic\":\"" + TOPIC + "\","
            + "\"key\":\"" + encodeBase64Binary(KEY) + "\","
            + "\"value\":\"" + encodeBase64Binary(VALUE) + "\","
            + "\"partition\":" + PARTITION + ","
            + "\"offset\":" + OFFSET
            + "}]",
        response);

    verify(kafkaConsumerManager, simpleConsumerManager);
  }

  @Test
  public void getMessages_withTimestamp_noSuchOffset_returnsEmpty() {
    expect(kafkaConsumerManager.getOffsetForTime(TOPIC, PARTITION, TIMESTAMP))
        .andReturn(Optional.empty());
    replay(kafkaConsumerManager, simpleConsumerManager);

    String response =
        target("/topics/{topic}/partitions/{partition}/messages")
            .resolveTemplate("topic", TOPIC)
            .resolveTemplate("partition", PARTITION)
            .queryParam("timestamp", DATE_TIME_FORMATTER.format(TIMESTAMP))
            .request(Versions.KAFKA_V1_JSON_BINARY)
            .get(String.class);

    assertEquals("[]", response);

    verify(kafkaConsumerManager, simpleConsumerManager);
  }
}
