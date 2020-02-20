package io.confluent.kafkarest.integration.v2;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.ProtobufConverter;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.SchemaTopicProduceRequest.SchemaTopicProduceRecord;

import java.util.Arrays;
import java.util.List;

public final class ProtobufProduceConsumeTest extends SchemaProduceConsumeTest {

  private static final ProtobufConverter PROOTBUF_CONVERTER = new ProtobufConverter();

  private static final ProtobufSchema KEY_SCHEMA =
      new ProtobufSchema("syntax = \"proto3\"; message KeyRecord { int32 key = 1; }");

  private static final ProtobufSchema VALUE_SCHEMA =
      new ProtobufSchema("syntax = \"proto3\"; message ValueRecord { int32 value = 1; }");

  private static final List<SchemaTopicProduceRecord> PRODUCE_RECORDS =
      Arrays.asList(
          new SchemaTopicProduceRecord(
              PROOTBUF_CONVERTER.toJson(getMessage(KEY_SCHEMA, "key", 1)).getJson(),
              PROOTBUF_CONVERTER.toJson(getMessage(VALUE_SCHEMA, "value", 11)).getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              PROOTBUF_CONVERTER.toJson(getMessage(KEY_SCHEMA, "key", 2)).getJson(),
              PROOTBUF_CONVERTER.toJson(getMessage(VALUE_SCHEMA, "value", 12)).getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              PROOTBUF_CONVERTER.toJson(getMessage(KEY_SCHEMA, "key", 3)).getJson(),
              PROOTBUF_CONVERTER.toJson(getMessage(VALUE_SCHEMA, "value", 13)).getJson(),
              /* partition= */ 0));

  private static Message getMessage(ProtobufSchema schema, String fieldName, int value) {
    DynamicMessage.Builder messageBuilder = schema.newMessageBuilder();
    Descriptors.FieldDescriptor fieldDesc =
        messageBuilder.getDescriptorForType().findFieldByName(fieldName);
    messageBuilder.setField(fieldDesc, value);
    return messageBuilder.build();
  }

  public ProtobufProduceConsumeTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ true);
  }

  @Override
  protected EmbeddedFormat getFormat() {
    return EmbeddedFormat.PROTOBUF;
  }

  @Override
  protected String getContentType() {
    return Versions.KAFKA_V2_JSON_PROTOBUF;
  }

  @Override
  protected ParsedSchema getKeySchema() {
    return KEY_SCHEMA;
  }

  @Override
  protected ParsedSchema getValueSchema() {
    return VALUE_SCHEMA;
  }

  @Override
  protected List<SchemaTopicProduceRecord> getProduceRecords() {
    return PRODUCE_RECORDS;
  }
}
