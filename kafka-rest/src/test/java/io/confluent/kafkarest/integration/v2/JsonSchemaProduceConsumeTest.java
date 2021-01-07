package io.confluent.kafkarest.integration.v2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.JsonSchemaConverter;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.SchemaTopicProduceRequest.SchemaTopicProduceRecord;

import java.util.Arrays;
import java.util.List;

public final class JsonSchemaProduceConsumeTest extends SchemaProduceConsumeTest {

  private static final JsonSchemaConverter JSON_SCHEMA_CONVERTER = new JsonSchemaConverter();

  private static final JsonSchema KEY_SCHEMA =
      new JsonSchema("{\"type\":\"number\"}");

  private static final JsonSchema VALUE_SCHEMA =
      new JsonSchema("{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"number\"}}}");

  private static final List<SchemaTopicProduceRecord> PRODUCE_RECORDS =
      Arrays.asList(
          new SchemaTopicProduceRecord(
              new IntNode(1),
              JSON_SCHEMA_CONVERTER.toJson(getMessage(11)).getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              new IntNode(2),
              JSON_SCHEMA_CONVERTER.toJson(getMessage(12)).getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              new IntNode(3),
              JSON_SCHEMA_CONVERTER.toJson(getMessage(13)).getJson(),
              /* partition= */ 0));

  private static JsonNode getMessage(int value) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode root = objectMapper.createObjectNode();
    root.put("value", value);
    return root;
  }

  public JsonSchemaProduceConsumeTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ true);
  }

  @Override
  protected EmbeddedFormat getFormat() {
    return EmbeddedFormat.JSONSCHEMA;
  }

  @Override
  protected String getContentType() {
    return Versions.KAFKA_V2_JSON_JSON_SCHEMA;
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
