/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.integration.v2;

import com.fasterxml.jackson.databind.node.IntNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.SchemaTopicProduceRequest.SchemaTopicProduceRecord;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;

public final class AvroProduceConsumeTest extends SchemaProduceConsumeTest {

  private static final AvroConverter AVRO_CONVERTER = new AvroConverter();

  private static final Schema KEY_SCHEMA =
      new Schema.Parser().parse("" + "{" + "  \"type\": \"int\"," + "  \"name\": \"key\"" + "}");

  private static final Schema VALUE_SCHEMA =
      new Schema.Parser()
          .parse(
              ""
                  + "{"
                  + "  \"type\": \"record\", "
                  + "  \"name\": \"ValueRecord\","
                  + "  \"fields\":[{"
                  + "    \"name\": \"value\", "
                  + "    \"type\": \"int\""
                  + "  }]"
                  + "}");

  private static final List<SchemaTopicProduceRecord> PRODUCE_RECORDS =
      Arrays.asList(
          new SchemaTopicProduceRecord(
              new IntNode(1),
              AVRO_CONVERTER
                  .toJson(new GenericRecordBuilder(VALUE_SCHEMA).set("value", 11).build())
                  .getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              new IntNode(2),
              AVRO_CONVERTER
                  .toJson(new GenericRecordBuilder(VALUE_SCHEMA).set("value", 12).build())
                  .getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              new IntNode(3),
              AVRO_CONVERTER
                  .toJson(new GenericRecordBuilder(VALUE_SCHEMA).set("value", 13).build())
                  .getJson(),
              /* partition= */ 0));

  @Override
  protected EmbeddedFormat getFormat() {
    return EmbeddedFormat.AVRO;
  }

  @Override
  protected String getContentType() {
    return Versions.KAFKA_V2_JSON_AVRO;
  }

  @Override
  protected ParsedSchema getKeySchema() {
    return new AvroSchema(KEY_SCHEMA);
  }

  @Override
  protected ParsedSchema getValueSchema() {
    return new AvroSchema(VALUE_SCHEMA);
  }

  @Override
  protected List<SchemaTopicProduceRecord> getProduceRecords() {
    return PRODUCE_RECORDS;
  }
}
