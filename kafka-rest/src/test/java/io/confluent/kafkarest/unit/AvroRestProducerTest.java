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

package io.confluent.kafkarest.unit;

import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.ProduceTask;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.SchemaRestProducer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceRequest;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.util.Collections;
import java.util.concurrent.Future;
import javax.validation.ConstraintViolationException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class AvroRestProducerTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  private KafkaAvroSerializer keySerializer;
  private KafkaAvroSerializer valueSerializer;
  private KafkaProducer<Object, Object> producer;
  private SchemaRestProducer restProducer;
  private ProduceRequest<JsonNode, JsonNode> schemaHolder;
  private ProducerPool.ProduceRequestCallback produceCallback;

  @Before
  public void setUp() {
    keySerializer = EasyMock.createMock(KafkaAvroSerializer.class);
    valueSerializer = EasyMock.createMock(KafkaAvroSerializer.class);
    producer = EasyMock.createMock(KafkaProducer.class);
    restProducer = new SchemaRestProducer(producer, keySerializer, valueSerializer,
        new AvroSchemaProvider(), new AvroConverter());
    produceCallback = EasyMock.createMock(ProducerPool.ProduceRequestCallback.class);
  }

  @Test(expected= ConstraintViolationException.class)
  public void testInvalidSchema() throws Exception {
    schemaHolder =
        ProduceRequest.create(
            Collections.singletonList(
                ProduceRecord.create(mapper.readTree("{}"), mapper.readTree("{}"), null)),
            /* keySchema= */ null,
            /* keySchemaId= */ null,
            "invalidValueSchema",
            /* valueSchemaId= */ null);
    restProducer.produce(
        new ProduceTask(schemaHolder, 1, produceCallback),
        /* topic= */ "test",
        /* partition= */ null,
        schemaHolder.getRecords());
  }

  @Test
  public void testInvalidData() throws Exception {
    schemaHolder =
        ProduceRequest.create(
            Collections.singletonList(
                ProduceRecord.create(null, mapper.readTree("\"string\""), null)),
            /* keySchema= */ null,
            /* keySchemaId= */ null,
            /* valueSchema= */ "\"int\"",
            /* valueSchemaId= */ null);
    try {
      restProducer.produce(
          new ProduceTask(schemaHolder, 1, produceCallback),
          /* topic= */ "test",
          /* partition= */ null,
          schemaHolder.getRecords());
    } catch (RestConstraintViolationException e) {
      // expected, but should contain additional info
      assert (e.getMessage().startsWith(Errors.JSON_CONVERSION_MESSAGE));
      assert (e.getMessage().length() > Errors.JSON_CONVERSION_MESSAGE.length());
    } catch (Throwable t) {
      fail("Unexpected exception type");
    }
  }

  @Test
  public void testRepeatedProducer() throws Exception {
    final int schemaId = 1;
    final String valueSchemaStr =
        ""
            + "{"
            + "\"type\": \"record\","
            + "\"name\": \"User\","
            + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]"
            + "}";
    final ParsedSchema valueSchema = new AvroSchema(valueSchemaStr);
    // This is the key part of the test, we should only call register once with the same schema, and then see the lookup
    // by ID the rest of the times
    EasyMock.expect(
        valueSerializer.register(EasyMock.isA(String.class), EasyMock.isA(ParsedSchema.class)))
        .andReturn(schemaId);
    EasyMock.expect(valueSerializer.getSchemaById(schemaId)).andReturn(valueSchema).times(9999);
    EasyMock.replay(valueSerializer);
    Future f = EasyMock.createMock(Future.class);
    EasyMock.expect(
        producer.send(EasyMock.isA(ProducerRecord.class), EasyMock.isA(Callback.class)))
        .andStubReturn(f);
    EasyMock.replay(producer);
    schemaHolder =
        ProduceRequest.create(
            Collections.singletonList(
                ProduceRecord.create(null, mapper.readTree("{\"name\": \"bob\"}"), null)),
            /* keySchema= */ null,
            /* keySchemaId= */ null,
            valueSchemaStr,
            /* valueSchemaId= */ null);
    for (int i = 0; i < 10000; ++i) {
      restProducer.produce(
          new ProduceTask(schemaHolder, 1, produceCallback),
          /* topic= */ "test",
          /* partition= */ null,
          schemaHolder.getRecords());
    }

  }
}
