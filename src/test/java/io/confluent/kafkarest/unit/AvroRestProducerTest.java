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

package io.confluent.kafkarest.unit;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Future;

import javax.validation.ConstraintViolationException;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafkarest.AvroRestProducer;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.ProduceTask;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.entities.AvroTopicProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;
import io.confluent.rest.exceptions.RestConstraintViolationException;

import static org.junit.Assert.fail;

public class AvroRestProducerTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  private KafkaAvroSerializer keySerializer;
  private KafkaAvroSerializer valueSerializer;
  private KafkaProducer<Object, Object> producer;
  private AvroRestProducer restProducer;
  private SchemaHolder schemaHolder;
  private ProducerPool.ProduceRequestCallback produceCallback;

  @Before
  public void setUp() {
    keySerializer = EasyMock.createMock(KafkaAvroSerializer.class);
    valueSerializer = EasyMock.createMock(KafkaAvroSerializer.class);
    producer = EasyMock.createMock(KafkaProducer.class);
    restProducer = new AvroRestProducer(producer, keySerializer, valueSerializer);
    produceCallback = EasyMock.createMock(ProducerPool.ProduceRequestCallback.class);
  }

  @Test(expected= ConstraintViolationException.class)
  public void testInvalidSchema() throws Exception {
    schemaHolder = new SchemaHolder(null, "invalidValueSchema");
    restProducer.produce(
        new ProduceTask(schemaHolder, 1, produceCallback),
        "test", null,
        Arrays.asList(
            new AvroTopicProduceRecord(
                mapper.readTree("{}"),
                mapper.readTree("{}"),
                null)
        )
    );
  }

  @Test
  public void testInvalidData() throws Exception {
    schemaHolder = new SchemaHolder(null, "\"int\"");
    try {
      restProducer.produce(
          new ProduceTask(schemaHolder, 1, produceCallback),
          "test", null,
          Arrays.asList(
              new AvroTopicProduceRecord(
                  null,
                  mapper.readTree("\"string\""),
                  null
              )
          )
      );
    } catch (RestConstraintViolationException e) {
      // expected, but should contain additional info
      assert(e.getMessage().startsWith(Errors.JSON_AVRO_CONVERSION_MESSAGE));
      assert(e.getMessage().length() > Errors.JSON_AVRO_CONVERSION_MESSAGE.length());
    } catch (Throwable t) {
      fail("Unexpected exception type");
    }
  }

  @Test
  public void testRepeatedProducer() throws Exception {
    final int schemaId = 1;
    final String valueSchemaStr = "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    final Schema valueSchema = new Schema.Parser().parse(valueSchemaStr);
    // This is the key part of the test, we should only call register once with the same schema, and then see the lookup
    // by ID the rest of the times
    EasyMock.expect(valueSerializer.register(EasyMock.isA(String.class), EasyMock.isA(Schema.class))).andReturn(schemaId);
    EasyMock.expect(valueSerializer.getById(schemaId)).andReturn(valueSchema).times(9999);
    EasyMock.replay(valueSerializer);
    Future f = EasyMock.createMock(Future.class);
    EasyMock.expect(producer.send(EasyMock.isA(ProducerRecord.class), EasyMock.isA(Callback.class))).andStubReturn(f);
    EasyMock.replay(producer);
    schemaHolder = new SchemaHolder(null, valueSchemaStr);
    for (int i = 0; i < 10000; ++i) {
      restProducer.produce(
              new ProduceTask(schemaHolder, 1, produceCallback),
              "test",
              null,
              Arrays.asList(
                      new AvroTopicProduceRecord(null, mapper.readTree("{\"name\": \"bob\"}"), null)
              )
      );
    }

  }
}
