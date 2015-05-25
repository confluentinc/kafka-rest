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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import javax.validation.ConstraintViolationException;

import io.confluent.kafkarest.AvroRestProducer;
import io.confluent.kafkarest.ProduceTask;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.entities.AvroTopicProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;

public class AvroRestProducerTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  private KafkaProducer<Object, Object> producer;
  private AvroRestProducer restProducer;
  private SchemaHolder schemaHolder;
  private ProducerPool.ProduceRequestCallback produceCallback;

  @Before
  public void setUp() {
    producer = EasyMock.createMock(KafkaProducer.class);
    restProducer = new AvroRestProducer(producer, null, null);
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
}
