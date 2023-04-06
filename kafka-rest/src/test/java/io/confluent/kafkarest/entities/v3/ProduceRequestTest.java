/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.entities.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProduceRequestTest {

  private static final String REQUEST =
      "{\"key\":{\"type\":\"BINARY\",\"data\":\"Zm9v\"},\"value\":{\"type\":\"BINARY\","
          + "\"data\":\"YmFy\"}}";

  private ObjectMapper mapper;

  @BeforeEach
  public void setup() {
    mapper = new ObjectMapper();
    mapper.registerModule(new GuavaModule());
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"));
    mapper.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  @Test
  public void testProduceRequestDeserializerAddsSizeFromString() throws JsonProcessingException {
    ProduceRequest pr = mapper.readValue(REQUEST, ProduceRequest.class);

    assertEquals(REQUEST.length(), pr.getOriginalSize());
  }

  @Test
  public void testProduceRequestDeserializerAddsSizeFromStream() throws IOException {

    InputStream requestStream = new ByteArrayInputStream(REQUEST.getBytes());
    ProduceRequest pr = mapper.readValue(requestStream, ProduceRequest.class);

    assertEquals(REQUEST.length(), pr.getOriginalSize());
  }

  @Test
  public void testProduceRequestDeserializerUsesBaseDeserializerValues()
      throws JsonProcessingException {
    ObjectMapper baseMapper = new ObjectMapper();
    baseMapper.registerModule(new GuavaModule());
    baseMapper.registerModule(new Jdk8Module());
    baseMapper.registerModule(new JavaTimeModule());
    baseMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    baseMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"));
    baseMapper.setTimeZone(TimeZone.getTimeZone("UTC"));

    ProduceRequest basePr = baseMapper.readValue(REQUEST, ProduceRequest.class);
    ProduceRequest pr = mapper.readValue(REQUEST, ProduceRequest.class);

    assertEquals(basePr.getKey(), pr.getKey());
    assertEquals(basePr.getValue(), pr.getValue());
    assertEquals(basePr.getTimestamp(), pr.getTimestamp());
    assertEquals(basePr.getHeaders(), pr.getHeaders());
    assertEquals(basePr.getPartitionId(), pr.getPartitionId());
  }
}
