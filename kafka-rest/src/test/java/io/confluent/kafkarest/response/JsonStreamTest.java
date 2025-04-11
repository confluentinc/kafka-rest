/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafkarest.response;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.exceptions.ProduceRequestTooLargeException;
import io.confluent.kafkarest.response.JsonStream.SizeLimitEntityStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import javax.ws.rs.BadRequestException;
import org.junit.jupiter.api.Test;

// CHECKSTYLE:OFF:ClassDataAbstractionCoupling
class JsonStreamTest {
  private static final Random RNG = new Random();
  private static final String TEST_MESSAGE_TEMPLATE =
      "{ " + "\"value\" : { " + "\"type\" : \"JSON\", " + "\"data\" : \"%s\" " + "}}";
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .registerModule(new GuavaModule())
          .registerModule(new Jdk8Module())
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"))
          .setTimeZone(TimeZone.getTimeZone("UTC"));
  private static final int TEST_DATA_SIZE = 1024 * 1024;

  @Test
  public void testReadOneProduceRequest_NoLimit() throws Exception {
    Properties properties = new Properties();
    properties.put(KafkaRestConfig.PRODUCE_REQUEST_SIZE_LIMIT_MAX_BYTES_CONFIG, "0");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    File tempFile = createTempFile(UUID.randomUUID().toString());
    // Use FileOutputStream and FileInputStream on the same file to simulate
    // writing from clients and reading in server
    try (FileOutputStream fos = new FileOutputStream(tempFile);
        FileInputStream fis = new FileInputStream(tempFile);
        JsonStream<ProduceRequest> jsonStream = createJsonStream(fis, config); ) {
      FileChannel outputChannel = fos.getChannel();
      // write a produce request message
      outputChannel.write(
          ByteBuffer.wrap(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8)));
      assertTrue(jsonStream.hasNext(), "Has one ProduceRequest");
      assertTrue(
          jsonStream.nextValue().getOriginalSize() > TEST_DATA_SIZE,
          "Produce request is larger than data size");
      assertFalse(jsonStream.hasNext(), "No more ProduceRequest");
    }
  }

  @Test
  public void testReadOneProduceRequest_WithinLimit() throws Exception {
    final long sizeLimit = TEST_DATA_SIZE + 8 * 1024;
    Properties properties = new Properties();
    properties.put(
        KafkaRestConfig.PRODUCE_REQUEST_SIZE_LIMIT_MAX_BYTES_CONFIG, String.valueOf(sizeLimit));
    KafkaRestConfig config = new KafkaRestConfig(properties);

    File tempFile = createTempFile(UUID.randomUUID().toString());
    // Use FileOutputStream and FileInputStream on the same file to simulate
    // writing from clients and reading in server
    try (FileOutputStream fos = new FileOutputStream(tempFile);
        FileInputStream fis = new FileInputStream(tempFile);
        JsonStream<ProduceRequest> jsonStream = createJsonStream(fis, config); ) {
      FileChannel outputChannel = fos.getChannel();
      // write a produce request message
      outputChannel.write(
          ByteBuffer.wrap(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8)));
      assertTrue(jsonStream.hasNext(), "Has one ProduceRequest");
      ProduceRequest produceRequest = jsonStream.nextValue();
      assertTrue(
          produceRequest.getOriginalSize() > TEST_DATA_SIZE,
          "Produce request is larger than data size");
      assertTrue(
          produceRequest.getOriginalSize() < sizeLimit,
          "Produce request is smaller than size limit");
      assertFalse(jsonStream.hasNext(), "No more ProduceRequest");
    }
  }

  @Test
  public void testReadOneProduceRequest_BeyondLimit() throws Exception {
    final long sizeLimit = 8 * 1024;
    Properties properties = new Properties();
    properties.put(
        KafkaRestConfig.PRODUCE_REQUEST_SIZE_LIMIT_MAX_BYTES_CONFIG, String.valueOf(sizeLimit));
    KafkaRestConfig config = new KafkaRestConfig(properties);

    File tempFile = createTempFile(UUID.randomUUID().toString());
    // Use FileOutputStream and FileInputStream on the same file to simulate
    // writing from clients and reading in server
    try (FileOutputStream fos = new FileOutputStream(tempFile);
        FileInputStream fis = new FileInputStream(tempFile);
        JsonStream<ProduceRequest> jsonStream = createJsonStream(fis, config); ) {
      FileChannel outputChannel = fos.getChannel();
      // write a produce request message that is bigger than limit
      outputChannel.write(
          ByteBuffer.wrap(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8)));
      assertTrue(jsonStream.hasNext(), "Has one ProduceRequest");
      Exception exception = assertThrows(JsonMappingException.class, jsonStream::nextValue);
      assertTrue(exception.getCause() instanceof ProduceRequestTooLargeException);
    }
  }

  @Test
  public void testStreamingProduceRequest_NoLimit() throws Exception {
    Properties properties = new Properties();
    properties.put(KafkaRestConfig.PRODUCE_REQUEST_SIZE_LIMIT_MAX_BYTES_CONFIG, "0");
    KafkaRestConfig config = new KafkaRestConfig(properties);

    File tempFile = createTempFile(UUID.randomUUID().toString());
    // Use FileOutputStream and FileInputStream on the same file to simulate
    // writing from clients and reading in server
    try (FileOutputStream fos = new FileOutputStream(tempFile);
        FileInputStream fis = new FileInputStream(tempFile);
        JsonStream<ProduceRequest> jsonStream = createJsonStream(fis, config); ) {
      FileChannel outputChannel = fos.getChannel();
      // write a produce request message
      outputChannel.write(
          ByteBuffer.wrap(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8)));
      assertTrue(jsonStream.hasNext(), "Has one ProduceRequest");
      assertTrue(
          jsonStream.nextValue().getOriginalSize() > TEST_DATA_SIZE,
          "Produce request is larger than data size");

      // write another produce request message
      outputChannel.write(
          ByteBuffer.wrap(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8)));
      assertTrue(jsonStream.hasNext(), "Has another ProduceRequest");
      assertTrue(
          jsonStream.nextValue().getOriginalSize() > TEST_DATA_SIZE,
          "Produce request is larger than data size");
      assertFalse(jsonStream.hasNext(), "No more ProduceRequest");
    }
  }

  @Test
  public void testStreamingProduceRequest_WithinLimit() throws Exception {
    final long sizeLimit = TEST_DATA_SIZE + 8 * 1024;
    Properties properties = new Properties();
    properties.put(
        KafkaRestConfig.PRODUCE_REQUEST_SIZE_LIMIT_MAX_BYTES_CONFIG, String.valueOf(sizeLimit));
    KafkaRestConfig config = new KafkaRestConfig(properties);

    File tempFile = createTempFile(UUID.randomUUID().toString());
    // Use FileOutputStream and FileInputStream on the same file to simulate
    // writing from clients and reading in server
    try (FileOutputStream fos = new FileOutputStream(tempFile);
        FileInputStream fis = new FileInputStream(tempFile);
        JsonStream<ProduceRequest> jsonStream = createJsonStream(fis, config); ) {
      FileChannel outputChannel = fos.getChannel();
      // write a produce request message
      outputChannel.write(
          ByteBuffer.wrap(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8)));
      assertTrue(jsonStream.hasNext(), "Has one ProduceRequest");
      ProduceRequest produceRequest = jsonStream.nextValue();
      assertTrue(
          produceRequest.getOriginalSize() > TEST_DATA_SIZE,
          "Produce request is larger than data size");
      assertTrue(
          produceRequest.getOriginalSize() < sizeLimit,
          "Produce request is smaller than size limit");

      // write another produce request message
      outputChannel.write(
          ByteBuffer.wrap(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8)));
      assertTrue(jsonStream.hasNext(), "Has another ProduceRequest");
      produceRequest = jsonStream.nextValue();
      assertTrue(
          produceRequest.getOriginalSize() > TEST_DATA_SIZE,
          "Produce request is larger than data size");
      assertTrue(
          produceRequest.getOriginalSize() < sizeLimit,
          "Produce request is smaller than size limit");
      assertFalse(jsonStream.hasNext(), "No more ProduceRequest");
    }
  }

  @Test
  public void testStreamingProduceRequest_BeyondLimitSecondMessage() throws Exception {
    final long sizeLimit = TEST_DATA_SIZE + 8 * 1024;
    Properties properties = new Properties();
    properties.put(
        KafkaRestConfig.PRODUCE_REQUEST_SIZE_LIMIT_MAX_BYTES_CONFIG, String.valueOf(sizeLimit));
    KafkaRestConfig config = new KafkaRestConfig(properties);

    File tempFile = createTempFile(UUID.randomUUID().toString());
    // Use FileOutputStream and FileInputStream on the same file to simulate
    // writing from clients and reading in server
    try (FileOutputStream fos = new FileOutputStream(tempFile);
        FileInputStream fis = new FileInputStream(tempFile);
        JsonStream<ProduceRequest> jsonStream = createJsonStream(fis, config); ) {
      FileChannel outputChannel = fos.getChannel();
      // write a produce request message
      outputChannel.write(
          ByteBuffer.wrap(generateData(TEST_DATA_SIZE).getBytes(StandardCharsets.UTF_8)));
      assertTrue(jsonStream.hasNext(), "Has one ProduceRequest");
      ProduceRequest produceRequest = jsonStream.nextValue();
      assertTrue(
          produceRequest.getOriginalSize() > TEST_DATA_SIZE,
          "Produce request is larger than data size");
      assertTrue(
          produceRequest.getOriginalSize() < sizeLimit,
          "Produce request is smaller than size limit");
      // write another produce request message that is bigger than limit
      outputChannel.write(
          ByteBuffer.wrap(
              generateData(TEST_DATA_SIZE + 16 * 1024).getBytes(StandardCharsets.UTF_8)));
      assertTrue(jsonStream.hasNext(), "Has second ProduceRequest");
      Exception exception = assertThrows(JsonMappingException.class, jsonStream::nextValue);
      assertTrue(exception.getCause() instanceof ProduceRequestTooLargeException);
    }
  }

  // this function mimics the creation of JsonStream in JsonStreamMessageBodyReader.readFrom
  // function
  private static JsonStream<ProduceRequest> createJsonStream(
      InputStream entityStream, KafkaRestConfig config) {
    JavaType wrappedType = OBJECT_MAPPER.constructType(ProduceRequest.class);
    SizeLimitEntityStream wrappedInputStream =
        (config.getProduceRequestSizeLimitMaxBytesConfig() > 0)
            ? new SizeLimitEntityStream(
                entityStream, config.getProduceRequestSizeLimitMaxBytesConfig())
            : null;
    return new JsonStream<>(
        () -> {
          try {
            JsonParser parser =
                OBJECT_MAPPER.createParser(
                    wrappedInputStream == null ? entityStream : wrappedInputStream);
            return OBJECT_MAPPER.readValues(parser, wrappedType);
          } catch (IOException e) {
            throw new BadRequestException("Unexpected error while starting JSON stream: ", e);
          }
        },
        wrappedInputStream);
  }

  private static String generateData(int size) {
    String value = TestUtils.generateAlphanumericString(RNG, size);
    return String.format(TEST_MESSAGE_TEMPLATE, value);
  }

  private static File createTempFile(String prefix) throws IOException {
    final File tempFile = File.createTempFile(prefix, ".jsonl");
    tempFile.deleteOnExit();
    return tempFile;
  }
}
// CHECKSTYLE:ON:ClassDataAbstractionCoupling
