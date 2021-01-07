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

package io.confluent.kafkarest.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.common.utils.AbstractPerformanceTest;
import io.confluent.common.utils.PerformanceStats;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryTopicProduceRequest;
import io.confluent.rest.entities.ErrorMessage;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;

public class ProducerPerformance extends AbstractPerformanceTest {

  long iterations;
  long iterationsPerSec;
  int recordsPerIteration;
  long bytesPerIteration;

  String targetUrl;
  String requestEntityLength;
  byte[] requestEntity;
  byte[] buffer;

  private final ObjectMapper jsonDeserializer = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    if (args.length < 6) {
      System.out.println(
          "Usage: java " + ProducerPerformance.class.getName() + " rest_url topic_name "
          + "num_records record_size batch_size target_records_sec"
      );
      System.exit(1);
    }

    String baseUrl = args[0];
    String topic = args[1];
    int numRecords = Integer.parseInt(args[2]);
    int recordSize = Integer.parseInt(args[3]);
    int batchSize = Integer.parseInt(args[4]);
    int throughput = Integer.parseInt(args[5]) / batchSize;

    ProducerPerformance
        perf =
        new ProducerPerformance(
            baseUrl,
            topic,
            numRecords / batchSize,
            batchSize,
            throughput,
            recordSize
        );
    perf.run(throughput);
  }

  public ProducerPerformance(
      String baseUrl, String topic, long iterations, int recordsPerIteration,
      long iterationsPerSec, int recordSize
  ) throws Exception {
    super(iterations * recordsPerIteration);
    this.iterations = iterations;
    this.iterationsPerSec = iterationsPerSec;
    this.recordsPerIteration = recordsPerIteration;
    this.bytesPerIteration = recordsPerIteration * recordSize;

    /* setup perf test */
    targetUrl = baseUrl + "/topics/" + topic;
    BinaryTopicProduceRequest.BinaryTopicProduceRecord record =
            new BinaryTopicProduceRequest.BinaryTopicProduceRecord(null, "payload", null);
    BinaryTopicProduceRequest.BinaryTopicProduceRecord[] records =
            new BinaryTopicProduceRequest.BinaryTopicProduceRecord[recordsPerIteration];
    Arrays.fill(records, record);
    BinaryTopicProduceRequest request = BinaryTopicProduceRequest.create(Arrays.asList(records));
    requestEntity = new ObjectMapper().writeValueAsBytes(request);
    requestEntityLength = Integer.toString(requestEntity.length);
    buffer = new byte[1024 * 1024];
  }

  @Override
  protected void doIteration(PerformanceStats.Callback cb) {
    HttpURLConnection connection = null;
    try {
      URL url = new URL(targetUrl);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", Versions.KAFKA_V2_JSON);
      connection.setRequestProperty("Content-Length", requestEntityLength);

      connection.setUseCaches(false);
      connection.setDoInput(true);
      connection.setDoOutput(true);

      OutputStream os = connection.getOutputStream();
      os.write(requestEntity);
      os.flush();
      os.close();

      int responseStatus = connection.getResponseCode();
      if (responseStatus >= 400) {
        InputStream es = connection.getErrorStream();
        ErrorMessage errorMessage = jsonDeserializer.readValue(es, ErrorMessage.class);
        es.close();
        throw new RuntimeException(
            String.format("Unexpected HTTP error status %d: %s",
                          responseStatus, errorMessage.getMessage()
            ));
      }
      InputStream is = connection.getInputStream();
      while (is.read(buffer) > 0) {
        // Ignore output, just make sure we actually receive it
      }
      is.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
    cb.onCompletion(recordsPerIteration, bytesPerIteration);
  }

  @Override
  protected boolean finished(int iteration) {
    return iteration >= iterations;
  }

  @Override
  protected boolean runningFast(int iteration, float elapsed) {
    return (iteration / elapsed > iterationsPerSec);
  }
}

