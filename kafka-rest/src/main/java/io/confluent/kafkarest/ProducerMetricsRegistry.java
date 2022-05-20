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

package io.confluent.kafkarest;

public class ProducerMetricsRegistry {

  public static final String GROUP_NAME = "produce-api-metrics";

  /*
  Rate, Average and Percentile metrics use underlying SampledStat and are therefore over a window
  not equal to the whole lifetime of the sensor (default 30s)
   */
  // sensor names
  public static final String REQUEST_SENSOR = "request-sensor";
  public static final String REQUEST_SIZE_SENSOR = "request-size-sensor";
  public static final String RESPONSE_SENSOR = "response-sensor";
  public static final String RECORD_ERROR_SENSOR = "record-error-sensor";
  public static final String REQUEST_LATENCY_SENSOR = "request-latency-sensor";

  // request
  public static final String REQUEST_RATE = "request-rate";
  public static final String REQUEST_RATE_DOC = "The average number of requests sent per second.";

  public static final String REQUEST_SIZE_AVG = "request-size-avg";
  public static final String REQUEST_SIZE_AVG_DOC = "The average request size in bytes.";

  public static final String REQUEST_COUNT_WINDOWED = "request-count-windowed";
  public static final String REQUEST_COUNT_WINDOWED_DOC =
      "The total number of requests sent within the given window.";

  // response
  public static final String RESPONSE_SEND_RATE = "response-rate";
  public static final String RESPONSE_SEND_RATE_DOC =
      "The average number of responses sent per second.";

  public static final String RESPONSE_COUNT_WINDOWED = "response-count-windowed";
  public static final String RESPONSE_COUNT_WINDOWED_DOC =
      "The total number of responses sent in the given window.";

  // errors
  public static final String ERROR_COUNT_WINDOWED = "error-count-windowed";
  public static final String ERROR_COUNT_WINDOWED_DOC =
      "The total number of record sends that resulted in errors in the given window.";

  public static final String RECORD_ERROR_RATE = "record-error-rate";
  public static final String RECORD_ERROR_RATE_DOC =
      "The average per-second number of record sends that resulted in errors.";

  // latency
  public static final String REQUEST_LATENCY_AVG = "request-latency-avg";
  public static final String REQUEST_LATENCY_AVG_DOC = "The average request latency";

  public static final String REQUEST_LATENCY_MAX = "request-latency-max";
  public static final String REQUEST_LATENCY_MAX_DOC = "The max request latency";

  public static final String REQUEST_LATENCY_PCT = "request-latency-avg-";
  public static final String REQUEST_LATENCY_PCT_DOC = "Request latency percentiles.";
}
