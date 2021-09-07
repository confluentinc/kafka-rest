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

  // request
  public static final String REQUEST_RATE = "request-send-rate";
  public static final String REQUEST_RATE_DOC = "The average number of requests sent per second.";

  public static final String REQUEST_SIZE_AVG = "request-size-avg";
  public static final String REQUEST_SIZE_AVG_DOC = "The average request size in bytes";

  public static final String REQUEST_SIZE_TOTAL = "request-size-total";
  public static final String REQUEST_SIZE_TOTAL_DOC = "The total request bytes sent";

  public static final String REQUEST_TOTAL = "request-total";
  public static final String REQUEST_TOTAL_DOC = "The total number of requests sent.";

  // response
  public static final String RESPONSE_SEND_RATE = "response-send-rate";
  public static final String RESPONSE_SEND_RATE_DOC =
      "The average number of response sent per second.";

  public static final String RESPONSE_SIZE_AVG = "response-size-avg";
  public static final String RESPONSE_SIZE_AVG_DOC = "The average response size in bytes";

  public static final String RESPONSE_SIZE_TOTAL = "response-size-total";
  public static final String RESPONSE_SIZE_TOTAL_DOC = "The total response bytes sent";

  public static final String RESPONSE_TOTAL = "response-total";
  public static final String RESPONSE_TOTAL_DOC = "The total number of response sent.";

  // errors
  public static final String RECORD_ERROR_TOTAL = "record-error-total";
  public static final String RECORD_ERROR_TOTAL_DOC =
      "The total number of record sends that resulted in errors.";

  public static final String RECORD_ERROR_RATE = "record-error-rate";
  public static final String RECORD_ERROR_RATE_DOC =
      "The average per-second number of record sends that resulted in errors.";

  // latency
  public static final String REQUEST_LATENCY_AVG = "request-latency-avg";
  public static final String REQUEST_LATENCY_AVG_DOC = "The average request latency";

  public static final String REQUEST_LATENCY_MAX = "request-latency-max";
  public static final String REQUEST_LATENCY_MAX_DOC = "The max request latency";
}
