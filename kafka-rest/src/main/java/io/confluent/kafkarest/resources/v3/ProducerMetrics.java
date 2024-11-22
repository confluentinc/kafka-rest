/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.resources.v3;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafkarest.KafkaRestConfig;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE:OFF:ClassDataAbstractionCoupling
final class ProducerMetrics {

  private static final Logger log = LoggerFactory.getLogger(ProducerMetrics.class);

  private static final String GROUP_NAME = "produce-api-metrics";

  /*
   * Rate, Average and Percentile metrics use underlying SampledStat and are therefore over a window
   * not equal to the whole lifetime of the sensor (default 30s)
   */
  // sensor names
  private static final String REQUEST_SENSOR_NAME = "request-sensor";
  private static final String REQUEST_SIZE_SENSOR_NAME = "request-size-sensor";
  private static final String RESPONSE_SENSOR_NAME = "response-sensor";
  private static final String RECORD_ERROR_SENSOR_NAME = "record-error-sensor";
  private static final String RECORD_RATE_LIMITED_SENSOR_NAME = "record-rate-limited-sensor";
  private static final String REQUEST_LATENCY_SENSOR_NAME = "request-latency-sensor";

  // request
  static final String REQUEST_RATE_METRIC_NAME = "request-rate";
  private static final String REQUEST_RATE_METRIC_DOC =
      "The average number of requests sent per second.";

  static final String REQUEST_COUNT_WINDOWED_METRIC_NAME = "request-count-windowed";
  private static final String REQUEST_COUNT_WINDOWED_METRIC_DOC =
      "The total number of requests sent within the given window.";

  // response
  static final String RESPONSE_RATE_METRIC_NAME = "response-rate";
  private static final String RESPONSE_RATE_METRIC_DOC =
      "The average number of responses sent per second.";

  static final String RESPONSE_COUNT_WINDOWED_METRIC_NAME = "response-count-windowed";
  private static final String RESPONSE_COUNT_WINDOWED_METRIC_DOC =
      "The total number of responses sent in the given window.";

  // errors
  static final String RECORD_ERROR_COUNT_WINDOWED_METRIC_NAME = "error-count-windowed";
  private static final String RECORD_ERROR_COUNT_WINDOWED_METRIC_DOC =
      "The total number of record sends that resulted in errors in the given window.";

  static final String RECORD_ERROR_RATE_METRIC_NAME = "record-error-rate";
  private static final String RECORD_ERROR_RATE_METRIC_DOC =
      "The average per-second number of record sends that resulted in errors.";

  // rate-limiting
  static final String RECORD_RATE_LIMITED_COUNT_WINDOWED_METRIC_NAME =
      "record-rate-limited-count-windowed";
  private static final String RECORD_RATE_LIMITED_COUNT_WINDOWED_METRIC_DOC =
      "The total number of record sends that resulted in rate-limit errors in the given window.";

  static final String RECORD_RATE_LIMITED_RATE_METRIC_NAME = "record-rate-limited-rate";
  private static final String RECORD_RATE_LIMITED_RATE_METRIC_DOC =
      "The average per-second number of record sends that resulted in rate-limit errors.";

  // latency
  static final String REQUEST_LATENCY_AVG_METRIC_NAME = "request-latency-avg";
  private static final String REQUEST_LATENCY_AVG_METRIC_DOC = "The average request latency";

  static final String REQUEST_LATENCY_MAX_METRIC_NAME = "request-latency-max";
  private static final String REQUEST_LATENCY_MAX_METRIC_DOC = "The max request latency";

  static final String REQUEST_LATENCY_PCT_METRIC_PREFIX = "request-latency-";
  private static final String REQUEST_LATENCY_PCT_METRIC_DOC = "Request latency percentiles.";

  private final Metrics metrics;
  private final String jmxPrefix;
  private final String requestSensorName;
  private final String requestSizeSensorName;
  private final String responseSensorName;
  private final String recordErrorSensorName;
  private final String recordRateLimitedSensorName;
  private final String requestLatencySensorName;

  // TODO: Delete once all usages have been removed.
  ProducerMetrics(KafkaRestConfig config, Time time, Map<String, String> metricsTags) {
    this(config, metricsTags);
  }

  ProducerMetrics(KafkaRestConfig config, Map<String, String> metricsTags) {
    this.metrics = requireNonNull(config.getMetrics());
    this.jmxPrefix = config.getString(KafkaRestConfig.METRICS_JMX_PREFIX_CONFIG);
    String sensorNamePrefix = jmxPrefix + ":" + GROUP_NAME + ":";
    this.recordErrorSensorName = sensorNamePrefix + RECORD_ERROR_SENSOR_NAME;
    this.recordRateLimitedSensorName = sensorNamePrefix + RECORD_RATE_LIMITED_SENSOR_NAME;
    this.requestSensorName = sensorNamePrefix + REQUEST_SENSOR_NAME;
    this.requestLatencySensorName = sensorNamePrefix + REQUEST_LATENCY_SENSOR_NAME;
    this.requestSizeSensorName = sensorNamePrefix + REQUEST_SIZE_SENSOR_NAME;
    this.responseSensorName = sensorNamePrefix + RESPONSE_SENSOR_NAME;
    setupSensors(metricsTags);
  }

  private void setupSensors(Map<String, String> metricsTags) {
    // request metrics
    setupRequestSensor(metricsTags);
    setupRequestSizeSensor(metricsTags);

    // response metrics
    setupResponseSensor(metricsTags);
    setupRecordErrorSensor(metricsTags);
    setupRecordRateLimitedSensor(metricsTags);
    setupRequestLatencySensor(metricsTags);

    log.info("Successfully registered kafka-rest produce metrics with JMX");
  }

  private void setupRequestSensor(Map<String, String> metricsTags) {
    Sensor requestSensor = createSensor(REQUEST_SENSOR_NAME);
    addAvg(requestSensor, REQUEST_RATE_METRIC_NAME, REQUEST_RATE_METRIC_DOC, metricsTags);
    addWindowedCount(
        requestSensor,
        REQUEST_COUNT_WINDOWED_METRIC_NAME,
        REQUEST_COUNT_WINDOWED_METRIC_DOC,
        metricsTags);
  }

  private void setupRequestSizeSensor(Map<String, String> metricsTags) {
    Sensor requestSizeSensor = createSensor(REQUEST_SIZE_SENSOR_NAME);
    requestSizeSensor.add(createMeter(metrics, metricsTags, "request-byte", "request bytes"));
  }

  private Meter createMeter(
      Metrics metrics, Map<String, String> metricTags, String baseName, String descriptiveName) {
    MetricName rateMetricName =
        metrics.metricName(
            baseName + "-rate",
            GROUP_NAME,
            String.format("The number of %s per second", descriptiveName),
            metricTags);
    MetricName totalMetricName =
        metrics.metricName(
            baseName + "-total",
            GROUP_NAME,
            String.format("The total number of %s", descriptiveName),
            metricTags);
    return new Meter(rateMetricName, totalMetricName);
  }

  private void setupResponseSensor(Map<String, String> metricsTags) {
    Sensor responseSensor = createSensor(RESPONSE_SENSOR_NAME);
    addRate(responseSensor, RESPONSE_RATE_METRIC_NAME, RESPONSE_RATE_METRIC_DOC, metricsTags);
    addWindowedCount(
        responseSensor,
        RESPONSE_COUNT_WINDOWED_METRIC_NAME,
        RESPONSE_COUNT_WINDOWED_METRIC_DOC,
        metricsTags);
  }

  private void setupRecordErrorSensor(Map<String, String> metricsTags) {
    Sensor recordErrorSensor = createSensor(RECORD_ERROR_SENSOR_NAME);
    addRate(
        recordErrorSensor,
        RECORD_ERROR_RATE_METRIC_NAME,
        RECORD_ERROR_RATE_METRIC_DOC,
        metricsTags);
    addWindowedCount(
        recordErrorSensor,
        RECORD_ERROR_COUNT_WINDOWED_METRIC_NAME,
        RECORD_ERROR_COUNT_WINDOWED_METRIC_DOC,
        metricsTags);
  }

  private void setupRecordRateLimitedSensor(Map<String, String> metricsTags) {
    Sensor recordRateLimitedSensor = createSensor(RECORD_RATE_LIMITED_SENSOR_NAME);
    addRate(
        recordRateLimitedSensor,
        RECORD_RATE_LIMITED_RATE_METRIC_NAME,
        RECORD_RATE_LIMITED_RATE_METRIC_DOC,
        metricsTags);
    addWindowedCount(
        recordRateLimitedSensor,
        RECORD_RATE_LIMITED_COUNT_WINDOWED_METRIC_NAME,
        RECORD_RATE_LIMITED_COUNT_WINDOWED_METRIC_DOC,
        metricsTags);
  }

  private void setupRequestLatencySensor(Map<String, String> metricsTags) {
    Sensor requestLatencySensor = createSensor(REQUEST_LATENCY_SENSOR_NAME);
    addMax(
        requestLatencySensor,
        REQUEST_LATENCY_MAX_METRIC_NAME,
        REQUEST_LATENCY_MAX_METRIC_DOC,
        metricsTags);
    addAvg(
        requestLatencySensor,
        REQUEST_LATENCY_AVG_METRIC_NAME,
        REQUEST_LATENCY_AVG_METRIC_DOC,
        metricsTags);
    addPercentiles(
        requestLatencySensor,
        REQUEST_LATENCY_PCT_METRIC_PREFIX,
        ImmutableMap.of(
            "p95", 0.95,
            "p99", 0.99,
            "p999", 0.999),
        REQUEST_LATENCY_PCT_METRIC_DOC,
        metricsTags);
  }

  private Sensor createSensor(String name) {
    return metrics.sensor(String.join(":", jmxPrefix, GROUP_NAME, name));
  }

  private void addAvg(Sensor sensor, String name, String doc, Map<String, String> metricsTags) {
    sensor.add(getMetricName(name, doc, metricsTags), new Avg());
  }

  private void addRate(Sensor sensor, String name, String doc, Map<String, String> metricsTags) {
    sensor.add(getMetricName(name, doc, metricsTags), new Rate());
  }

  private void addMax(Sensor sensor, String name, String doc, Map<String, String> metricsTags) {
    sensor.add(getMetricName(name, doc, metricsTags), new Max());
  }

  private void addWindowedCount(
      Sensor sensor, String name, String doc, Map<String, String> metricsTags) {
    sensor.add(getMetricName(name, doc, metricsTags), new WindowedCount());
  }

  private void addPercentiles(
      Sensor sensor,
      String prefix,
      Map<String, Double> percentiles,
      String doc,
      Map<String, String> metricsTags) {
    sensor.add(
        new Percentiles(
            30 * 1000 * 4,
            30 * 1000,
            Percentiles.BucketSizing.CONSTANT,
            percentiles.entrySet().stream()
                .map(
                    entry ->
                        new Percentile(
                            getMetricName(prefix + entry.getKey(), doc, metricsTags),
                            entry.getValue()))
                .toArray(Percentile[]::new)));
  }

  private MetricName getMetricName(String name, String doc, Map<String, String> metricsTags) {
    return metrics.metricInstance(
        new MetricNameTemplate(name, GROUP_NAME, doc, metricsTags.keySet()), metricsTags);
  }

  void recordResponse() {
    recordMetric(responseSensorName, 1.0);
  }

  void recordRequestLatency(long valueMs) {
    recordMetric(requestLatencySensorName, valueMs);
  }

  void recordError() {
    recordMetric(recordErrorSensorName, 1.0);
  }

  void recordRateLimited() {
    recordMetric(recordRateLimitedSensorName, 1.0);
  }

  void recordRequest() {
    recordMetric(requestSensorName, 1.0);
  }

  void recordRequestSize(double value) {
    recordMetric(requestSizeSensorName, value);
  }

  private void recordMetric(String sensorName, double value) {
    Sensor sensor = metrics.getSensor(sensorName);
    if (sensor != null) {
      sensor.record(value);
    }
  }
  // CHECKSTYLE:ON:ClassDataAbstractionCoupling
}
