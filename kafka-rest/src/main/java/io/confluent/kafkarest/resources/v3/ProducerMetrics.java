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

package io.confluent.kafkarest.resources.v3;

import static java.util.Collections.singletonList;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.rest.metrics.RestMetricsContext;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProducerMetrics {
  private static final Logger log = LoggerFactory.getLogger(ProducerMetrics.class);

  private static final String GROUP_NAME = "produce-api-metrics";
  private static final ImmutableMap<String, String> METRIC_TAGS = ImmutableMap.of();
  private static final int NUM_SAMPLES = 10;
  private static final long SAMPLE_WINDOW_MS = 10000L;
  private static final Sensor.RecordingLevel RECORDING_LEVEL = Sensor.RecordingLevel.INFO;

  /*
   * Rate, Average and Percentile metrics use underlying SampledStat and are therefore over a window
   * not equal to the whole lifetime of the sensor (default 30s)
   */
  // sensor names
  private static final String REQUEST_SENSOR_NAME = "request-sensor";
  private static final String REQUEST_SIZE_SENSOR_NAME = "request-size-sensor";
  private static final String RESPONSE_SENSOR_NAME = "response-sensor";
  private static final String RECORD_ERROR_SENSOR_NAME = "record-error-sensor";
  private static final String REQUEST_LATENCY_SENSOR_NAME = "request-latency-sensor";

  // request
  static final String REQUEST_RATE_METRIC_NAME = "request-rate";
  private static final String REQUEST_RATE_METRIC_DOC =
      "The average number of requests sent per second.";

  static final String REQUEST_SIZE_AVG_METRIC_NAME = "request-size-avg";
  private static final String REQUEST_SIZE_AVG_METRIC_DOC = "The average request size in bytes.";

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
  private final String requestLatencySensorName;

  ProducerMetrics(KafkaRestConfig config, Time time) {
    this.metrics = createMetrics(config.getMetricsContext(), time);
    this.jmxPrefix = config.getString(KafkaRestConfig.METRICS_JMX_PREFIX_CONFIG);
    String sensorNamePrefix = jmxPrefix + ":" + GROUP_NAME + ":";
    this.recordErrorSensorName = sensorNamePrefix + RECORD_ERROR_SENSOR_NAME;
    this.requestSensorName = sensorNamePrefix + REQUEST_SENSOR_NAME;
    this.requestLatencySensorName = sensorNamePrefix + REQUEST_LATENCY_SENSOR_NAME;
    this.requestSizeSensorName = sensorNamePrefix + REQUEST_SIZE_SENSOR_NAME;
    this.responseSensorName = sensorNamePrefix + RESPONSE_SENSOR_NAME;
    setupSensors();
  }

  private static Metrics createMetrics(RestMetricsContext metricsContext, Time time) {
    JmxReporter reporter = new JmxReporter();
    reporter.contextChange(metricsContext);
    return new Metrics(
        new MetricConfig()
            .samples(NUM_SAMPLES)
            .timeWindow(SAMPLE_WINDOW_MS, TimeUnit.MILLISECONDS)
            .recordLevel(RECORDING_LEVEL),
        singletonList(reporter),
        time,
        metricsContext);
  }

  private void setupSensors() {
    // request metrics
    setupRequestSensor();
    setupRequestSizeSensor();

    // response metrics
    setupResponseSensor();
    setupRecordErrorSensor();
    setupRequestLatencySensor();

    log.info("Successfully registered kafka-rest produce metrics with JMX");
  }

  private void setupRequestSensor() {
    Sensor requestSensor = createSensor(REQUEST_SENSOR_NAME);
    addAvg(requestSensor, REQUEST_RATE_METRIC_NAME, REQUEST_RATE_METRIC_DOC);
    addWindowedCount(
        requestSensor, REQUEST_COUNT_WINDOWED_METRIC_NAME, REQUEST_COUNT_WINDOWED_METRIC_DOC);
  }

  private void setupRequestSizeSensor() {
    Sensor requestSizeSensor = createSensor(REQUEST_SIZE_SENSOR_NAME);
    addAvg(requestSizeSensor, REQUEST_SIZE_AVG_METRIC_NAME, REQUEST_SIZE_AVG_METRIC_DOC);
  }

  private void setupResponseSensor() {
    Sensor responseSensor = createSensor(RESPONSE_SENSOR_NAME);
    addRate(responseSensor, RESPONSE_RATE_METRIC_NAME, RESPONSE_RATE_METRIC_DOC);
    addWindowedCount(
        responseSensor, RESPONSE_COUNT_WINDOWED_METRIC_NAME, RESPONSE_COUNT_WINDOWED_METRIC_DOC);
  }

  private void setupRecordErrorSensor() {
    Sensor recordErrorSensor = createSensor(RECORD_ERROR_SENSOR_NAME);
    addRate(recordErrorSensor, RECORD_ERROR_RATE_METRIC_NAME, RECORD_ERROR_RATE_METRIC_DOC);
    addWindowedCount(
        recordErrorSensor,
        RECORD_ERROR_COUNT_WINDOWED_METRIC_NAME,
        RECORD_ERROR_COUNT_WINDOWED_METRIC_DOC);
  }

  private void setupRequestLatencySensor() {
    Sensor requestLatencySensor = createSensor(REQUEST_LATENCY_SENSOR_NAME);
    addMax(requestLatencySensor, REQUEST_LATENCY_MAX_METRIC_NAME, REQUEST_LATENCY_MAX_METRIC_DOC);
    addAvg(requestLatencySensor, REQUEST_LATENCY_AVG_METRIC_NAME, REQUEST_LATENCY_AVG_METRIC_DOC);
    addPercentiles(
        requestLatencySensor,
        REQUEST_LATENCY_PCT_METRIC_PREFIX,
        ImmutableMap.of(
            "p95", 0.95,
            "p99", 0.99,
            "p999", 0.999),
        REQUEST_LATENCY_PCT_METRIC_DOC);
  }

  private Sensor createSensor(String name) {
    return metrics.sensor(String.join(":", jmxPrefix, GROUP_NAME, name));
  }

  private void addAvg(Sensor sensor, String name, String doc) {
    sensor.add(getMetricName(name, doc), new Avg());
  }

  private void addRate(Sensor sensor, String name, String doc) {
    sensor.add(getMetricName(name, doc), new Rate());
  }

  private void addMax(Sensor sensor, String name, String doc) {
    sensor.add(getMetricName(name, doc), new Max());
  }

  private void addWindowedCount(Sensor sensor, String name, String doc) {
    sensor.add(getMetricName(name, doc), new WindowedCount());
  }

  private void addPercentiles(
      Sensor sensor, String prefix, Map<String, Double> percentiles, String doc) {
    sensor.add(
        new Percentiles(
            30 * 1000 * 4,
            30 * 1000,
            Percentiles.BucketSizing.CONSTANT,
            percentiles.entrySet().stream()
                .map(
                    entry ->
                        new Percentile(
                            getMetricName(prefix + entry.getKey(), doc), entry.getValue()))
                .toArray(Percentile[]::new)));
  }

  private MetricName getMetricName(String name, String doc) {
    return metrics.metricInstance(
        new MetricNameTemplate(name, GROUP_NAME, doc, Collections.emptySet()), METRIC_TAGS);
  }

  void recordResponse() {
    recordMetric(responseSensorName, 1.0);
  }

  void recordRequestLatency(long value) {
    recordMetric(requestLatencySensorName, value);
  }

  void recordError() {
    recordMetric(recordErrorSensorName, 1.0);
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
}
